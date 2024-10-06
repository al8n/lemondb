use core::{cmp::Reverse, mem, sync::atomic::Ordering};

use std::collections::{BTreeMap, HashSet};

use aol::{
  buffer::VacantBuffer,
  error::{IncompleteBuffer, InsufficientBuffer},
  Entry,
};
use arbitrary_int::u63;
use smol_str::SmolStr;

use crate::types::{
  fid::{AtomicFid, Fid},
  table_id::{AtomicTableId, TableId},
  table_name::TableName,
};

mod disk;
mod error;
pub use error::*;
mod memory;
mod options;
pub use options::ManifestOptions;

const MANIFEST_DELETIONS_RATIO: usize = 10;

impl aol::Snapshot for Manifest {
  type Record = ManifestRecord;

  type Options = ManifestOptions;

  type Error = ManifestError;

  fn new(opts: Self::Options) -> Result<Self, Self::Error> {
    Ok(Self {
      tables: BTreeMap::new(),
      last_fid: Fid::new(u63::new(0)),
      last_table_id: TableId::new(0),
      creations: 0,
      deletions: 0,
      opts,
    })
  }

  fn should_rewrite(&self, _size: u64) -> bool {
    self.deletions > self.opts.rewrite_threshold
      && self.deletions > MANIFEST_DELETIONS_RATIO * self.creations.saturating_sub(self.deletions)
  }

  #[inline]
  fn validate(&self, entry: &Entry<Self::Record>) -> Result<(), Self::Error> {
    self.validate_in(entry)
  }

  #[inline]
  fn insert(&mut self, entry: aol::Entry<Self::Record>) -> Result<(), Self::Error> {
    self.insert_in(entry)
  }

  fn clear(&mut self) -> Result<(), Self::Error> {
    self.tables.clear();
    self.last_fid = Fid::new(u63::new(0));
    self.creations = 0;
    self.deletions = 0;
    Ok(())
  }
}

/// The manifest record.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ManifestRecord {
  /// Log record.
  Log {
    /// File ID.
    fid: Fid,
    /// Table ID.
    tid: TableId,
  },
  /// Table record.
  Table {
    /// Table ID.
    id: TableId,
    /// Table name.
    name: TableName,
  },
}

impl ManifestRecord {
  /// Creates a new log record.
  #[inline]
  pub fn log(fid: Fid, tid: TableId) -> Self {
    Self::Log { fid, tid }
  }

  /// Creates a new table record.
  #[inline]
  pub fn table(table_id: TableId, name: TableName) -> Self {
    Self::Table { id: table_id, name }
  }
}

impl aol::Record for ManifestRecord {
  type Error = ManifestRecordError;

  fn encoded_size(&self) -> usize {
    match self {
      Self::Log { fid, tid, .. } => 1 + fid.encoded_len() + tid.encoded_len(),
      Self::Table { id, name } => 1 + id.encoded_len() + mem::size_of::<u8>() + name.len(),
    }
  }

  fn encode(&self, buf: &mut VacantBuffer<'_>) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_size();
    let cap = buf.capacity();
    if cap < encoded_len {
      return Err(InsufficientBuffer::with_information(encoded_len, cap).into());
    }

    match self {
      Self::Log { fid, tid } => {
        let mut cur = 0;
        buf.put_u8(0)?;
        cur += 1;
        cur += fid.encode_to_buffer(buf)?;
        cur += tid.encode_to_buffer(buf)?;
        Ok(cur)
      }
      Self::Table { id, name } => {
        let mut cur = 0;
        buf.put_u8(1)?;
        cur += 1;
        cur += id.encode_to_buffer(buf)?;

        let remaining = buf.remaining();
        let want = 1 + name.len();
        if want > remaining {
          return Err(InsufficientBuffer::with_information(cur + want, cur + remaining).into());
        }

        buf.put_u8(name.len() as u8)?;
        cur += 1;
        buf.put_slice(name.as_bytes())?;
        cur += name.len();
        Ok(cur)
      }
    }
  }

  fn decode(buf: &[u8]) -> Result<(usize, Self), Self::Error> {
    if buf.is_empty() {
      return Err(IncompleteBuffer::new().into());
    }

    let kind = buf[0];
    let mut cur = 1;
    Ok(match kind {
      0 => {
        let (n, fid) = Fid::decode(&buf[cur..])?;
        cur += n;
        let (n, tid) = TableId::decode(&buf[cur..])?;
        cur += n;

        (cur, Self::Log { fid, tid })
      }
      1 => {
        let (n, id) = TableId::decode(&buf[cur..])?;

        cur += n;
        let len = buf[cur] as usize;
        cur += 1;
        if buf.len() < cur + len {
          return Err(IncompleteBuffer::with_information(cur + len, buf.len()).into());
        }

        let name = SmolStr::from(core::str::from_utf8(&buf[cur..cur + len])?);
        cur += len;
        (
          cur,
          Self::Table {
            id,
            name: name.into(),
          },
        )
      }
      _ => {
        return Err(Self::Error::UnknownRecordType(UnknownManifestRecordType(
          kind,
        )))
      }
    })
  }
}

/// Manifest record type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
pub enum ManifestRecordType {
  /// Add log event.
  AddLog = 0,
  /// Add vlog event.
  AddVlog = 1,
  /// Remove log event.
  RemoveLog = 2,
  /// Remove vlog event.
  RemoveVlog = 3,
  /// Add table event.
  AddTable = 4,
  /// Remove table event.
  RemoveTable = 5,
}

impl TryFrom<u8> for ManifestRecordType {
  type Error = UnknownManifestRecordType;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    Ok(match value {
      0 => Self::AddLog,
      1 => Self::AddVlog,
      2 => Self::RemoveLog,
      3 => Self::RemoveVlog,
      4 => Self::AddTable,
      5 => Self::RemoveTable,
      _ => return Err(UnknownManifestRecordType(value)),
    })
  }
}

impl ManifestRecordType {
  /// Returns `true` if the record is a creation event.
  #[inline]
  pub const fn is_creation(&self) -> bool {
    matches!(self, Self::AddVlog | Self::AddLog | Self::AddTable)
  }

  /// Returns `true` if the record is a deletion event.
  #[inline]
  pub const fn is_deletion(&self) -> bool {
    matches!(self, Self::RemoveVlog | Self::RemoveLog | Self::RemoveTable)
  }
}

/// The table manifest.
#[derive(Debug)]
pub struct TableManifest {
  name: TableName,
  id: TableId,
  removed: bool,
  vlogs: HashSet<Fid>,
  logs: HashSet<Fid>,
}

impl TableManifest {
  /// Returns the table id.
  #[inline]
  pub fn id(&self) -> TableId {
    self.id
  }

  #[inline]
  fn new(id: TableId, name: TableName) -> Self {
    Self {
      name,
      id,
      vlogs: HashSet::new(),
      logs: HashSet::new(),
      removed: false,
    }
  }

  /// Returns `true` if the table is marked as removed.
  #[inline]
  const fn is_removed(&self) -> bool {
    self.removed
  }
}

/// The in-memory snapshot of the manifest file.
#[derive(Debug, Default)]
pub struct Manifest {
  tables: BTreeMap<Reverse<TableId>, TableManifest>,
  last_fid: Fid,
  last_table_id: TableId,

  // Contains total number of creation and deletion changes in the manifest -- used to compute
  // whether it'd be useful to rewrite the manifest.
  creations: usize,
  deletions: usize,

  opts: ManifestOptions,
}

impl Manifest {
  /// Returns `true` if the manifest contains the table with the given name.
  #[inline]
  pub fn contains_table(&self, name: &str) -> bool {
    self.tables.values().any(|table| table.name.eq(name))
  }

  /// Returns the table with the given ID.
  #[inline]
  pub fn get_table(&self, name: &str) -> Option<&TableManifest> {
    self.tables.values().find(|table| table.name.eq(name))
  }

  fn validate_in(&self, entry: &aol::Entry<ManifestRecord>) -> Result<(), ManifestError> {
    let flag = entry.flag();
    match entry.data() {
      ManifestRecord::Table { id, name } => {
        if flag.is_creation() {
          if let Some(table) = self.tables.get(&Reverse(*id)) {
            if table.name.eq(name) {
              return Ok(());
            }

            return Err(ManifestError::duplicate_table_id(
              *id,
              name.clone(),
              table.name.clone(),
            ));
          }

          for table in self.tables.values() {
            if table.name.eq(name) && !table.is_removed() {
              return Err(ManifestError::TableAlreadyExists(name.clone()));
            }
          }

          Ok(())
        } else {
          if let Some(table) = self.tables.get(&Reverse(*id)) {
            if table.name.eq(name) {
              return Ok(());
            }
          }

          Err(ManifestError::TableNotFound(*id))
        }
      }
      ManifestRecord::Log { tid, .. } => {
        if self.tables.contains_key(&Reverse(*tid)) {
          Ok(())
        } else {
          Err(ManifestError::TableNotFound(*tid))
        }
      }
    }
  }

  fn insert_in(&mut self, entry: aol::Entry<ManifestRecord>) -> Result<(), ManifestError> {
    let flag = entry.flag();
    let record = entry.into_data();

    match record {
      ManifestRecord::Log { fid, tid } => {
        if let Some(table) = self.tables.get_mut(&Reverse(tid)) {
          if flag.is_creation() {
            if flag.custom_flag().bit1() {
              table.vlogs.insert(fid);
            } else {
              table.logs.insert(fid);
            }
          } else if flag.custom_flag().bit1() {
            table.vlogs.remove(&fid);
          } else {
            table.logs.remove(&fid);
          }

          Ok(())
        } else {
          Err(ManifestError::TableNotFound(tid))
        }
      }
      ManifestRecord::Table { id, name } => {
        if flag.is_creation() {
          self.last_table_id = self.last_table_id.max(id);
          self
            .tables
            .insert(Reverse(id), TableManifest::new(id, name));
          Ok(())
        } else if self.tables.remove(&Reverse(id)).is_some() {
          Ok(())
        } else {
          Err(ManifestError::TableNotFound(id))
        }
      }
    }
  }
}

#[derive(derive_more::From)]
enum ManifestFileKind {
  Memory(memory::MemoryManifest),
  Disk(disk::DiskManifest),
}

/// A manifest file.
pub struct ManifestFile {
  kind: ManifestFileKind,
  fid: AtomicFid,
  tid: AtomicTableId,
}

impl ManifestFile {
  /// Opens a manifest file.
  pub fn open<P: AsRef<std::path::Path>>(
    dir: Option<P>,
    opts: ManifestOptions,
  ) -> Result<Self, ManifestFileError> {
    match dir {
      Some(dir) => disk::DiskManifest::open(dir, opts.rewrite_threshold, opts.version)
        .map(|file| {
          let manifest = file.manifest();
          Self {
            fid: AtomicFid::new(manifest.last_fid),
            tid: AtomicTableId::new(manifest.last_table_id),
            kind: ManifestFileKind::Disk(file),
          }
        })
        .map_err(Into::into),
      None => Ok(Self {
        fid: AtomicFid::zero(),
        tid: AtomicTableId::zero(),
        kind: ManifestFileKind::Memory(memory::MemoryManifest::new(opts)),
      }),
    }
  }

  /// Opens a memory manifest file.
  #[cfg(not(feature = "std"))]
  pub fn open(opts: ManifestOptions) -> Result<Self, ManifestFileError> {
    Ok(Self {
      kind: ManifestFileKind::Memory(memory::MemoryManifest::new(opts)),
    })
  }

  /// Appends an entry to the manifest file.
  #[inline]
  pub fn append(&mut self, ent: Entry<ManifestRecord>) -> Result<(), ManifestFileError> {
    match &mut self.kind {
      ManifestFileKind::Memory(m) => m.append(ent).map_err(Into::into),
      ManifestFileKind::Disk(d) => d.append(ent).map_err(Into::into),
    }
  }

  /// Appends a batch of entries to the manifest file.
  #[inline]
  pub fn append_batch(
    &mut self,
    entries: Vec<Entry<ManifestRecord>>,
  ) -> Result<(), ManifestFileError> {
    match &mut self.kind {
      ManifestFileKind::Memory(m) => m.append_batch(entries).map_err(Into::into),
      ManifestFileKind::Disk(d) => d.append_batch(entries).map_err(Into::into),
    }
  }

  /// Returns the manifest.
  #[inline]
  pub fn manifest(&self) -> &Manifest {
    match &self.kind {
      ManifestFileKind::Memory(m) => m.manifest(),
      ManifestFileKind::Disk(d) => d.manifest(),
    }
  }

  /// Increments the file ID and returns the new value.
  #[inline]
  pub fn next_fid(&self) -> Fid {
    self.fid.increment(Ordering::AcqRel)
  }

  /// Increments the table ID and returns the new value.
  #[inline]
  pub fn next_table_id(&self) -> TableId {
    self.tid.increment(Ordering::AcqRel)
  }
}
