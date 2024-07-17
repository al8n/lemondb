use core::mem;

use std::collections::btree_set::BTreeSet;

#[cfg(feature = "std")]
use std::collections::{HashMap, HashSet};

#[cfg(feature = "std")]
use either::Either;

use aol::{CustomFlags, Entry};
#[cfg(not(feature = "std"))]
use hashbrown::{HashMap, HashSet};
use smol_str::SmolStr;

use crate::{options::ManifestOptions, util::VarintError, Fid, TableId};

mod disk;
mod memory;

const MANIFEST_DELETIONS_REWRITE_THRESHOLD: usize = 10000;
const MANIFEST_DELETIONS_RATIO: usize = 10;
const KB: usize = 1000;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "std", derive(thiserror::Error))]
#[non_exhaustive]
pub(crate) enum ManifestError {
  /// Table not found.
  #[cfg_attr(feature = "std", error("table {0} does not exist"))]
  TableNotFound(TableId),
  /// Table already exists.
  #[cfg_attr(feature = "std", error("table {0} already exists"))]
  TableAlreadyExists(SmolStr),
  /// Table name is too long.
  #[cfg_attr(
    feature = "std",
    error("table name is too long: the maximum length is 255 bytes, but got {0}")
  )]
  LargeTableName(usize),
  /// Duplicate table id.
  #[cfg_attr(
    feature = "std",
    error("table {exist} with table id {id} already exists, but got table {new} with the same id")
  )]
  DuplicateTableId {
    id: TableId,
    exist: SmolStr,
    new: SmolStr,
  },
}

#[cfg(not(feature = "std"))]
impl core::fmt::Display for ManifestError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::TableNotFound(tid) => write!(f, "table {} does not exist", tid),
      Self::TableAlreadyExists(name) => write!(f, "table {} already exists", name),
    }
  }
}

impl ManifestError {
  #[inline]
  pub(crate) fn duplicate_table_id(id: TableId, exist: SmolStr, new: SmolStr) -> Self {
    Self::DuplicateTableId { id, exist, new }
  }
}

#[derive(Debug)]
#[cfg_attr(feature = "std", derive(thiserror::Error))]
#[non_exhaustive]
pub(crate) enum ManifestRecordError {
  /// Buffer is too small to encode the manifest record.
  #[cfg_attr(feature = "std", error("buffer is too small to encode header"))]
  BufferTooSmall,
  /// Not enough bytes to decode the value pointer.
  #[cfg_attr(feature = "std", error("not enough bytes to decode record"))]
  NotEnoughBytes,
  /// Returned when decoding varint failed.
  #[cfg_attr(feature = "std", error("invalid record, manifest may be corrupted"))]
  Corrupted,
  /// Unknown manifest event.
  #[cfg_attr(feature = "std", error("unknown manifest record type: {0}"))]
  UnknownManifestRecordType(u8),
}

impl ManifestRecordError {
  #[inline]
  const fn from_varint_error(e: VarintError) -> Self {
    match e {
      VarintError::Invalid => Self::Corrupted,
      VarintError::BufferTooSmall => Self::BufferTooSmall,
    }
  }
}

/// Errors for manifest file.
pub struct ManifestFileError {
  #[cfg(feature = "std")]
  source: Either<ManifestError, aol::fs::Error<crate::manifest::Manifest>>,
}

impl core::fmt::Debug for ManifestFileError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    #[cfg(feature = "std")]
    match &self.source {
      Either::Left(e) => e.fmt(f),
      Either::Right(e) => e.fmt(f),
    }

    #[cfg(not(feature = "std"))]
    write!(f, "ManifestFileError")
  }
}

#[cfg(feature = "std")]
impl From<aol::fs::Error<crate::manifest::Manifest>> for ManifestFileError {
  fn from(e: aol::fs::Error<crate::manifest::Manifest>) -> Self {
    Self {
      source: Either::Right(e),
    }
  }
}

#[cfg(feature = "std")]
impl From<ManifestError> for ManifestFileError {
  fn from(e: ManifestError) -> Self {
    Self {
      source: Either::Left(e),
    }
  }
}

impl core::fmt::Display for ManifestFileError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    #[cfg(feature = "std")]
    match self.source {
      Either::Left(ref e) => e.fmt(f),
      Either::Right(ref e) => e.fmt(f),
    }

    #[cfg(not(feature = "std"))]
    write!(f, "ManifestFileError")
  }
}

#[cfg(feature = "std")]
impl std::error::Error for ManifestFileError {}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub(super) enum ManifestRecord {
  Log { fid: Fid, tid: TableId },
  Table { id: TableId, name: SmolStr },
}

impl ManifestRecord {
  #[inline]
  pub(super) fn log(fid: Fid, tid: TableId) -> Self {
    Self::Log { fid, tid }
  }

  #[inline]
  pub(super) fn table(table_id: TableId, name: SmolStr) -> Self {
    Self::Table { id: table_id, name }
  }
}

#[cfg(feature = "std")]
impl aol::Record for ManifestRecord {
  type Error = ManifestRecordError;

  fn encoded_size(&self) -> usize {
    match self {
      Self::Log { fid, tid, .. } => 1 + fid.encoded_len() + tid.encoded_len(),
      Self::Table { id, name } => 1 + id.encoded_len() + mem::size_of::<u8>() + name.len(),
    }
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_size();
    if buf.len() < encoded_len {
      return Err(Self::Error::BufferTooSmall);
    }

    match self {
      Self::Log { fid, tid, .. } => {
        let mut cur = 0;
        buf[cur] = 0;
        cur += 1;
        cur += fid
          .encode(&mut buf[cur..])
          .map_err(Self::Error::from_varint_error)?;
        cur += tid
          .encode(&mut buf[cur..])
          .map_err(Self::Error::from_varint_error)?;
        Ok(cur)
      }
      Self::Table { id, name } => {
        let mut cur = 0;
        buf[cur] = 1;
        cur += 1;
        cur += id
          .encode(&mut buf[cur..])
          .map_err(Self::Error::from_varint_error)?;

        if cur + 1 + name.len() > buf.len() {
          return Err(Self::Error::BufferTooSmall);
        }

        buf[cur] = name.len() as u8;
        cur += 1;
        buf[cur..cur + name.len()].copy_from_slice(name.as_bytes());
        cur += name.len();
        Ok(cur)
      }
    }
  }

  fn decode(buf: &[u8]) -> Result<(usize, Self), Self::Error> {
    if buf.is_empty() {
      return Err(Self::Error::NotEnoughBytes);
    }

    let kind = buf[0];
    let mut cur = 1;
    Ok(match kind {
      0 => {
        let (n, fid) = Fid::decode(&buf[cur..]).map_err(Self::Error::from_varint_error)?;
        cur += n;
        let (n, tid) = TableId::decode(&buf[cur..]).map_err(Self::Error::from_varint_error)?;
        cur += n;
        // if n is larger than max u16 varint size, it's corrupted
        if n > 3 {
          return Err(Self::Error::Corrupted);
        }

        (cur, Self::Log { fid, tid })
      }
      1 => {
        let (n, id) = TableId::decode(&buf[cur..]).map_err(Self::Error::from_varint_error)?;

        // if n is larger than max u16 varint size, it's corrupted
        if n > 3 {
          return Err(Self::Error::Corrupted);
        }

        cur += n;
        let len = buf[cur] as usize;
        cur += 1;
        if buf.len() < cur + len {
          return Err(Self::Error::NotEnoughBytes);
        }

        let name = SmolStr::from(String::from_utf8_lossy(&buf[cur..cur + len]));
        cur += len;
        (cur, Self::Table { id, name })
      }
      _ => return Err(Self::Error::UnknownManifestRecordType(kind)),
    })
  }
}

/// Unknown manifest event.

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "std", derive(thiserror::Error))]
#[cfg_attr(feature = "std", error("unknown manifest record type: {0}"))]
pub struct UnknownManifestRecordType(pub(crate) u8);

#[cfg(not(feature = "std"))]
impl core::fmt::Display for UnknownManifestRecordType {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "unknown manifest record type: {}", self.0)
  }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
pub(crate) enum ManifestRecordType {
  AddLog = 0,
  AddVlog = 1,
  RemoveLog = 2,
  RemoveVlog = 3,
  AddTable = 4,
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
  #[inline]
  const fn is_creation(&self) -> bool {
    matches!(self, Self::AddVlog | Self::AddLog | Self::AddTable)
  }

  #[inline]
  const fn is_deletion(&self) -> bool {
    matches!(self, Self::RemoveVlog | Self::RemoveLog | Self::RemoveTable)
  }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ManifestEntry {
  entry: aol::Entry<Fid>,
}

impl ManifestEntry {
  #[inline]
  pub(crate) const fn add_log(fid: Fid) -> Self {
    Self {
      entry: Entry::creation(fid),
    }
  }

  #[inline]
  pub(crate) fn add_vlog(fid: Fid) -> Self {
    Self {
      entry: Entry::creation_with_custom_flags(CustomFlags::empty().with_bit1(), fid),
    }
  }

  #[inline]
  pub(crate) fn remove_log(fid: Fid) -> Self {
    Self {
      entry: Entry::deletion(fid),
    }
  }

  #[inline]
  pub(crate) fn remove_vlog(fid: Fid) -> Self {
    Self {
      entry: Entry::deletion_with_custom_flags(CustomFlags::empty().with_bit1(), fid),
    }
  }
}

#[viewit::viewit(getters(skip), setters(skip))]
#[derive(Debug)]
pub(crate) struct TableManifest {
  name: SmolStr,
  id: TableId,
  removed: bool,
  vlogs: BTreeSet<Fid>,
  logs: HashSet<Fid>,
}

impl TableManifest {
  #[inline]
  fn new(id: TableId, name: SmolStr) -> Self {
    Self {
      name,
      id,
      vlogs: BTreeSet::new(),
      logs: HashSet::new(),
      removed: false,
    }
  }

  /// Sets the table as removed.
  #[inline]
  fn set_removed(&mut self) {
    self.removed = true;
  }

  /// Returns `true` if the table is marked as removed.
  #[inline]
  const fn is_removed(&self) -> bool {
    self.removed
  }
}

#[derive(Debug, Default)]
pub(crate) struct Manifest {
  tables: HashMap<TableId, TableManifest>,
  last_fid: Fid,
  last_table_id: TableId,

  // Contains total number of creation and deletion changes in the manifest -- used to compute
  // whether it'd be useful to rewrite the manifest.
  creations: usize,
  deletions: usize,

  opts: ManifestOptions,
}

impl Manifest {
  #[inline]
  pub(crate) fn contains_table(&self, name: &str) -> bool {
    self.tables.values().any(|table| table.name.eq(name))
  }

  #[inline]
  pub(crate) fn get_table(&self, name: &str) -> Option<&TableManifest> {
    self.tables.values().find(|table| table.name.eq(name))
  }

  fn validate_in(&self, entry: &aol::Entry<ManifestRecord>) -> Result<(), ManifestError> {
    let flag = entry.flag();
    match entry.data() {
      ManifestRecord::Table { id, name } => {
        if flag.is_creation() {
          if let Some(table) = self.tables.get(id) {
            if table.name.eq(name) {
              return Ok(());
            }

            return Err(ManifestError::duplicate_table_id(
              *id,
              table.name.clone(),
              name.clone(),
            ));
          }

          for table in self.tables.values() {
            if table.name.eq(name) && !table.is_removed() {
              return Err(ManifestError::TableAlreadyExists(name.clone()));
            }
          }

          Ok(())
        } else {
          if let Some(table) = self.tables.get(id) {
            if table.name.eq(name) {
              return Ok(());
            }
          }

          Err(ManifestError::TableNotFound(*id))
        }
      }
      ManifestRecord::Log { tid, .. } => {
        if self.tables.contains_key(tid) {
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
        if let Some(table) = self.tables.get_mut(&tid) {
          if flag.custom_flag().bit1() {
            table.vlogs.insert(fid);
          } else {
            table.logs.insert(fid);
          }
          Ok(())
        } else {
          Err(ManifestError::TableNotFound(tid))
        }
      }
      ManifestRecord::Table { id, name } => {
        if flag.is_creation() {
          self.last_table_id = self.last_table_id.max(id);
          self.tables.insert(id, TableManifest::new(id, name));
          Ok(())
        } else if self.tables.remove(&id).is_some() {
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
  #[cfg(feature = "std")]
  Disk(disk::DiskManifest),
}

#[cfg(feature = "future")]
mod future;
#[cfg(feature = "future")]
pub(crate) use future::AsyncManifestFile;

#[cfg(feature = "sync")]
pub(crate) use sync::ManifestFile;
mod sync;
