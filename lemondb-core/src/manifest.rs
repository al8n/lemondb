use core::{cmp::Reverse, sync::atomic::Ordering};

use std::collections::{BTreeMap, HashSet};

use among::Among;
use aol::{Batch, Entry, Record};
use arbitrary_int::u63;
use either::Either;

use crate::types::{
  fid::{AtomicFid, Fid},
  table_id::{AtomicTableId, TableId},
  table_name::{TableName, DEFAULT_TABLE_NAME},
};

mod disk;
mod entry;
pub use entry::*;
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
  fn validate(
    &self,
    entry: &Entry<Self::Record>,
  ) -> Result<(), Either<<Self::Record as Record>::Error, Self::Error>> {
    self.validate_in(entry)
  }

  #[inline]
  fn insert(
    &mut self,
    entry: aol::Entry<Self::Record>,
  ) -> Result<(), Either<<Self::Record as Record>::Error, Self::Error>> {
    self.insert_in(entry).map_err(Either::Right)
  }

  fn clear(&mut self) -> Result<(), Self::Error> {
    self.tables.clear();
    self.last_fid = Fid::new(u63::new(0));
    self.creations = 0;
    self.deletions = 0;
    Ok(())
  }
}

/// The table manifest.
#[derive(Debug)]
pub struct TableManifest {
  name: TableName,
  id: TableId,
  removed: bool,
  vlogs: HashSet<Fid>,
  active_logs: HashSet<Fid>,
  frozen_logs: HashSet<Fid>,
  bloomfilters: HashSet<Fid>,
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
      active_logs: HashSet::new(),
      frozen_logs: HashSet::new(),
      bloomfilters: HashSet::new(),
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

  fn validate_in(
    &self,
    entry: &aol::Entry<ManifestRecord>,
  ) -> Result<(), Either<ManifestRecordError, ManifestError>> {
    let flag = entry.flag();

    if !ManifestEntryFlags::is_possible_flag(flag.bits()) {
      return Err(Either::Left(ManifestRecordError::InvalidEntryFlag(
        flag.into(),
      )));
    }

    match entry.data() {
      ManifestRecord::Table { id, name } => {
        if flag.is_creation() {
          if let Some(table) = self.tables.get(&Reverse(*id)) {
            if table.name.eq(name) {
              return Ok(());
            }

            return Err(Either::Right(ManifestError::duplicate_table_id(
              *id,
              name.clone(),
              table.name.clone(),
            )));
          }

          for table in self.tables.values() {
            if table.name.eq(name) && !table.is_removed() {
              return Err(Either::Right(ManifestError::TableAlreadyExists(
                name.clone(),
              )));
            }
          }

          Ok(())
        } else {
          if let Some(table) = self.tables.get(&Reverse(*id)) {
            if table.name.eq(name) {
              return Ok(());
            }
          }

          Err(Either::Right(ManifestError::TableNotFound(*id)))
        }
      }
      ManifestRecord::Log { tid, .. } => {
        if self.tables.contains_key(&Reverse(*tid)) {
          Ok(())
        } else {
          Err(Either::Right(ManifestError::TableNotFound(*tid)))
        }
      }
    }
  }

  fn insert_in(&mut self, entry: aol::Entry<ManifestRecord>) -> Result<(), ManifestError> {
    let flag = ManifestEntryFlags::from(entry.flag());
    let record = entry.into_data();

    match record {
      ManifestRecord::Log { fid, tid } => {
        if let Some(table) = self.tables.get_mut(&Reverse(tid)) {
          if flag.is_creation() {
            match () {
              _ if flag.is_active_log() => table.active_logs.insert(fid),
              _ if flag.is_frozen_log() => table.frozen_logs.insert(fid),
              _ if flag.is_bloomfilter() => table.bloomfilters.insert(fid),
              _ if flag.is_value_log() => table.vlogs.insert(fid),
              _ => unreachable!(),
            };
          } else {
            match () {
              _ if flag.is_active_log() => table.active_logs.remove(&fid),
              _ if flag.is_frozen_log() => table.frozen_logs.remove(&fid),
              _ if flag.is_bloomfilter() => table.bloomfilters.remove(&fid),
              _ if flag.is_value_log() => table.vlogs.remove(&fid),
              _ => unreachable!(),
            };
          }

          Ok(())
        } else {
          Err(ManifestError::TableNotFound(tid))
        }
      }
      ManifestRecord::Table { id, name } => {
        if name.eq(DEFAULT_TABLE_NAME) {
          return Err(ManifestError::ReservedTable);
        }

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
  ) -> Result<Self, Among<ManifestRecordError, ManifestError, ManifestFileError>> {
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
  pub fn open(
    opts: ManifestOptions,
  ) -> Result<Self, Among<ManifestRecordError, ManifestError, ManifestFileError>> {
    Ok(Self {
      kind: ManifestFileKind::Memory(memory::MemoryManifest::new(opts)),
    })
  }

  /// Appends an entry to the manifest file.
  #[inline]
  pub fn append(
    &mut self,
    ent: ManifestEntry,
  ) -> Result<(), Among<ManifestRecordError, ManifestError, ManifestFileError>> {
    let ent = ent.into();
    match &mut self.kind {
      ManifestFileKind::Memory(m) => m.append(ent).map_err(Into::into),
      ManifestFileKind::Disk(d) => d.append(ent),
    }
  }

  /// Appends a batch of entries to the manifest file.
  #[inline]
  pub fn append_batch<B>(
    &mut self,
    entries: B,
  ) -> Result<(), Among<ManifestRecordError, ManifestError, ManifestFileError>>
  where
    B: Batch<ManifestEntry, ManifestRecord>,
  {
    match &mut self.kind {
      ManifestFileKind::Memory(m) => m.append_batch(entries).map_err(Into::into),
      ManifestFileKind::Disk(d) => d.append_batch(entries),
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
