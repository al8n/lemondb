use std::{fs::OpenOptions, path::Path};

use aol::{
  fs::{AppendLog, Error, Options},
  Entry,
};
use parking_lot::Mutex;

use crate::Fid;

use super::*;

const MANIFEST_FILENAME: &str = "MANIFEST";

impl aol::fs::Snapshot for Manifest {
  type Record = ManifestRecord;

  type Options = ManifestOptions;

  type Error = ManifestError;

  fn new(opts: Self::Options) -> Result<Self, Self::Error> {
    Ok(Self {
      tables: HashMap::new(),
      last_fid: Fid::new(0),
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
    self.last_fid = Fid::new(0);
    self.creations = 0;
    self.deletions = 0;
    Ok(())
  }
}

pub(super) struct DiskManifest {
  log: Mutex<AppendLog<Manifest>>,
}

impl DiskManifest {
  /// Open and replay the manifest file.
  pub(super) fn open<P: AsRef<Path>>(
    path: P,
    rewrite_threshold: usize,
    version: u16,
  ) -> Result<Self, Error<Manifest>> {
    let path = path.as_ref().join(MANIFEST_FILENAME);
    let mut open_options = OpenOptions::new();
    open_options.read(true).create(true).append(true);
    let log = AppendLog::open(
      &path,
      ManifestOptions::new().with_rewrite_threshold(rewrite_threshold),
      open_options,
      Options::new().with_magic_version(version),
    )?;

    Ok(Self {
      log: Mutex::new(log),
    })
  }

  #[inline]
  pub(super) fn append(&self, ent: Entry<ManifestRecord>) -> Result<(), Error<Manifest>> {
    self.log.lock().append(ent)
  }

  #[inline]
  pub(super) fn append_batch(
    &self,
    entries: Vec<Entry<ManifestRecord>>,
  ) -> Result<(), Error<Manifest>> {
    self.log.lock().append_batch(entries)
  }

  #[inline]
  pub(super) fn last_fid(&self) -> Fid {
    self.log.lock().snapshot().last_fid
  }
}
