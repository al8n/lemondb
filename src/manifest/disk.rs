use std::{fs::OpenOptions, path::Path};

use aol::{
  fs::{AppendLog, Error, Options},
  Entry,
};

use crate::Fid;

use super::*;

const MANIFEST_FILENAME: &str = "MANIFEST";

impl aol::fs::Snapshot for Manifest {
  type Data = Fid;

  type Options = ManifestOptions;

  type Error = core::convert::Infallible;

  fn new(opts: Self::Options) -> Result<Self, Self::Error> {
    Ok(Self {
      vlogs: HashSet::new(),
      logs: HashSet::new(),
      last_fid: Fid(0),
      creations: 0,
      deletions: 0,
      opts,
    })
  }

  fn should_rewrite(&self, _size: u64) -> bool {
    self.deletions > self.opts.rewrite_threshold
      && self.deletions > MANIFEST_DELETIONS_RATIO * self.creations.saturating_sub(self.deletions)
  }

  fn insert(&mut self, entry: aol::Entry<Self::Data>) -> Result<(), Self::Error> {
    let fid = *entry.data();
    self.last_fid = self.last_fid.max(fid);
    if entry.flag().custom_flag().bit1() {
      self.vlogs.insert(fid);
    } else {
      self.logs.insert(fid);
    }
    Ok(())
  }

  fn insert_batch(&mut self, entries: Vec<aol::Entry<Self::Data>>) -> Result<(), Self::Error> {
    for entry in entries {
      let fid = *entry.data();
      self.last_fid = self.last_fid.max(fid);
      if entry.flag().custom_flag().bit1() {
        self.vlogs.insert(fid);
      } else {
        self.logs.insert(fid);
      }
    }
    Ok(())
  }

  fn clear(&mut self) -> Result<(), Self::Error> {
    self.vlogs.clear();
    self.logs.clear();
    self.last_fid = Fid(0);
    self.creations = 0;
    self.deletions = 0;
    Ok(())
  }
}

pub(super) struct DiskManifest {
  log: AppendLog<Manifest>,
}

impl DiskManifest {
  /// Open and replay the manifest file.
  pub fn open<P: AsRef<Path>>(
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

    Ok(Self { log })
  }

  #[inline]
  pub fn append(&mut self, ent: Entry<Fid>) -> Result<(), Error<Manifest>> {
    self.log.append(ent)
  }

  #[inline]
  pub fn append_batch(&mut self, entries: Vec<Entry<Fid>>) -> Result<(), Error<Manifest>> {
    self.log.append_batch(entries)
  }

  #[inline]
  pub const fn last_fid(&self) -> Fid {
    self.log.snapshot().last_fid
  }
}
