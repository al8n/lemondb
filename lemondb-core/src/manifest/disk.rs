use std::{fs::OpenOptions, path::Path};

use aol::{AppendLog, Entry, Options};

use super::*;

const MANIFEST_FILENAME: &str = "MANIFEST";

pub(super) struct DiskManifest {
  log: AppendLog<Manifest>,
}

impl DiskManifest {
  /// Open and replay the manifest file.
  pub(super) fn open<P: AsRef<Path>>(
    path: P,
    rewrite_threshold: usize,
    version: u16,
  ) -> Result<Self, ManifestFileError> {
    let path = path.as_ref().join(MANIFEST_FILENAME);
    let mut open_options = OpenOptions::new();
    open_options.read(true).create(true).append(true);
    let log = AppendLog::open(
      &path,
      ManifestOptions::new().with_rewrite_threshold(rewrite_threshold),
      Options::new().with_magic_version(version),
    )?;

    Ok(Self { log })
  }

  #[inline]
  pub(super) fn append(&mut self, ent: Entry<ManifestRecord>) -> Result<(), ManifestFileError> {
    self.log.append(ent).map_err(Into::into)
  }

  #[inline]
  pub(super) fn append_batch<B>(&mut self, entries: B) -> Result<(), ManifestFileError>
  where
    B: Batch<ManifestEntry, ManifestRecord>,
  {
    self.log.append_batch(entries).map_err(Into::into)
  }

  #[inline]
  pub(super) const fn manifest(&self) -> &Manifest {
    self.log.snapshot()
  }
}
