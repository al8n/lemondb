use std::path::Path;

use among::Among;
use aol::{AppendLog, Builder, Entry};

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
  ) -> Result<Self, Among<ManifestRecordError, ManifestError, ManifestFileError>> {
    let path = path.as_ref().join(MANIFEST_FILENAME);
    Builder::new(ManifestOptions::new().with_rewrite_threshold(rewrite_threshold))
      .with_create(true)
      .with_append(true)
      .with_read(true)
      .with_magic_version(version)
      .build(&path)
      .map(|log| Self { log })
      .map_err(|e| e.map_right(Into::into))
  }

  #[inline]
  pub(super) fn append(&mut self, ent: Entry<ManifestRecord>) -> Result<(), Among<ManifestRecordError, ManifestError, ManifestFileError>> {
    self.log.append(ent).map_err(|e| e.map_right(Into::into))
  }

  #[inline]
  pub(super) fn append_batch<B>(&mut self, entries: B) -> Result<(), Among<ManifestRecordError, ManifestError, ManifestFileError>>
  where
    B: Batch<ManifestEntry, ManifestRecord>,
  {
    self.log.append_batch(entries).map_err(|e| e.map_right(Into::into))
  }

  #[inline]
  pub(super) const fn manifest(&self) -> &Manifest {
    self.log.snapshot()
  }
}
