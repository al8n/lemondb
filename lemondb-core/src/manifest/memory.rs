use aol::{Batch, Snapshot};

use super::*;

pub(crate) struct MemoryManifest {
  manifest: Manifest,
}

impl MemoryManifest {
  #[inline]
  pub(super) fn new(opts: ManifestOptions) -> Self {
    Self {
      manifest: Manifest::new(opts).unwrap(),
    }
  }

  #[inline]
  pub(super) fn append(&mut self, entry: aol::Entry<ManifestRecord>) -> Result<(), ManifestError> {
    self.manifest.insert(entry)
  }

  #[inline]
  pub(super) fn append_batch<B>(&mut self, entries: B) -> Result<(), ManifestError>
  where
    B: Batch<ManifestEntry, ManifestRecord>,
  {
    self.manifest.insert_batch(entries)
  }

  #[inline]
  pub(super) fn manifest(&self) -> &Manifest {
    &self.manifest
  }
}
