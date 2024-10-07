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
  pub(super) fn append(
    &mut self,
    entry: aol::Entry<ManifestRecord>,
  ) -> Result<(), Either<ManifestRecordError, ManifestError>> {
    self.manifest.validate(entry.as_ref()).map(|_| {
      self.manifest.insert(entry.into());
    })
  }

  #[inline]
  pub(super) fn append_batch<B>(
    &mut self,
    entries: B,
  ) -> Result<(), Either<ManifestRecordError, ManifestError>>
  where
    B: Batch<ManifestEntry, ManifestRecord>,
  {
    self.manifest.validate_batch(&entries).map(|_| {
      self.manifest.insert_batch(entries);
    })
  }

  #[inline]
  pub(super) fn manifest(&self) -> &Manifest {
    &self.manifest
  }
}
