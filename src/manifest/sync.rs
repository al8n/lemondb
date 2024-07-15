use super::*;

#[cfg(not(feature = "parking_lot"))]
use std::sync::Mutex;

#[cfg(feature = "parking_lot")]
use parking_lot::Mutex;

use crate::Mu;

pub(crate) struct ManifestFile {
  kind: Mutex<ManifestFileKind>,
  fid: Fid,
}

impl ManifestFile {
  #[cfg(feature = "std")]
  pub(crate) fn open<P: AsRef<std::path::Path>>(
    dir: Option<P>,
    opts: ManifestOptions,
  ) -> Result<Self, ManifestFileError> {
    match dir {
      Some(dir) => disk::DiskManifest::open(dir, opts.rewrite_threshold, opts.version)
        .map(|file| Self {
          fid: Fid::new(0),
          kind: Mutex::new(ManifestFileKind::Disk(file)),
        })
        .map_err(Into::into),
      None => Ok(Self {
        fid: Fid::new(0),
        kind: Mutex::new(ManifestFileKind::Memory(memory::MemoryManifest::new(opts))),
      }),
    }
  }

  #[cfg(not(feature = "std"))]
  pub(crate) fn open() -> Result<Self, ManifestFileError> {
    Ok(Self {
      kind: Mutex::new(ManifestFileKind::Memory(memory::MemoryManifest::new())),
    })
  }

  #[inline]
  pub(crate) fn append(&self, ent: Entry<ManifestRecord>) -> Result<(), ManifestFileError> {
    let mut kind = self.kind.lock_me();
    match &mut *kind {
      ManifestFileKind::Memory(m) => m.append(ent).map_err(Into::into),
      #[cfg(feature = "std")]
      ManifestFileKind::Disk(d) => d.append(ent).map_err(Into::into),
    }
  }

  #[inline]
  pub(crate) fn append_batch(
    &self,
    entries: Vec<Entry<ManifestRecord>>,
  ) -> Result<(), ManifestFileError> {
    let mut kind = self.kind.lock_me();
    match &mut *kind {
      ManifestFileKind::Memory(m) => m.append_batch(entries).map_err(Into::into),
      #[cfg(feature = "std")]
      ManifestFileKind::Disk(d) => d.append_batch(entries).map_err(Into::into),
    }
  }

  #[inline]
  pub(crate) fn last_fid(&self) -> Fid {
    let kind = self.kind.lock_me();
    match &*kind {
      ManifestFileKind::Memory(m) => m.last_fid(),
      #[cfg(feature = "std")]
      ManifestFileKind::Disk(d) => d.last_fid(),
    }
  }
}
