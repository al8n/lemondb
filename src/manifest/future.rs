use super::*;

#[cfg(not(feature = "tokio"))]
use futures::lock::Mutex;

#[cfg(feature = "tokio")]
use tokio::sync::Mutex;

use crate::AsyncMu;

pub(crate) struct AsyncManifestFile {
  kind: Mutex<ManifestFileKind>,
  fid: Fid,
}

impl AsyncManifestFile {
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
  pub(crate) async fn append(&self, ent: Entry<ManifestRecord>) -> Result<(), ManifestFileError> {
    let mut kind = self.kind.lock_me().await;
    match &mut *kind {
      ManifestFileKind::Memory(m) => m.append(ent).map_err(Into::into),
      #[cfg(feature = "std")]
      ManifestFileKind::Disk(d) => d.append(ent).map_err(Into::into),
    }
  }

  #[inline]
  pub(crate) async fn append_batch(
    &self,
    entries: Vec<Entry<ManifestRecord>>,
  ) -> Result<(), ManifestFileError> {
    let mut kind = self.kind.lock_me().await;
    match &mut *kind {
      ManifestFileKind::Memory(m) => m.append_batch(entries).map_err(Into::into),
      #[cfg(feature = "std")]
      ManifestFileKind::Disk(d) => d.append_batch(entries).map_err(Into::into),
    }
  }

  #[inline]
  pub(crate) async fn last_fid(&self) -> Fid {
    let kind = self.kind.lock_me().await;
    match &*kind {
      ManifestFileKind::Memory(m) => m.last_fid(),
      #[cfg(feature = "std")]
      ManifestFileKind::Disk(d) => d.last_fid(),
    }
  }
}
