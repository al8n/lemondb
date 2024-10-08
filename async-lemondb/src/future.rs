use super::*;

use crate::{AtomicFid, AtomicTableId};

pub(crate) struct AsyncManifestFile {
  kind: ManifestFileKind,
  fid: AtomicFid,
  tid: AtomicTableId,
}

impl AsyncManifestFile {
  
  pub(crate) fn open<P: AsRef<std::path::Path>>(
    dir: Option<P>,
    opts: ManifestOptions,
  ) -> Result<Self, ManifestFileError> {
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

  #[cfg(not(feature = "std"))]
  pub(crate) fn open(opts: ManifestOptions) -> Result<Self, ManifestFileError> {
    Ok(Self {
      kind: Mutex::new(ManifestFileKind::Memory(memory::MemoryManifest::new(opts))),
    })
  }

  #[inline]
  pub(crate) async fn append(
    &mut self,
    ent: Entry<ManifestRecord>,
  ) -> Result<(), ManifestFileError> {
    match &mut self.kind {
      ManifestFileKind::Memory(m) => m.append(ent).map_err(Into::into),
      
      ManifestFileKind::Disk(d) => d.append(ent).map_err(Into::into),
    }
  }

  #[inline]
  pub(crate) async fn append_batch(
    &mut self,
    entries: Vec<Entry<ManifestRecord>>,
  ) -> Result<(), ManifestFileError> {
    match &mut self.kind {
      ManifestFileKind::Memory(m) => m.append_batch(entries).map_err(Into::into),
      
      ManifestFileKind::Disk(d) => d.append_batch(entries).map_err(Into::into),
    }
  }

  #[inline]
  pub(crate) fn manifest(&self) -> &Manifest {
    match &self.kind {
      ManifestFileKind::Memory(m) => m.manifest(),
      
      ManifestFileKind::Disk(d) => d.manifest(),
    }
  }

  #[inline]
  pub(crate) fn next_fid(&self) -> Fid {
    self.fid.increment()
  }

  #[inline]
  pub(crate) fn next_table_id(&self) -> TableId {
    self.tid.increment()
  }
}
