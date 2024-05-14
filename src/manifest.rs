use core::mem;

#[cfg(feature = "std")]
use std::collections::HashSet;

#[cfg(not(feature = "std"))]
use hashbrown::HashSet;

use crate::options::ManifestOptions;

#[cfg(feature = "std")]
use crate::error::{ManifestError, UnknownManifestEvent};

mod disk;
mod memory;

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
#[non_exhaustive]
pub(crate) enum ManifestEventKind {
  AddVlog = 0,
  AddLog = 1,
  RemoveVlog = 2,
  RemoveLog = 3,
}

#[cfg(feature = "std")]
impl TryFrom<u8> for ManifestEventKind {
  type Error = UnknownManifestEvent;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    Ok(match value {
      0 => Self::AddVlog,
      1 => Self::AddLog,
      2 => Self::RemoveVlog,
      3 => Self::RemoveLog,
      _ => return Err(UnknownManifestEvent(value)),
    })
  }
}

impl ManifestEventKind {
  #[inline]
  const fn is_creation(&self) -> bool {
    matches!(self, Self::AddVlog | Self::AddLog)
  }

  #[inline]
  const fn is_deletion(&self) -> bool {
    matches!(self, Self::RemoveVlog | Self::RemoveLog)
  }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ManifestEvent {
  kind: ManifestEventKind,
  fid: u32,
}

impl ManifestEvent {
  const MAX_ENCODED_SIZE: usize = mem::size_of::<u8>()  // kind
    + mem::size_of::<u32>() // fid
    + mem::size_of::<u32>() // checksum 
    + 1; // newline character
}

#[derive(Debug, Default)]
pub(crate) struct Manifest {
  vlogs: HashSet<u32>,
  logs: HashSet<u32>,

  // Contains total number of creation and deletion changes in the manifest -- used to compute
  // whether it'd be useful to rewrite the manifest.
  creations: usize,
  deletions: usize,
}

#[derive(derive_more::From)]
enum ManifestFileKind {
  Memory(memory::MemoryManifest),
  #[cfg(feature = "std")]
  Disk(disk::DiskManifest),
}

pub(crate) struct ManifestFile {
  kind: ManifestFileKind,
}

impl ManifestFile {
  #[cfg(feature = "std")]
  pub fn open<P: AsRef<std::path::Path>>(
    dir: Option<P>,
    opts: ManifestOptions,
  ) -> Result<Self, ManifestError> {
    match dir {
      Some(dir) => disk::DiskManifest::open(
        dir,
        opts.rewrite_threshold,
        opts.external_version,
        opts.version,
      )
      .map(|file| Self {
        kind: ManifestFileKind::Disk(file),
      }),
      None => Ok(Self {
        kind: ManifestFileKind::Memory(memory::MemoryManifest::new()),
      }),
    }
  }

  #[cfg(not(feature = "std"))]
  pub fn open() -> Result<Self, ManifestError> {
    Ok(Self {
      kind: ManifestFileKind::Memory(memory::MemoryManifest::new()),
    })
  }

  #[inline]
  pub fn append(&mut self, event: ManifestEvent) -> Result<(), ManifestError> {
    match &mut self.kind {
      ManifestFileKind::Memory(m) => {
        m.append(event);
        Ok(())
      }
      #[cfg(feature = "std")]
      ManifestFileKind::Disk(d) => d.append(event),
    }
  }

  #[inline]
  pub fn flush(&mut self) -> Result<(), ManifestError> {
    match &mut self.kind {
      ManifestFileKind::Memory(_) => Ok(()),
      #[cfg(feature = "std")]
      ManifestFileKind::Disk(d) => d.flush(),
    }
  }

  #[inline]
  pub fn sync_all(&mut self) -> Result<(), ManifestError> {
    match &mut self.kind {
      ManifestFileKind::Memory(_) => Ok(()),
      #[cfg(feature = "std")]
      ManifestFileKind::Disk(d) => d.sync_all(),
    }
  }
}
