#[cfg(feature = "std")]
use std::collections::HashSet;

use aol::{CustomFlags, Entry};
#[cfg(not(feature = "std"))]
use hashbrown::HashSet;

use crate::{options::ManifestOptions, Fid};

#[cfg(feature = "std")]
use crate::error::{ManifestError, UnknownManifestEvent};

mod disk;
mod memory;

const MANIFEST_DELETIONS_REWRITE_THRESHOLD: usize = 10000;
const MANIFEST_DELETIONS_RATIO: usize = 10;
const KB: usize = 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
pub(crate) enum ManifestEventKind {
  AddLog = 0,
  AddVlog = 1,
  RemoveLog = 2,
  RemoveVlog = 3,
}

#[cfg(feature = "std")]
impl TryFrom<u8> for ManifestEventKind {
  type Error = UnknownManifestEvent;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    Ok(match value {
      0 => Self::AddLog,
      1 => Self::AddVlog,
      2 => Self::RemoveLog,
      3 => Self::RemoveVlog,
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
pub(crate) struct ManifestEntry {
  entry: aol::Entry<Fid>,
}

impl ManifestEntry {
  #[inline]
  pub(crate) const fn add_log(fid: Fid) -> Self {
    Self {
      entry: Entry::creation(fid),
    }
  }

  #[inline]
  pub(crate) fn add_vlog(fid: Fid) -> Self {
    Self {
      entry: Entry::creation_with_custom_flags(CustomFlags::empty().with_bit1(), fid),
    }
  }

  #[inline]
  pub(crate) fn remove_log(fid: Fid) -> Self {
    Self {
      entry: Entry::deletion(fid),
    }
  }

  #[inline]
  pub(crate) fn remove_vlog(fid: Fid) -> Self {
    Self {
      entry: Entry::deletion_with_custom_flags(CustomFlags::empty().with_bit1(), fid),
    }
  }
}

#[derive(Debug, Default)]
pub(crate) struct Manifest {
  vlogs: HashSet<Fid>,
  logs: HashSet<Fid>,
  last_fid: Fid,

  // Contains total number of creation and deletion changes in the manifest -- used to compute
  // whether it'd be useful to rewrite the manifest.
  creations: usize,
  deletions: usize,

  opts: ManifestOptions,
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
      Some(dir) => disk::DiskManifest::open(dir, opts.rewrite_threshold, opts.version)
        .map(|file| Self {
          kind: ManifestFileKind::Disk(file),
        })
        .map_err(Into::into),
      None => Ok(Self {
        kind: ManifestFileKind::Memory(memory::MemoryManifest::new(opts)),
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
  pub fn append(&mut self, ent: Entry<Fid>) -> Result<(), ManifestError> {
    match &mut self.kind {
      ManifestFileKind::Memory(m) => {
        m.append(ent);
        Ok(())
      }
      #[cfg(feature = "std")]
      ManifestFileKind::Disk(d) => d.append(ent).map_err(Into::into),
    }
  }

  #[inline]
  pub fn append_batch(&mut self, entries: Vec<Entry<Fid>>) -> Result<(), ManifestError> {
    match &mut self.kind {
      ManifestFileKind::Memory(m) => {
        m.append_batch(entries);
        Ok(())
      }
      #[cfg(feature = "std")]
      ManifestFileKind::Disk(d) => d.append_batch(entries).map_err(Into::into),
    }
  }

  #[inline]
  pub const fn last_fid(&self) -> Fid {
    match &self.kind {
      ManifestFileKind::Memory(m) => m.last_fid(),
      #[cfg(feature = "std")]
      ManifestFileKind::Disk(d) => d.last_fid(),
    }
  }
}
