#[cfg(feature = "std")]
use std::collections::HashSet;

use aol::{memory::Snapshot, CustomFlags, Entry};
#[cfg(not(feature = "std"))]
use hashbrown::HashSet;

use crate::Fid;

use super::*;

impl Snapshot for Manifest {
  type Data = Fid;

  type Options = ManifestOptions;

  type Error = core::convert::Infallible;

  fn open(opts: Self::Options) -> Result<Self, Self::Error> {
    Ok(Self {
      vlogs: HashSet::new(),
      logs: HashSet::new(),
      last_fid: Fid(0),
      creations: 0,
      deletions: 0,
      opts,
    })
  }

  fn options(&self) -> &Self::Options {
    &self.opts
  }

  fn should_rewrite(&self) -> bool {
    self.deletions > self.opts.rewrite_threshold
      && self.deletions > MANIFEST_DELETIONS_RATIO * self.creations.saturating_sub(self.deletions)
  }

  fn insert(&mut self, entry: Entry<Self::Data>) -> Result<(), Self::Error> {
    let fid = *entry.data();
    self.last_fid = self.last_fid.max(fid);
    if entry.flag().custom_flag().bit1() {
      self.vlogs.insert(fid);
    } else {
      self.logs.insert(fid);
    }
    Ok(())
  }

  fn insert_batch(
    &mut self,
    entries: impl Iterator<Item = Entry<Self::Data>>,
  ) -> Result<(), Self::Error> {
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

  fn into_iter(self) -> impl Iterator<Item = Entry<Self::Data>> {
    self
      .vlogs
      .into_iter()
      .map(|fid| Entry::creation_with_custom_flags(CustomFlags::empty().with_bit1(), fid))
      .chain(self.logs.into_iter().map(|fid| Entry::creation(fid)))
  }
}

pub(crate) struct MemoryManifest {
  manifest: Manifest,
}

impl MemoryManifest {
  #[inline]
  pub fn new(opts: ManifestOptions) -> Self {
    Self {
      manifest: Manifest {
        vlogs: HashSet::new(),
        logs: HashSet::new(),
        last_fid: Fid(0),
        creations: 0,
        deletions: 0,
        opts,
      },
    }
  }

  #[inline]
  pub fn append(&mut self, entry: aol::Entry<Fid>) {
    self.manifest.insert(entry).unwrap();
  }

  #[inline]
  pub fn append_batch(&mut self, entries: Vec<aol::Entry<Fid>>) {
    self.manifest.insert_batch(entries.into_iter()).unwrap();
  }

  #[inline]
  pub const fn last_fid(&self) -> Fid {
    self.manifest.last_fid
  }
}
