#[cfg(feature = "std")]
use std::collections::HashSet;

#[cfg(not(feature = "std"))]
use hashbrown::HashSet;

use super::{ManifestEvent, ManifestEventKind};

pub struct MemoryManifest {
  vlogs: HashSet<u32>,
  logs: HashSet<u32>,
  last_fid: u32,
}

impl MemoryManifest {
  pub fn new() -> Self {
    Self {
      vlogs: HashSet::new(),
      logs: HashSet::new(),
      last_fid: 0,
    }
  }

  pub fn append(&mut self, event: ManifestEvent) {
    match event.kind {
      ManifestEventKind::AddVlog => {
        self.vlogs.insert(event.fid);
        self.last_fid = self.last_fid.max(event.fid);
      }
      ManifestEventKind::AddLog => {
        self.logs.insert(event.fid);
        self.last_fid = self.last_fid.max(event.fid);
      }
      ManifestEventKind::RemoveVlog => {
        self.vlogs.remove(&event.fid);
      }
      ManifestEventKind::RemoveLog => {
        self.logs.remove(&event.fid);
      }
    }
  }

  #[inline]
  pub const fn last_fid(&self) -> u32 {
    self.last_fid
  }
}
