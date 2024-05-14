#[cfg(feature = "std")]
use std::collections::HashSet;

#[cfg(not(feature = "std"))]
use hashbrown::HashSet;

use super::{Manifest, ManifestEvent, ManifestEventKind};

pub struct MemoryManifest {
  vlogs: HashSet<u32>,
  logs: HashSet<u32>,
}

impl MemoryManifest {
  pub fn new() -> Self {
    Self {
      vlogs: HashSet::new(),
      logs: HashSet::new(),
    }
  }

  pub fn append(&mut self, event: ManifestEvent) {
    match event.kind {
      ManifestEventKind::AddVlog => {
        self.vlogs.insert(event.fid);
      }
      ManifestEventKind::AddLog => {
        self.logs.insert(event.fid);
      }
      ManifestEventKind::RemoveVlog => {
        self.vlogs.remove(&event.fid);
      }
      ManifestEventKind::RemoveLog => {
        self.logs.remove(&event.fid);
      }
    }
  }
}
