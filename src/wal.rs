use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
use lf::LogFile;
use manifest::ManifestFile;
#[cfg(feature = "std")]
use quick_cache::sync::Cache;
use skl::Ascend;
use vlf::ValueLog;

use super::*;

pub struct LogManager<C = Ascend> {
  /// All of the log files.
  lfs: SkipMap<u32, LogFile<C>>,

  /// The active value log files.
  vlfs: SkipMap<u32, Arc<ValueLog>>,

  /// Cache for value log files.
  #[cfg(feature = "std")]
  vcache: Option<Arc<Cache<u32, Arc<ValueLog>>>>,


}

impl<C: Comparator> LogManager<C> {
  pub fn insert_bytes(&self, key: &[u8], val: &[u8]) {
    // first check if the value is big enough to be written to a standalone value log file

    // first try to write to the active value log file
    let active_vlf = self.vlfs.front().expect("no active value log file");


  }
}