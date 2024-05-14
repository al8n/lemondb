use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
use lf::LogFile;
use manifest::ManifestFile;
#[cfg(feature = "std")]
use quick_cache::sync::Cache;
use skl::Ascend;
use vlf::ValueLog;

use super::*;

pub struct Wal<C = Ascend> {
  /// All of the log files.
  lfs: SkipMap<u32, LogFile<C>>,

  /// The active value log files.
  vlfs: SkipMap<u32, ValueLog>,

  /// Cache for value log files.
  #[cfg(feature = "std")]
  vcache: Option<Cache<u32, ValueLog>>,
}
