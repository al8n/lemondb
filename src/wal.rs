use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use crossbeam_skiplist::SkipMap;
use indexmap::IndexMap;
use lf::LogFile;
#[cfg(feature = "std")]
use quick_cache::sync::Cache;
use skl::Ascend;
use vlf::ValueLog;

use super::*;

#[cfg(feature = "std")]
mod cache;

mod lf;
mod manifest;
mod vlf;

pub struct Wal<C = Ascend> {
  lfs: SkipMap<u32, LogFile<C>>,

  /// The active value log file.
  /// 
  /// Only one thread can update this, so [`AtomicRefCell`] is enough to handle this situation.
  vlog: AtomicRefCell<ValueLog>,

  /// Cache for value log files.
  #[cfg(feature = "std")]
  vcache: Option<Cache<u32, ValueLog>>,
}
