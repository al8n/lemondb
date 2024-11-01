use std::sync::Arc;

use aol::checksum::BuildChecksumer;
use crossbeam_skiplist::SkipMap;
use lemondb_core::{
  active_log::log::{ActiveLogFile, ActiveLogFileReader},
  immutable_log::ImmutableLogFile,
  manifest::ManifestFile,
  types::{fid::Fid, key::Key, table_id::TableId, table_name::TableName},
  value_log::ValueLog,
};
use parking_lot::Mutex;
use skl::Comparator;

pub(crate) struct Table<C> {
  id: TableId,
  name: TableName,
  active_logs: Arc<SkipMap<Fid, ActiveLogFileReader<C>>>,
  frozen_logs: Arc<SkipMap<Fid, ImmutableLogFile<Key<C>>>>,
  vlfs: Arc<SkipMap<Fid, Arc<ValueLog>>>,
}

impl<C> Table<C> {
  // pub(crate) fn read(&self, version: u64, key: &[u8]) {

  //   self.active_logs.iter().find_map(|f| {
  //     let reader = f.value();
  //     if reader.get(version) {

  //     }
  //   });
  // }
}

// /// a
// pub struct TableWriter {
//   id: TableId,
//   name: TableName,
//   manifest: Arc<Mutex<ManifestFile>>,
//   active_logs: SkipMap<Fid, ActiveLogFile<Arc<dyn Comparator>>>,

//   // TODO: immutable logs
//   // immutable_logs: SkipMap<Fid, Arc<ImmutableLogFile>>,

//   /// The value log files, the last one is the active value log file.
//   /// not all value log files are stored in this map.
//   vlfs: SkipMap<Fid, Arc<ValueLog>>,

//   // /// Cache for value log files.
//   // vcache: Option<Arc<ValueLogCache>>,
//   manifest: Arc<Mutex<ManifestFile>>,
// }

// impl Table {
//   pub(crate) fn insert(&mut self) {

//   }
// }
