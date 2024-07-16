use std::sync::Arc;

use aol::CustomFlags;
use crossbeam_skiplist::SkipMap;
use error::ValueLogError;
use lf::LogFile;
use manifest::{ManifestFile, ManifestRecord};
#[cfg(feature = "std")]
use quick_cache::sync::Cache;
use skl::{Ascend, Trailer};

#[cfg(feature = "std")]
use vlf::ValueLog;

use crate::options::CreateOptions;

use self::util::checksum;

use super::{
  error::{Error, LogFileError},
  options::WalOptions,
  *,
};

mod lf;
#[cfg(feature = "std")]
mod vlf;

#[cfg(feature = "sync")]
mod sync;
#[cfg(feature = "sync")]
pub(crate) use sync::Wal;

#[cfg(feature = "future")]
mod future;
// #[cfg(feature = "future")]
// pub(crate) use future::AsyncWal;
