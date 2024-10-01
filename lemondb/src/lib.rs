//! A template for creating Rust open-source repo on GitHub
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]

/// The default hash builder used by the database.
pub type DefaultHashBuilder = std::collections::hash_map::RandomState;

/// Options for configuring the database.
pub mod options;

// /// Errors that can occur when working with the database.
// pub mod error;

mod active_log;
// mod cache;
// mod manifest;
// mod wal;

// mod db;
// pub use db::*;

mod types;
pub use types::*;

/// Utility functions.
pub mod util;

const CURRENT_VERSION: u16 = 0;
const MAX_DIGITS: usize = 20; // u64::MAX has 20 digits

const VLOG_EXTENSION: &str = "vlog";
const LOG_EXTENSION: &str = "slog";
// 20 digits + 1 dot + 4 extension
const MAX_FILENAME_SUFFIX_LEN: usize = 4 + MAX_DIGITS + 1;

fn filename<P>(path: P, fid: Fid, ext: &str) -> std::path::PathBuf
where
  P: AsRef<std::path::Path>,
{
  use std::fmt::Write;

  let mut path = path.as_ref().to_path_buf();
  path.reserve_exact(MAX_FILENAME_SUFFIX_LEN + 1);
  write!(
    path.as_mut_os_string(),
    "{}{:020}.{}",
    std::path::MAIN_SEPARATOR_STR,
    fid,
    ext
  )
  .unwrap();

  path
}

trait Mu {
  type Guard<'a>
  where
    Self: 'a;

  fn lock_me(&self) -> Self::Guard<'_>;
}

#[cfg(feature = "parking_lot")]
impl<T: ?Sized> Mu for parking_lot::Mutex<T> {
  type Guard<'a> = parking_lot::MutexGuard<'a, T> where Self: 'a;

  fn lock_me(&self) -> Self::Guard<'_> {
    self.lock()
  }
}

#[cfg(not(feature = "parking_lot"))]
impl<T: ?Sized> Mu for std::sync::Mutex<T> {
  type Guard<'a> = std::sync::MutexGuard<'a, T> where Self: 'a;

  fn lock_me(&self) -> Self::Guard<'_> {
    self.lock().unwrap()
  }
}
