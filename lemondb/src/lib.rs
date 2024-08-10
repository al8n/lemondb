//! A template for creating Rust open-source repo on GitHub
#![cfg_attr(not(any(feature = "std", test)), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]

#[cfg(feature = "std")]
extern crate std;

#[cfg(not(feature = "std"))]
extern crate alloc as std;

#[cfg(not(any(feature = "std", feature = "alloc")))]
compile_error!("This crate requires either the 'std' or 'alloc' feature to be enabled.");

/// The default hash builder used by the database.
#[cfg(feature = "std")]
pub type DefaultHashBuilder = std::collections::hash_map::RandomState;

/// The default hash builder used by the database.
#[cfg(not(feature = "std"))]
pub type DefaultHashBuilder = ahash::RandomState;

/// Options for configuring the database.
pub mod options;

/// Errors that can occur when working with the database.
pub mod error;

#[cfg(feature = "std")]
mod cache;
mod manifest;
mod wal;

mod db;
pub use db::*;

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

std::thread_local! {
  static LOG_FILENAME_BUFFER: core::cell::RefCell<Option<(std::path::PathBuf, std::path::PathBuf)>> = core::cell::RefCell::new(None);
}

fn with_filename<P, F, R>(path: P, fid: Fid, ext: &str, f: F) -> R
where
  P: AsRef<std::path::Path>,
  F: FnOnce(&std::path::PathBuf) -> R,
{
  use std::fmt::Write;

  LOG_FILENAME_BUFFER.with_borrow_mut(|pb| {
    let (prefix, full_path) = pb.get_or_insert_with(|| {
      let path = path.as_ref().to_path_buf();
      let cap = path.as_os_str().len() + MAX_FILENAME_SUFFIX_LEN;
      (path, std::path::PathBuf::with_capacity(cap))
    });

    full_path.clear();
    full_path.as_mut_os_string().push(prefix.as_os_str());
    full_path.push(std::path::MAIN_SEPARATOR_STR);

    write!(full_path.as_mut_os_string(), "{:020}.{}", fid, ext).unwrap();

    f(full_path)
  })
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
