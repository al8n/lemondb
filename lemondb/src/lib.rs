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
const LOG_EXTENSION: &str = "wal";

std::thread_local! {
  static LOG_FILENAME_BUFFER: core::cell::RefCell<std::string::String> = core::cell::RefCell::new(std::string::String::with_capacity(MAX_DIGITS + VLOG_EXTENSION.len().max(LOG_EXTENSION.len()) + 1));
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
