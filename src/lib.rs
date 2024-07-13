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
const MAX_DIGITS: usize = 10; // u32::MAX has 10 digits

const VLOG_EXTENSION: &str = "vlog";
const LOG_EXTENSION: &str = "wal";

std::thread_local! {
  static LOG_FILENAME_BUFFER: core::cell::RefCell<std::string::String> = core::cell::RefCell::new(std::string::String::with_capacity(MAX_DIGITS + VLOG_EXTENSION.len().max(LOG_EXTENSION.len()) + 1));
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Fid(u32);

impl Fid {
  #[inline]
  const fn next(&self) -> Self {
    Self(self.0 + 1)
  }

  #[inline]
  fn next_assign(&mut self) {
    self.0 += 1;
  }

  #[inline]
  fn max(&self, other: Self) -> Self {
    Self(self.0.max(other.0))
  }
}

#[cfg(feature = "std")]
impl aol::Data for Fid {
  type Error = core::convert::Infallible;

  fn encoded_size(&self) -> usize {
    core::mem::size_of::<u32>()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    buf.copy_from_slice(&self.0.to_le_bytes());
    Ok(core::mem::size_of::<u32>())
  }

  fn decode(buf: &[u8]) -> Result<(usize, Self), Self::Error> {
    let fid = u32::from_le_bytes(buf.try_into().unwrap());
    Ok((core::mem::size_of::<u32>(), Self(fid)))
  }
}
