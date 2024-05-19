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
mod wal;

mod lf;
mod manifest;
#[cfg(feature = "std")]
mod vlf;

mod db;
pub use db::*;

mod types;
pub use types::*;

/// Utility functions.
pub mod util;
