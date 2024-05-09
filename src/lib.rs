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

mod lf;
mod vlf;

mod types;
pub use types::*;

/// Utility functions.
pub mod util;
