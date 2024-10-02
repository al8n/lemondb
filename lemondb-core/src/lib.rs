//! LemonDB core library.
#![cfg_attr(not(any(feature = "std", test)), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]

#[cfg(not(feature = "std"))]
extern crate alloc as std;

#[cfg(feature = "std")]
extern crate std;

/// An active log.
pub mod active_log;

/// An active log which supports generic types.
pub mod generic_active_log;

/// A frozen log.
pub mod immutable_log;

/// Core types used by the database.
pub mod types;

/// Utility functions.
pub mod utils;
