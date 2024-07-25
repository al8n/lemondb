use std::sync::Arc;

pub use skl::{Ascend, Comparator, Descend};

/// Synchronous database.
#[cfg(feature = "sync")]
pub mod sync;

#[cfg(feature = "future")]
mod future;
