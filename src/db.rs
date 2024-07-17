use std::sync::Arc;

pub use skl::{Ascend, Comparator, Descend};

#[cfg(feature = "sync")]
mod sync;

#[cfg(feature = "future")]
mod future;
