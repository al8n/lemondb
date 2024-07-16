use std::sync::Arc;

use indexmap::IndexMap;
pub use skl::{Ascend, Comparator, Descend};

#[cfg(feature = "sync")]
mod sync;

#[cfg(feature = "future")]
mod future;

/// Options for opening a table.
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with")
)]
pub struct TableOptions {
  #[viewit(
    getter(const, attrs(doc = "Returns whether the table is read-only.")),
    setter(attrs(doc = "Sets whether the table is read-only."))
  )]
  read_only: bool,
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns whether to create the table if it does not exist.")
    ),
    setter(attrs(doc = "Sets whether to create the table if it does not exist."))
  )]
  create: bool,
  #[viewit(
    getter(const, attrs(doc = "Returns whether to force create a new table.")),
    setter(attrs(doc = "Sets whether to force create a new table."))
  )]
  create_new: bool,
}

impl Default for TableOptions {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl TableOptions {
  /// Create a new table options with the default values.
  #[inline]
  pub const fn new() -> Self {
    Self {
      read_only: false,
      create: false,
      create_new: false,
    }
  }
}
