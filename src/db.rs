use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use indexmap::IndexMap;
pub use skl::{Ascend, Comparator, Descend};

// /// Database
// pub struct Db<C = Ascend> {
//   manifest: (),
//   lfs: Arc<AtomicRefCell<IndexMap<u32, LogFile<C>>>>,
// }
