use std::sync::Arc;

use crate::{wal::ValueLog, Fid};

pub type ValueLogCache = quick_cache::sync::Cache<Fid, Arc<ValueLog>>;
