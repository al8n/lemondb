use crate::Fid;

const MB: usize = 1 << 20;
const GB: usize = 1 << 30;
const DEFAULT_WRITE_BUFFER_SIZE: usize = 1024;

/// The options for creating a log.
#[viewit::viewit(getters(style = "move"), setters(prefix = "with"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct CreateOptions {
  /// The file ID of the log.
  #[viewit(
    getter(const, attrs(doc = "Returns the file ID of the log.")),
    setter(attrs(doc = "Sets the file ID of the log."))
  )]
  fid: Fid,

  /// The maximum size of the log. Default is 2GB.
  ///
  /// The log is backed by a mmaped file with the given size.
  /// So this size determines the mmaped file size.
  #[viewit(
    getter(const, attrs(doc = "Returns the size of the log.")),
    setter(attrs(doc = "Sets the size of the log."))
  )]
  size: u64,

  /// Whether to lock the log. Default is `true`.
  ///
  /// If `true`, the log will be locked exlusively when it is created.
  #[viewit(
    getter(const, attrs(doc = "Returns if we should lock the log.")),
    setter(attrs(doc = "Sets whether to lock the log."))
  )]
  lock: bool,

  /// Whether to sync on write. Default is `true`.
  ///
  /// If `true`, the log will sync the data to disk on write.
  #[viewit(
    getter(const, attrs(doc = "Returns if we should sync on write.")),
    setter(attrs(doc = "Sets whether to sync on write."))
  )]
  sync_on_write: bool,

  /// Whether to open in-memory log. Default is `false`.
  ///
  /// If `true`, the log will be opened in memory.
  #[viewit(
    getter(const, attrs(doc = "Returns if we should open in-memory log.")),
    setter(attrs(doc = "Sets whether to open in-memory log."))
  )]
  in_memory: bool,
}

impl CreateOptions {
  /// Creates a new create options with the default values.
  #[inline]
  pub const fn new(fid: Fid) -> Self {
    Self {
      fid,
      size: 2 * GB as u64,
      lock: true,
      sync_on_write: true,
      in_memory: false,
    }
  }
}

/// The options for opening a log.
#[viewit::viewit(getters(style = "move"), setters(prefix = "with"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct OpenOptions {
  /// The file ID of the log.
  #[viewit(
    getter(const, attrs(doc = "Returns the file ID of the log.")),
    setter(attrs(doc = "Sets the file ID of the log."))
  )]
  fid: Fid,

  /// Whether to lock the log. Default is `true`.
  ///
  /// If `true`, the log will be locked exlusively when it is created.
  #[viewit(
    getter(const, attrs(doc = "Returns if we should lock the log.")),
    setter(attrs(doc = "Sets whether to lock the log."))
  )]
  lock: bool,
}

impl OpenOptions {
  /// Creates a new create options with the default values.
  #[inline]
  pub const fn new(fid: Fid) -> Self {
    Self { fid, lock: true }
  }
}

/// The options for configuring the value log.
#[viewit::viewit(getters(style = "move"), setters(prefix = "with"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ValueLogOptions {
  /// The value threshold for values to be stored in the value log. Default is 1MB.
  ///
  /// If the value size is less than this threshold, the value will be stored within the key log.
  /// Otherwise, the value will be stored in the value log.
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the value threshold for values to be stored in the value log.")
    ),
    setter(attrs(doc = "Sets the value threshold for values to be stored in the value log."))
  )]
  value_threshold: u64,

  /// The value threshold for big values. Default is 1GB.
  ///
  /// If the value size is greater than this threshold, the value will be stored in a standalone value log.
  /// Otherwise, the value will be stored in a shared value log.
  #[viewit(
    getter(const, attrs(doc = "Returns the value threshold for big values.")),
    setter(attrs(doc = "Sets the value threshold for big values."))
  )]
  big_value_threshold: u64,
}

impl Default for ValueLogOptions {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl ValueLogOptions {
  /// Creates a new value log options with the default values.
  #[inline]
  pub const fn new() -> Self {
    Self {
      value_threshold: MB as u64,
      big_value_threshold: GB as u64,
    }
  }
}

/// The options for opening a manifest file.
#[viewit::viewit(getters(style = "move"), setters(prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ManifestOptions {
  /// The version of the lemon manifest file. Default is `0`.
  #[viewit(
    getter(const, attrs(doc = "Returns the version of the manifest file.")),
    setter(attrs(doc = "Sets the version of the manifest file."))
  )]
  version: u16,
  /// The rewrite threshold for the manifest file. Default is `10000`.
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the rewrite threshold for the manifest file.")
    ),
    setter(attrs(doc = "Sets the rewrite threshold for the manifest file."))
  )]
  rewrite_threshold: usize,
}

impl Default for ManifestOptions {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl ManifestOptions {
  /// Creates a new manifest options with the default values.
  #[inline]
  pub const fn new() -> Self {
    Self {
      version: 0,
      rewrite_threshold: 10000,
    }
  }
}

/// The options for configuring the write-ahead log.
#[viewit::viewit(getters(style = "move"), setters(prefix = "with"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalOptions {
  /// The default size of the write-ahead log size. Default is 2GB.
  #[viewit(
    getter(const, attrs(doc = "Returns the size of the write-ahead log.")),
    setter(attrs(doc = "Sets the size of the write-ahead log."))
  )]
  log_size: u64,

  /// The default size of the shared value log. Default is 2GB.
  #[viewit(
    getter(const, attrs(doc = "Returns the size of the shared value log.")),
    setter(attrs(doc = "Sets the size of the shared value log."))
  )]
  vlog_size: u64,

  /// The value threshold for values to be stored in the value log. Default is 1MB.
  ///
  /// If the value size is less than this threshold, the value will be stored within the key log.
  /// Otherwise, the value will be stored in the value log.
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the value threshold for values to be stored in the value log.")
    ),
    setter(attrs(doc = "Sets the value threshold for values to be stored in the value log."))
  )]
  value_threshold: u64,

  /// The value threshold for big values. Default is `vlog_size * 0.5`.
  ///
  /// If the value size is greater than this threshold, the value will be stored in a standalone value log.
  /// Otherwise, the value will be stored in a shared value log.
  #[viewit(
    getter(const, attrs(doc = "Returns the value threshold for big values.")),
    setter(attrs(doc = "Sets the value threshold for big values."))
  )]
  big_value_threshold: u64,

  /// Whether to lock the log. Default is `true`.
  ///
  /// If `true`, the log will be locked exlusively when it is created.
  #[viewit(
    getter(const, attrs(doc = "Returns if we should lock the log.")),
    setter(attrs(doc = "Sets whether to lock the log."))
  )]
  lock: bool,

  /// Whether to sync on write. Default is `true`.
  ///
  /// If `true`, the log will sync the data to disk on write.
  #[viewit(
    getter(const, attrs(doc = "Returns if we should sync on write.")),
    setter(attrs(doc = "Sets whether to sync on write."))
  )]
  sync_on_write: bool,

  /// Whether to open in-memory log. Default is `false`.
  ///
  /// If `true`, the log will be opened in memory.
  #[viewit(
    getter(const, attrs(doc = "Returns if we should open in-memory log.")),
    setter(attrs(doc = "Sets whether to open in-memory log."))
  )]
  in_memory: bool,
}

impl Default for WalOptions {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl WalOptions {
  /// Creates a new log manager options with the default values.
  #[inline]
  pub const fn new() -> Self {
    Self {
      log_size: 2 * GB as u64,
      vlog_size: 2 * GB as u64,
      value_threshold: MB as u64,
      big_value_threshold: GB as u64,
      lock: true,
      sync_on_write: true,
      in_memory: false,
    }
  }

  /// Creates a new log manager options with the given log size.
  #[inline]
  pub(crate) const fn create_options(&self, fid: Fid) -> CreateOptions {
    CreateOptions {
      fid,
      size: self.log_size,
      lock: self.lock,
      sync_on_write: self.sync_on_write,
      in_memory: self.in_memory,
    }
  }

  /// Creates a new value log options with the given log size.
  #[inline]
  pub(crate) const fn open_options(&self, fid: Fid) -> OpenOptions {
    OpenOptions {
      fid,
      lock: self.lock,
    }
  }
}

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

  #[viewit(
    getter(
      const,
      attrs(doc = "
    Returns whether to run the table in standalone mode. Default is `false`.

    Standalone mode means that table does not have relationship with other table, e.g., when reading or writing key-value pair to this table, it will not depends on key-value pairs in other tables. 
    In this mode, the table will have it own write thread.

    ")
    ),
    setter(attrs(doc = "
    Sets whether to run the table in standalone mode. Default is `false`.

    Standalone mode means that table does not have relationship with other table, e.g., when reading or writing key-value pair to this table, it will not depends on key-value pairs in other tables. 
    In this mode, the table will have it own write thread.
 
    "))
  )]
  standalone: bool,

  /// The default size of the write-ahead log size. Default is 2GB.
  #[viewit(
    getter(const, attrs(doc = "Returns the size of the write-ahead log.")),
    setter(attrs(doc = "Sets the size of the write-ahead log."))
  )]
  log_size: u64,

  /// The default size of the shared value log. Default is 2GB.
  #[viewit(
    getter(const, attrs(doc = "Returns the size of the shared value log.")),
    setter(attrs(doc = "Sets the size of the shared value log."))
  )]
  vlog_size: u64,

  /// The value threshold for values to be stored in the value log. Default is 1MB.
  ///
  /// If the value size is less than this threshold, the value will be stored within the key log.
  /// Otherwise, the value will be stored in the value log.
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the value threshold for values to be stored in the value log.")
    ),
    setter(attrs(doc = "Sets the value threshold for values to be stored in the value log."))
  )]
  value_threshold: u64,

  /// The value threshold for big values. Default is `vlog_size * 0.5`.
  ///
  /// If the value size is greater than this threshold, the value will be stored in a standalone value log.
  /// Otherwise, the value will be stored in a shared value log.
  #[viewit(
    getter(const, attrs(doc = "Returns the value threshold for big values.")),
    setter(attrs(doc = "Sets the value threshold for big values."))
  )]
  big_value_threshold: u64,

  /// The write buffer size. Default is `1024`.
  ///
  /// The write buffer is used to buffer the write operations before they are written to the database.
  #[viewit(
    getter(const, attrs(doc = "Returns the write buffer size.")),
    setter(attrs(doc = "Sets the write buffer size."))
  )]
  write_buffer_size: usize,

  /// Whether to lock the log. Default is `true`.
  ///
  /// If `true`, the log will be locked exlusively when it is created.
  #[viewit(
    getter(const, attrs(doc = "Returns if we should lock the log.")),
    setter(attrs(doc = "Sets whether to lock the log."))
  )]
  lock: bool,

  /// Whether to sync on write. Default is `true`.
  ///
  /// If `true`, the log will sync the data to disk on write.
  #[viewit(
    getter(const, attrs(doc = "Returns if we should sync on write.")),
    setter(attrs(doc = "Sets whether to sync on write."))
  )]
  sync_on_write: bool,
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
    let wal = WalOptions::new();
    Self {
      read_only: false,
      create: false,
      create_new: false,
      standalone: false,
      write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
      log_size: wal.log_size,
      vlog_size: wal.vlog_size,
      value_threshold: wal.value_threshold,
      big_value_threshold: wal.big_value_threshold,
      sync_on_write: wal.sync_on_write,
      lock: wal.lock,
    }
  }

  #[inline]
  pub(crate) fn to_wal_options(&self, in_memory: bool) -> WalOptions {
    WalOptions {
      log_size: self.log_size,
      vlog_size: self.vlog_size,
      value_threshold: self.value_threshold,
      big_value_threshold: self.big_value_threshold,
      sync_on_write: self.sync_on_write,
      in_memory,
      lock: self.lock,
    }
  }
}

impl From<WalOptions> for TableOptions {
  fn from(val: WalOptions) -> Self {
    Self {
      read_only: false,
      create: false,
      create_new: false,
      standalone: false,
      write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
      log_size: val.log_size,
      vlog_size: val.vlog_size,
      value_threshold: val.value_threshold,
      big_value_threshold: val.big_value_threshold,
      sync_on_write: val.sync_on_write,
      lock: val.lock,
    }
  }
}

/// The options for configuring the database.
#[viewit::viewit(getters(style = "move"), setters(prefix = "with"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Options {
  /// The default size of the write-ahead log size. Default is 2GB.
  #[viewit(
    getter(const, attrs(doc = "Returns the size of the write-ahead log.")),
    setter(attrs(doc = "Sets the size of the write-ahead log."))
  )]
  log_size: u64,

  /// The default size of the shared value log. Default is 2GB.
  #[viewit(
    getter(const, attrs(doc = "Returns the size of the shared value log.")),
    setter(attrs(doc = "Sets the size of the shared value log."))
  )]
  vlog_size: u64,

  /// The value threshold for values to be stored in the value log. Default is 1MB.
  ///
  /// If the value size is less than this threshold, the value will be stored within the key log.
  /// Otherwise, the value will be stored in the value log.
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the value threshold for values to be stored in the value log.")
    ),
    setter(attrs(doc = "Sets the value threshold for values to be stored in the value log."))
  )]
  value_threshold: u64,

  /// The value threshold for big values. Default is `vlog_size * 0.5`.
  ///
  /// If the value size is greater than this threshold, the value will be stored in a standalone value log.
  /// Otherwise, the value will be stored in a shared value log.
  #[viewit(
    getter(const, attrs(doc = "Returns the value threshold for big values.")),
    setter(attrs(doc = "Sets the value threshold for big values."))
  )]
  big_value_threshold: u64,

  /// Whether to lock the log. Default is `true`.
  ///
  /// If `true`, the log will be locked exlusively when it is created.
  #[viewit(
    getter(const, attrs(doc = "Returns if we should lock the log.")),
    setter(attrs(doc = "Sets whether to lock the log."))
  )]
  lock: bool,

  /// Whether to sync on write. Default is `true`.
  ///
  /// If `true`, the log will sync the data to disk on write.
  #[viewit(
    getter(const, attrs(doc = "Returns if we should sync on write.")),
    setter(attrs(doc = "Sets whether to sync on write."))
  )]
  sync_on_write: bool,

  /// Whether to open in-memory log. Default is `false`.
  ///
  /// If `true`, the log will be opened in memory.
  #[viewit(
    getter(const, attrs(doc = "Returns if we should open in-memory log.")),
    setter(attrs(doc = "Sets whether to open in-memory log."))
  )]
  in_memory: bool,
}

impl Default for Options {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl Options {
  /// Creates a new log manager options with the default values.
  #[inline]
  pub const fn new() -> Self {
    Self {
      log_size: 2 * GB as u64,
      vlog_size: 2 * GB as u64,
      value_threshold: MB as u64,
      big_value_threshold: GB as u64,
      lock: true,
      sync_on_write: true,
      in_memory: false,
    }
  }

  /// Creates a new log manager options with the given log size.
  #[inline]
  pub(crate) const fn create_options(&self, fid: Fid) -> CreateOptions {
    CreateOptions {
      fid,
      size: self.log_size,
      lock: self.lock,
      sync_on_write: self.sync_on_write,
      in_memory: self.in_memory,
    }
  }
}
