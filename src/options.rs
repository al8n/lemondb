const MB: usize = 1 << 20;
const GB: usize = 1 << 30;

/// The options for creating a log.
#[viewit::viewit(getters(style = "move"), setters(prefix = "with"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CreateOptions {
  /// The file ID of the log.
  #[viewit(
    getter(const, attrs(doc = "Returns the file ID of the log.")),
    setter(attrs(doc = "Sets the file ID of the log."))
  )]
  fid: u32,

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
  pub const fn new(fid: u32) -> Self {
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
pub struct OpenOptions {
  /// The file ID of the log.
  #[viewit(
    getter(const, attrs(doc = "Returns the file ID of the log.")),
    setter(attrs(doc = "Sets the file ID of the log."))
  )]
  fid: u32,

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
  pub const fn new(fid: u32) -> Self {
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
  /// The external version of the lemon manifest file. Default is `0`.
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the external version of the manifest file.")
    ),
    setter(attrs(doc = "Sets the external version of the manifest file."))
  )]
  external_version: u16,
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
      external_version: 0,
      version: 0,
      rewrite_threshold: 10000,
    }
  }
}
