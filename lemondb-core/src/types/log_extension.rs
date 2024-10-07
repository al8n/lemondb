/// All the log file extensions.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum LogExtension {
  /// Active log.
  Active,
  /// Frozen log.
  Frozen,
  /// Bloomfilter.
  Bloomfilter,
  /// Value log.
  Value,
}

impl LogExtension {
  /// The length of the log extension.
  pub const LEN: usize = 4;

  /// Returns the log extension for constructing a file path.
  #[inline]
  pub const fn extension(&self) -> &'static str {
    match self {
      Self::Active => "alog",
      Self::Frozen => "flog",
      Self::Bloomfilter => "blog",
      Self::Value => "vlog",
    }
  }
}
