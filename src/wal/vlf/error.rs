#[derive(Debug)]
#[cfg_attr(feature = "std", derive(thiserror::Error))]
pub enum Error {
  /// An I/O error occurred.
  #[cfg_attr(feature = "std", error(transparent))]
  IO(#[cfg_attr(feature = "std", from)] std::io::Error),
  /// Returned when the value log is in closed status.
  #[cfg_attr(feature = "std", error("value log is closed"))]
  Closed,
  /// Returned when trying to write to a read-only value log.
  #[cfg_attr(feature = "std", error("value log is read only"))]
  ReadOnly,
  /// Returned when the value log checksum mismatch.
  #[cfg_attr(feature = "std", error("value log checksum mismatch"))]
  ChecksumMismatch,
  /// Returned when the value log is corrupted.
  #[cfg_attr(feature = "std", error("value log is corrupted"))]
  Corrupted,

  /// Returned when the value log does not have enough space to hold the value.
  #[cfg_attr(feature = "std", error("value log does not have enough space to hold the value, required: {required}, remaining: {remaining}"))]
  NotEnoughSpace {
    /// The required space.
    required: u64,
    /// The remaining space.
    remaining: u64,
  },

  /// Returned when the value offset is out of bound.
  #[cfg_attr(
    feature = "std",
    error("value offset is out of value log bound, offset: {offset}, len: {len}, size: {size}")
  )]
  OutOfBound {
    /// The value offset.
    offset: usize,
    /// The value size.
    len: usize,
    /// The value log size.
    size: u64,
  },
}

#[cfg(not(feature = "std"))]
impl core::fmt::Display for Error {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Error::IO(e) => write!(f, "{e}"),
      Error::Closed => write!(f, "value log is closed"),
      Error::ReadOnly => write!(f, "value log is read only"),
      Error::ChecksumMismatch => write!(f, "value log checksum mismatch"),
      Error::Corrupted => write!(f, "value log is corrupted"),
      Error::NotEnoughSpace {
        required,
        remaining,
      } => write!(
        f,
        "value log does not have enough space to hold the value, required: {}, remaining: {}",
        required, remaining
      ),
      Error::OutOfBound { offset, len, size } => write!(
        f,
        "value offset is out of value log bound, offset: {}, len: {}, size: {}",
        offset, len, size
      ),
    }
  }
}
