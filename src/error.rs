/// Errors for manifest file.
#[derive(Debug)]
#[cfg_attr(feature = "std", derive(thiserror::Error))]
pub enum ManifestError {
  /// Manifest has bad magic.
  #[error("manifest has bad magic")]
  #[cfg(feature = "std")]
  BadMagic,
  /// Cannot open manifest because the external version doesn't match.
  #[error("cannot open manifest because the external version doesn't match. expected {expected}, found {found}")]
  #[cfg(feature = "std")]
  BadExternalVersion {
    /// Expected external version.
    expected: u16,
    /// Found external version.
    found: u16,
  },
  /// Cannot open manifest because the version doesn't match.
  #[error(
    "cannot open manifest because the version doesn't match. expected {expected}, found {found}"
  )]
  #[cfg(feature = "std")]
  BadVersion {
    /// Expected version.
    expected: u16,
    /// Found version.
    found: u16,
  },
  /// Corrupted manifest file: entry checksum mismatch.
  #[error("corrupted manifest file: entry checksum mismatch")]
  #[cfg(feature = "std")]
  ChecksumMismatch,
  /// Corrupted manifest file: not enough bytes to decode manifest entry.
  #[error("corrupted manifest file: not enough bytes to decode manifest entry")]
  #[cfg(feature = "std")]
  Corrupted,
  /// Unknown manifest event.
  #[error(transparent)]
  #[cfg(feature = "std")]
  UnknownManifestEvent(#[from] UnknownManifestEvent),
  /// I/O error.
  #[error(transparent)]
  #[cfg(feature = "std")]
  IO(#[from] std::io::Error),
}

/// Unknown manifest event.
#[cfg(feature = "std")]
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("unknown manifest event: {0}")]
pub struct UnknownManifestEvent(pub(crate) u8);

/// Error type returned by the value log.
#[derive(Debug)]
#[cfg_attr(feature = "std", derive(thiserror::Error))]
pub enum ValueLogError {
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
impl core::fmt::Display for ValueLogError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      ValueLogError::IO(e) => write!(f, "{e}"),
      ValueLogError::Closed => write!(f, "value log is closed"),
      ValueLogError::ReadOnly => write!(f, "value log is read only"),
      ValueLogError::ChecksumMismatch => write!(f, "value log checksum mismatch"),
      ValueLogError::Corrupted => write!(f, "value log is corrupted"),
      ValueLogError::NotEnoughSpace {
        required,
        remaining,
      } => write!(
        f,
        "value log does not have enough space to hold the value, required: {}, remaining: {}",
        required, remaining
      ),
      ValueLogError::OutOfBound { offset, len, size } => write!(
        f,
        "value offset is out of value log bound, offset: {}, len: {}, size: {}",
        offset, len, size
      ),
    }
  }
}
