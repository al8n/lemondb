/// Errors for manifest file.
#[derive(Debug)]
#[cfg_attr(feature = "std", derive(thiserror::Error))]
pub enum ManifestError {
  /// Manifest has bad magic.
  #[error("manifest has bad magic")]
  #[cfg(feature = "std")]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  BadMagic,
  /// Cannot open manifest because the external version doesn't match.
  #[error("cannot open manifest because the external version doesn't match. expected {expected}, found {found}")]
  #[cfg(feature = "std")]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
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
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  BadVersion {
    /// Expected version.
    expected: u16,
    /// Found version.
    found: u16,
  },
  /// Corrupted manifest file: entry checksum mismatch.
  #[error("corrupted manifest file: entry checksum mismatch")]
  #[cfg(feature = "std")]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  ChecksumMismatch,
  /// Corrupted manifest file: not enough bytes to decode manifest entry.
  #[error("corrupted manifest file: not enough bytes to decode manifest entry")]
  #[cfg(feature = "std")]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  Corrupted,
  /// Unknown manifest event.
  #[error(transparent)]
  #[cfg(feature = "std")]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  UnknownManifestEvent(#[from] UnknownManifestEvent),
  /// I/O error.
  #[error(transparent)]
  #[cfg(feature = "std")]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  IO(#[from] std::io::Error),
}

/// Unknown manifest event.
#[cfg(feature = "std")]
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("unknown manifest event: {0}")]
pub struct UnknownManifestEvent(pub(crate) u8);

/// Errors that can occur when working with a log.
#[derive(Debug)]
#[cfg_attr(feature = "std", derive(thiserror::Error))]
pub enum LogFileError {
  /// An I/O error occurred.
  #[cfg(feature = "std")]
  #[cfg_attr(feature = "std", error(transparent))]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  IO(#[from] std::io::Error),
  /// A log error occurred.
  #[cfg_attr(feature = "std", error(transparent))]
  Log(#[cfg_attr(feature = "std", from)] skl::map::Error),
  /// Returned when writing the batch failed.
  #[cfg_attr(
    feature = "std",
    error("failed to write batch at index {idx}: {source}")
  )]
  WriteBatch {
    /// The index of the key-value pair that caused the error.
    idx: usize,
    /// The error that caused the failure.
    #[cfg_attr(feature = "std", source)]
    source: skl::map::Error,
  },

  /// Returned when checksum mismatch.
  #[cfg_attr(feature = "std", error("checksum mismatch"))]
  ChecksumMismatch,
}

#[cfg(not(feature = "std"))]
impl From<skl::map::Error> for LogFileError {
  fn from(e: skl::map::Error) -> Self {
    LogFileError::Log(e)
  }
}

#[cfg(not(feature = "std"))]
impl core::fmt::Display for LogFileError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Log(e) => write!(f, "{}", e),
      Self::WriteBatch { idx, source } => {
        write!(f, "failed to write batch at index {}: {}", idx, source)
      }
    }
  }
}

/// Error type returned by the value log.
#[derive(Debug)]
#[cfg_attr(feature = "std", derive(thiserror::Error))]
pub enum ValueLogError {
  /// An I/O error occurred.
  #[cfg_attr(feature = "std", error(transparent))]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  IO(#[cfg_attr(feature = "std", from)] std::io::Error),
  /// Returned when the value log is in closed status.
  #[cfg_attr(feature = "std", error("value log is closed"))]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  Closed,
  /// Returned when trying to write to a read-only value log.
  #[cfg_attr(feature = "std", error("value log is read only"))]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  ReadOnly,
  /// Returned when the value log checksum mismatch.
  #[cfg_attr(feature = "std", error("value log checksum mismatch"))]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  ChecksumMismatch,
  /// Returned when the value log is corrupted.
  #[cfg_attr(feature = "std", error("value log is corrupted"))]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
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

/// Errors that can occur when working with database
#[derive(Debug)]
#[cfg_attr(feature = "std", derive(thiserror::Error))]
pub enum Error {
  /// An I/O error occurred.
  #[cfg(feature = "std")]
  #[cfg_attr(feature = "std", error(transparent))]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  IO(#[from] std::io::Error),
  /// A log file error occurred.
  #[cfg_attr(feature = "std", error(transparent))]
  LogFile(#[cfg_attr(feature = "std", from)] LogFileError),
  /// A manifest file error occurred.
  #[cfg_attr(feature = "std", error(transparent))]
  Manifest(#[cfg_attr(feature = "std", from)] ManifestError),
  /// A value log error occurred.
  #[cfg_attr(feature = "std", error(transparent))]
  ValueLog(#[cfg_attr(feature = "std", from)] ValueLogError),
}
