/// Checksum mismatch.
#[derive(Debug)]
#[cfg_attr(feature = "std", derive(thiserror::Error))]
#[cfg_attr(feature = "std", error("checksum mismatch"))]
pub struct ChecksumMismatch;

#[cfg(not(feature = "std"))]
impl core::fmt::Display for ChecksumMismatch {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "checksum mismatch")
  }
}

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
  ChecksumMismatch(#[cfg_attr(feature = "std", from)] ChecksumMismatch),

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
  ChecksumMismatch(#[cfg_attr(feature = "std", from)] ChecksumMismatch),
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
      Self::ChecksumMismatch(e) => write!(f, "{e}"),
      Self::Log(e) => write!(f, "{e}"),
      Self::WriteBatch { idx, source } => {
        write!(f, "failed to write batch at index {}: {}", idx, source)
      }
    }
  }
}

/// Errors that can occur when encode/decode header.
#[derive(Debug, thiserror::Error)]
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub enum DecodeHeaderError {
  /// Returned when fail to decode header.
  #[error("fail to decode header: {0}")]
  Deocode(#[from] crate::util::VarintError),
  /// Returned when not enough bytes to decode header.
  #[error("not enough bytes to decode header")]
  NotEnoughBytes,
}

/// Error type returned by the value log.
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
#[derive(Debug, thiserror::Error)]
pub enum ValueLogError {
  /// An I/O error occurred.
  #[error(transparent)]
  IO(#[from] std::io::Error),

  /// Returned when the value log is in closed status.
  #[error("value log is closed")]
  Closed,

  /// Returned when trying to write to a read-only value log.
  #[error("value log is read only")]
  ReadOnly,

  /// Returned when the value log checksum mismatch.
  #[error("value log checksum mismatch")]
  ChecksumMismatch(#[from] ChecksumMismatch),

  /// Returned when the value log is corrupted.
  #[error("value log is corrupted")]
  Corrupted,

  /// Returned when fail to decode entry header from the value log.
  #[error(transparent)]
  DecodeHeader(#[from] DecodeHeaderError),

  /// Returned when fail to encode entry header.
  #[error("fail to encode header: {0}")]
  EncodeHeader(#[from] crate::util::VarintError),

  /// Returned when the value log does not have enough space to hold the value.
  #[error("value log does not have enough space to hold the value, required: {required}, remaining: {remaining}")]
  NotEnoughSpace {
    /// The required space.
    required: u64,
    /// The remaining space.
    remaining: u64,
  },

  /// Returned when the value offset is out of bound.
  #[error("value offset is out of value log bound, offset: {offset}, len: {len}, size: {size}")]
  OutOfBound {
    /// The value offset.
    offset: usize,
    /// The value size.
    len: usize,
    /// The value log size.
    size: u64,
  },
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
