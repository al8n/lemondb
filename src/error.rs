#[cfg(feature = "std")]
use core::convert::Infallible;

#[cfg(feature = "std")]
use either::Either;

use crate::manifest::ManifestFileError;

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
pub enum EncodeHeaderError {
  /// Buffer is too small to encode the value pointer.
  #[error("buffer is too small to encode header")]
  BufferTooSmall,
  /// Returned when encoding/decoding varint failed.
  #[error("fail to decode header: {0}")]
  VarintError(#[from] crate::util::VarintError),
}

/// Errors that can occur when encode/decode header.
#[derive(Debug, thiserror::Error)]
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub enum DecodeHeaderError {
  /// Not enough bytes to decode the value pointer.
  #[error("not enough bytes to decode header")]
  NotEnoughBytes,
  /// Returned when encoding/decoding varint failed.
  #[error("fail to decode header: {0}")]
  VarintError(#[from] crate::util::VarintError),
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
  #[error(transparent)]
  EncodeHeader(#[from] EncodeHeaderError),

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
  Manifest(#[cfg_attr(feature = "std", from)] ManifestFileError),
  /// A value log error occurred.
  #[cfg_attr(feature = "std", error(transparent))]
  ValueLog(#[cfg_attr(feature = "std", from)] ValueLogError),
}
