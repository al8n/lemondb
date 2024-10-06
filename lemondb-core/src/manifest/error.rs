use among::Among;
use aol::Error;
use derive_more::{AsRef, Deref, From, Into};

use crate::{
  error::{IncompleteBuffer, InsufficientBuffer},
  types::{
    fid::FidError,
    table_id::{TableId, TableIdError},
    table_name::TableName,
  },
};

use super::{ManifestEntryFlags, UnknownManifestRecordType};

/// An error that occurs when manipulating the manifest file.
#[derive(Debug, From, Into, Deref, AsRef)]
pub struct ManifestFileError(Among<ManifestRecordError, ManifestError, Error>);

impl From<ManifestRecordError> for ManifestFileError {
  #[inline]
  fn from(e: ManifestRecordError) -> Self {
    Self(Among::Left(e))
  }
}

impl From<ManifestError> for ManifestFileError {
  #[inline]
  fn from(e: ManifestError) -> Self {
    Self(Among::Middle(e))
  }
}

impl From<Error> for ManifestFileError {
  #[inline]
  fn from(e: Error) -> Self {
    Self(Among::Right(e))
  }
}

impl core::fmt::Display for ManifestFileError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match &self.0 {
      Among::Left(e) => e.fmt(f),
      Among::Middle(e) => e.fmt(f),
      Among::Right(e) => e.fmt(f),
    }
  }
}

impl core::error::Error for ManifestFileError {}

/// An error that occurs when manipulating the manifest.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum ManifestError {
  /// Table not found.
  #[error("table {0} does not exist")]
  TableNotFound(TableId),
  /// Table already exists.
  #[error("table {0} already exists")]
  TableAlreadyExists(TableName),
  /// Returned when trying to create or delete the default table.
  #[error("cannot create or delete the default table")]
  ReservedTable,
  /// Table name is too long.
  #[error("table name is too long: the maximum length is 255 bytes, but got {0}")]
  LargeTableName(usize),
  /// Returned when there is a duplicate table id.
  #[error("duplicate table id: {id}")]
  DuplicateTableId {
    /// The table id.
    id: TableId,
    /// The table name.
    name: TableName,
    /// The existing table name.
    existing: TableName,
  },
}

impl ManifestError {
  #[inline]
  pub(super) fn duplicate_table_id(id: TableId, name: TableName, existing: TableName) -> Self {
    Self::DuplicateTableId { id, name, existing }
  }
}

/// An error that occurs when manipulating the manifest record.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum ManifestRecordError {
  /// Buffer is too small to encode the manifest record.
  #[error("buffer is too small to encode header")]
  InsufficientBuffer(#[from] InsufficientBuffer),
  /// Not enough bytes to decode the value pointer.
  #[error("not enough bytes to decode record")]
  IncompleteBuffer(#[from] IncompleteBuffer),
  /// Returned when decoding varint failed.
  #[error("invalid record, manifest may be corrupted")]
  Corrupted,
  /// Returned when failed to encode or decode a file id.
  #[error(transparent)]
  Fid(#[from] FidError),
  /// Returned when failed to encode or decode a table id.
  #[error(transparent)]
  TableId(#[from] TableIdError),
  /// Returned when failed to decode a table name.
  #[error("failed to decode table name: {0}")]
  TableName(#[from] core::str::Utf8Error),
  /// Returned when decoding unknown manifest event.
  #[error("unknown manifest record type: {0}")]
  UnknownRecordType(#[from] UnknownManifestRecordType),
  /// Returned when there is an invalid entry flag.
  #[error("invalid entry flag: {0}")]
  InvalidEntryFlag(ManifestEntryFlags),
}
