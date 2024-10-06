use core::sync::atomic::{AtomicU16, Ordering};

use dbutils::{
  buffer::VacantBuffer,
  error::InsufficientBuffer,
  leb128::{decode_u16_varint, encode_u16_varint, encoded_u16_varint_len, DecodeVarintError},
};
use derive_more::{Display, From, Into};
use zerocopy::{FromBytes, FromZeroes};

/// Table id
#[derive(
  Copy,
  Clone,
  Debug,
  Display,
  From,
  Into,
  Default,
  PartialEq,
  Eq,
  PartialOrd,
  Ord,
  Hash,
  FromBytes,
  FromZeroes,
)]
#[repr(transparent)]
pub struct TableId(u16);

impl TableId {
  /// Creates a new instance of the table id.
  #[inline]
  pub const fn new(id: u16) -> Self {
    Self(id)
  }

  /// Returns the next file id.
  #[inline]
  pub const fn next(&self) -> Self {
    Self(self.0 + 1)
  }

  /// Increments the file id.
  #[inline]
  pub fn next_assign(&mut self) {
    self.0 += 1;
  }

  /// Returns the minimum of the two table ids.
  #[inline]
  pub fn max(&self, other: Self) -> Self {
    Self(self.0.max(other.0))
  }

  /// Encodes the table id into the given buffer.
  #[inline]
  pub fn encode(&self, buf: &mut [u8]) -> Result<usize, TableIdError> {
    encode_u16_varint(self.0, buf).map_err(TableIdError::Encode)
  }

  /// Encodes the table id into the given buffer.
  #[inline]
  pub fn encode_to_buffer(&self, buf: &mut VacantBuffer<'_>) -> Result<usize, TableIdError> {
    buf.put_u16_varint(self.0).map_err(TableIdError::Encode)
  }

  /// Decodes the table id from the given buffer.
  #[inline]
  pub fn decode(buf: &[u8]) -> Result<(usize, Self), TableIdError> {
    let (read, id) = decode_u16_varint(buf).map_err(TableIdError::Decode)?;
    Ok((read, Self(id)))
  }

  /// Returns the encoded length of the table id.
  #[inline]
  pub const fn encoded_len(&self) -> usize {
    encoded_u16_varint_len(self.0)
  }
}

/// An error that occurs when encoding or decoding the table id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableIdError {
  /// An error that occurs when encoding the table id.
  Encode(InsufficientBuffer),
  /// An error that occurs when decoding the table id.
  Decode(DecodeVarintError),
}

impl core::fmt::Display for TableIdError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Encode(e) => write!(f, "failed to encode table id: {}", e),
      Self::Decode(e) => write!(f, "failed to decode table id: {}", e),
    }
  }
}

impl core::error::Error for TableIdError {}

/// Atomic table id
#[derive(Debug)]
pub struct AtomicTableId(AtomicU16);

impl AtomicTableId {
  /// Creates a new instance of the atomic table id.
  #[inline]
  pub const fn new(id: TableId) -> Self {
    Self(AtomicU16::new(id.0))
  }

  /// Creates a new instance of the atomic table id with the initial value of `0`.
  #[inline]
  pub const fn zero() -> Self {
    Self(AtomicU16::new(0))
  }

  /// Loads the table id.
  #[inline]
  pub fn load(&self, order: Ordering) -> TableId {
    TableId(self.0.load(order))
  }

  /// Increments the table id.
  #[inline]
  pub fn increment(&self, order: Ordering) -> TableId {
    TableId(self.0.fetch_add(1, order))
  }
}
