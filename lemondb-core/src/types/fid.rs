use core::sync::atomic::{AtomicU64, Ordering};

pub use arbitrary_int::u63;

use arbitrary_int::Number;
use dbutils::{
  buffer::VacantBuffer,
  error::InsufficientBuffer,
  leb128::{decode_u64_varint, encode_u64_varint, encoded_u64_varint_len, DecodeVarintError},
  CheapClone,
};
use derive_more::{Add, AddAssign};

/// The file ID in the database.
///
/// The maximum number of files in the database is `2^63`.
#[derive(Debug, Default, Add, AddAssign, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fid(u63);

impl core::fmt::Display for Fid {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl CheapClone for Fid {
  #[inline]
  fn cheap_clone(&self) -> Self {
    *self
  }
}

impl Fid {
  /// Creates a new file ID.
  #[inline]
  pub const fn new(id: u63) -> Self {
    Self(id)
  }

  /// Encodes the file id into the given buffer.
  #[inline]
  pub fn encode(&self, buf: &mut [u8]) -> Result<usize, FidError> {
    encode_u64_varint((*self).into(), buf).map_err(FidError::Encode)
  }

  /// Encodes the file id into the given buffer.
  #[inline]
  pub fn encode_to_buffer(&self, buf: &mut VacantBuffer<'_>) -> Result<usize, FidError> {
    buf.put_u64_varint((*self).into()).map_err(FidError::Encode)
  }

  /// Decodes the file id from the given buffer.
  #[inline]
  pub fn decode(buf: &[u8]) -> Result<(usize, Self), FidError> {
    let (read, fid) = decode_u64_varint(buf).map_err(FidError::Decode)?;
    Fid::try_from(fid)
      .map(|fid| (read, fid))
      .map_err(FidError::TooLarge)
  }

  /// Returns the encoded length of the file ID.
  #[inline]
  pub fn encoded_len(&self) -> usize {
    encoded_u64_varint_len((*self).into())
  }
}

impl From<u63> for Fid {
  #[inline]
  fn from(id: u63) -> Self {
    Self(id)
  }
}

impl From<Fid> for u63 {
  #[inline]
  fn from(fid: Fid) -> Self {
    fid.0
  }
}

macro_rules! impl_from {
  ($($ty:ident), +$(,)?) => {
    $(
      impl From<$ty> for Fid {
        #[inline]
        fn from(id: $ty) -> Self {
          Self(id.into())
        }
      }
    )*
  };
}

impl_from!(u8, u16, u32);

impl TryFrom<u64> for Fid {
  type Error = LargeFid;

  #[inline]
  fn try_from(id: u64) -> Result<Self, Self::Error> {
    if id > u63::MAX.into() {
      Err(LargeFid(id))
    } else {
      Ok(Self(id.into()))
    }
  }
}

impl From<Fid> for u64 {
  #[inline]
  fn from(fid: Fid) -> Self {
    fid.0.into()
  }
}

impl From<&Fid> for u64 {
  #[inline]
  fn from(fid: &Fid) -> Self {
    fid.0.into()
  }
}

impl TryFrom<usize> for Fid {
  type Error = LargeFid;

  #[inline]
  fn try_from(id: usize) -> Result<Self, Self::Error> {
    (id as u64).try_into()
  }
}

/// An error indicating that the file ID is invalid.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LargeFid(u64);

impl From<u64> for LargeFid {
  #[inline]
  fn from(id: u64) -> Self {
    Self(id)
  }
}

impl core::fmt::Display for LargeFid {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(
      f,
      "invalid file id: {}, the maximum file id is {}",
      self.0,
      u63::MAX
    )
  }
}

impl core::error::Error for LargeFid {}

/// An error for the file ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FidError {
  /// Returned when the file ID is larger than the maximum file ID.
  TooLarge(LargeFid),
  /// Returned when the buffer is too small to encode the file ID.
  Encode(InsufficientBuffer),
  /// Returned when failed to decode the file ID.
  Decode(DecodeVarintError),
}

impl From<LargeFid> for FidError {
  #[inline]
  fn from(e: LargeFid) -> Self {
    Self::TooLarge(e)
  }
}

impl core::fmt::Display for FidError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::TooLarge(id) => id.fmt(f),
      Self::Encode(e) => write!(f, "failed to encode file id: {}", e),
      Self::Decode(e) => write!(f, "failed to decode file id: {}", e),
    }
  }
}

impl core::error::Error for FidError {}

/// An atomic file ID.
pub struct AtomicFid(AtomicU64);

impl AtomicFid {
  /// Creates a new atomic file ID.
  #[inline]
  pub fn new(fid: Fid) -> Self {
    Self(AtomicU64::new(fid.0.into()))
  }

  /// Creates a new atomic file ID with the initial value of `0`.
  #[inline]
  pub const fn zero() -> Self {
    Self(AtomicU64::new(0))
  }

  /// Loads the file ID with the given `order`.
  #[inline]
  pub fn load(&self, order: Ordering) -> Fid {
    Fid(self.0.load(order).into())
  }

  /// Stores the file ID with the given `order`.
  #[inline]
  pub fn increment(&self, order: Ordering) -> Fid {
    Fid(self.0.fetch_add(1, order).into())
  }
}
