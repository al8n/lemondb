use core::sync::atomic::{AtomicU16, AtomicU64, Ordering};

use skl::Trailer;
use zerocopy::{FromBytes, FromZeroes};

use crate::util::{decode_varint, encode_varint, encoded_len_varint, VarintError};

pub(crate) struct AtomicTableId(AtomicU16);

impl AtomicTableId {
  #[inline]
  pub(crate) const fn new(id: TableId) -> Self {
    Self(AtomicU16::new(id.0))
  }

  #[inline]
  pub(crate) const fn zero() -> Self {
    Self(AtomicU16::new(0))
  }

  #[inline]
  pub(crate) fn load(&self) -> TableId {
    TableId(self.0.load(Ordering::Acquire))
  }

  #[inline]
  pub(crate) fn increment(&self) -> TableId {
    TableId(self.0.fetch_add(1, Ordering::AcqRel))
  }
}

/// Table id
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(u16);

impl TableId {
  #[inline]
  pub(crate) const fn new(id: u16) -> Self {
    Self(id)
  }

  /// Returns the next file id.
  #[inline]
  pub(crate) const fn next(&self) -> Self {
    Self(self.0 + 1)
  }

  /// Increments the file id.
  #[inline]
  pub(crate) fn next_assign(&mut self) {
    self.0 += 1;
  }

  #[inline]
  pub(crate) fn max(&self, other: Self) -> Self {
    Self(self.0.max(other.0))
  }

  #[inline]
  pub(crate) fn encode(&self, buf: &mut [u8]) -> Result<usize, VarintError> {
    encode_varint(self.0 as u64, buf)
  }

  #[inline]
  pub(crate) fn decode(buf: &[u8]) -> Result<(usize, Self), VarintError> {
    let (read, id) = decode_varint(buf)?;
    Ok((read, Self(id as u16)))
  }

  #[inline]
  pub(crate) const fn encoded_len(&self) -> usize {
    encoded_len_varint(self.0 as u64)
  }
}

impl core::fmt::Display for TableId {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}

pub(crate) struct AtomicFid(AtomicU64);

impl AtomicFid {
  #[inline]
  pub(crate) const fn new(fid: Fid) -> Self {
    Self(AtomicU64::new(fid.0))
  }

  #[inline]
  pub(crate) const fn zero() -> Self {
    Self(AtomicU64::new(0))
  }

  #[inline]
  pub(crate) fn load(&self) -> Fid {
    Fid(self.0.load(Ordering::Acquire))
  }

  #[inline]
  pub(crate) fn increment(&self) -> Fid {
    Fid(self.0.fetch_add(1, Ordering::AcqRel))
  }
}

/// File id
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fid(u64);

impl Fid {
  /// use as a placeholder marker for the maximum file id.
  pub(crate) const MAX: Fid = Fid(u64::MAX);

  #[inline]
  pub(crate) const fn new(fid: u64) -> Self {
    Self(fid)
  }

  /// Returns the next file id.
  #[inline]
  pub(crate) const fn next(&self) -> Self {
    Self(self.0 + 1)
  }

  /// Increments the file id.
  #[inline]
  pub(crate) fn next_assign(&mut self) {
    self.0 += 1;
  }

  #[inline]
  pub(crate) fn max(&self, other: Self) -> Self {
    Self(self.0.max(other.0))
  }

  #[inline]
  pub(crate) fn encode(&self, buf: &mut [u8]) -> Result<usize, VarintError> {
    encode_varint(self.0, buf)
  }

  #[inline]
  pub(crate) fn decode(buf: &[u8]) -> Result<(usize, Self), VarintError> {
    let (read, fid) = decode_varint(buf)?;
    Ok((read, Self(fid)))
  }

  #[inline]
  pub(crate) const fn encoded_len(&self) -> usize {
    encoded_len_varint(self.0)
  }
}

impl core::fmt::Display for Fid {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{:020}", self.0)
  }
}

/// The metadata for the skip log.
///
/// The metadata is a 64-bit value with the following layout:
///
/// ```text
/// +---------------------+----------------------------------+----------------------+
/// | 63 bits for version |   1 bit for value pointer mark   | 32 bits for checksum |
/// +---------------------+----------------------------------+----------------------+
/// ```
#[derive(Copy, Clone, Eq, PartialEq, FromZeroes, FromBytes)]
#[repr(C, align(8))]
pub(crate) struct Meta {
  /// 63 bits for version, 1 bit for value pointer mark
  meta: u64,
  #[cfg(feature = "ttl")]
  expire_at: u64,
}

impl core::fmt::Debug for Meta {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    #[cfg(feature = "ttl")]
    return self
      .required_fields(f)
      .field("expire_at", &self.expire_at)
      .finish();

    #[cfg(not(feature = "ttl"))]
    self.required_fields(f).finish()
  }
}

impl Meta {
  pub(crate) const SIZE: usize = core::mem::size_of::<Self>();
  pub(crate) const VERSION_SIZE: usize = core::mem::size_of::<u64>();
  #[cfg(feature = "ttl")]
  pub(crate) const EXPIRES_AT_SIZE: usize = core::mem::size_of::<u64>();

  #[inline]
  fn required_fields<'a, 'b: 'a>(
    &'a self,
    f: &'a mut core::fmt::Formatter<'b>,
  ) -> core::fmt::DebugStruct<'a, 'b> {
    let mut s = f.debug_struct("Meta");
    s.field("version", &self.version())
      .field("pointer", &self.is_pointer());
    s
  }

  /// ## Panics
  /// - If the buffer is less than `Meta::VERSION_SIZE`.
  #[inline]
  pub(crate) fn decode_version(buf: &[u8]) -> u64 {
    u64::from_le_bytes(<[u8; Self::VERSION_SIZE]>::try_from(&buf[..Self::VERSION_SIZE]).unwrap())
      & Self::VERSION_MASK
  }
}

impl Meta {
  pub(crate) const VERSION_MASK: u64 = !0u64 >> 1; // 0xFFFFFFFFFFFFFFFE // 63 bits for version
  pub(crate) const VALUE_POINTER_FLAG: u64 = 1 << 63; // 64th bit for value pointer mark

  /// Create a new metadata with the given version.
  #[inline]
  pub const fn new(version: u64, #[cfg(feature = "ttl")] expire_at: u64) -> Self {
    assert!(version < (1 << 63), "version is too large");

    Self {
      meta: version,
      #[cfg(feature = "ttl")]
      expire_at,
    }
  }

  /// Create a new metadata with the given version and value pointer flag.
  #[inline]
  pub const fn value_pointer(mut version: u64, #[cfg(feature = "ttl")] expire_at: u64) -> Self {
    assert!(version < (1 << 63), "version is too large");

    version |= Self::VALUE_POINTER_FLAG;
    Self {
      meta: version,
      #[cfg(feature = "ttl")]
      expire_at,
    }
  }

  /// Set the value pointer flag.
  #[inline]
  pub fn set_value_pointer(&mut self) {
    self.meta |= Self::VALUE_POINTER_FLAG;
  }

  /// Returns `true` if the value of the entry is a value pointer.
  #[inline]
  pub const fn is_pointer(&self) -> bool {
    self.meta & Self::VALUE_POINTER_FLAG != 0
  }

  /// Returns the version.
  #[inline]
  pub fn version(&self) -> u64 {
    self.meta & Self::VERSION_MASK
  }

  /// Returns the timestamp of the expiration time.
  #[cfg(feature = "ttl")]
  #[inline]
  pub const fn expire_at(&self) -> u64 {
    self.expire_at
  }

  /// Returns the metadata as a raw 64-bit value.
  #[inline]
  pub(crate) const fn raw(&self) -> u64 {
    self.meta
  }
}

impl Trailer for Meta {
  #[cfg(feature = "ttl")]
  #[inline]
  fn is_valid(&self) -> bool {
    self.expire_at <= time::OffsetDateTime::now_utc().unix_timestamp() as u64
  }
}

/// Value pointer encode/decode error.
#[derive(Debug, Copy, Clone)]
pub enum PointerError {
  /// Buffer is too small to encode the value pointer.
  BufferTooSmall,
  /// Not enough bytes to decode the value pointer.
  NotEnoughBytes,
  /// Returned when encoding/decoding varint failed.
  VarintError(VarintError),
}

impl From<VarintError> for PointerError {
  #[inline]
  fn from(e: VarintError) -> Self {
    Self::VarintError(e)
  }
}

impl core::fmt::Display for PointerError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::BufferTooSmall => write!(f, "encode buffer too small"),
      Self::NotEnoughBytes => write!(f, "not enough bytes"),
      Self::VarintError(e) => write!(f, "{e}"),
    }
  }
}

impl std::error::Error for PointerError {}

/// A pointer to the bytes in the log.
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct Pointer {
  fid: Fid,
  size: u64,
  offset: u64,
}

impl Pointer {
  /// Create a new value pointer with the given file id, size, and offset.
  #[inline]
  pub const fn new(fid: Fid, size: u64, offset: u64) -> Self {
    Self { fid, size, offset }
  }

  /// Returns the id of the file which contains the value.
  #[inline]
  pub const fn fid(&self) -> Fid {
    self.fid
  }

  /// Returns the offset of the value in the file.
  #[inline]
  pub const fn offset(&self) -> u64 {
    self.offset
  }

  /// Returns the size of the value.
  #[inline]
  pub const fn size(&self) -> u64 {
    self.size
  }
}

impl Pointer {
  pub(crate) const MAX_ENCODING_SIZE: usize = 1 + 5 + 10 + 10; // 1 byte for encoded size and 3 varints

  /// Returns the encoded size of the value pointer.
  #[inline]
  pub const fn encoded_size(&self) -> usize {
    1 + self.fid.encoded_len() + encoded_len_varint(self.size) + encoded_len_varint(self.offset)
  }

  /// Encodes the value pointer into the buffer.
  pub fn encode(&self, buf: &mut [u8]) -> Result<usize, PointerError> {
    let encoded_size = self.encoded_size();
    if buf.len() < encoded_size {
      return Err(PointerError::BufferTooSmall);
    }

    let mut offset = 0;
    buf[offset] = encoded_size as u8;
    offset += 1;

    offset += self.fid.encode(&mut buf[offset..])?;
    offset += encode_varint(self.offset, &mut buf[offset..])?;
    offset += encode_varint(self.size, &mut buf[offset..])?;

    debug_assert_eq!(
      encoded_size, offset,
      "expected encoded size {} is not equal to actual encoded size {}",
      encoded_size, offset
    );
    Ok(offset)
  }

  /// Decodes the value pointer from the buffer.
  pub fn decode(buf: &[u8]) -> Result<(usize, Self), PointerError> {
    if buf.is_empty() {
      return Err(PointerError::NotEnoughBytes);
    }

    let encoded_size = buf[0] as usize;
    if buf.len() < encoded_size {
      return Err(PointerError::NotEnoughBytes);
    }

    let mut cur = 1;
    let (read, fid) = Fid::decode(&buf[cur..])?;
    cur += read;
    let (read, size) = decode_varint(&buf[cur..])?;
    cur += read;
    let (read, offset) = decode_varint(&buf[cur..])?;
    cur += read;
    debug_assert_eq!(
      encoded_size, cur,
      "expected read {} bytes is not equal to actual read bytes {}",
      encoded_size, cur
    );

    Ok((encoded_size, Self { fid, size, offset }))
  }
}
