use bytes::Bytes;
use skl::{map::EntryRef as MapEntryRef, map::VersionedEntryRef as MapVersionedEntryRef, Trailer};

use crate::util::{decode_varint, encode_varint, encoded_len_varint, VarintError};

/// The metadata for the skip log.
///
/// The metadata is a 64-bit value with the following layout:
///
/// ```text
/// +---------------------+----------------------------------+------------------------------+----------------------+
/// | 62 bits for version | 1 bit for big value pointer mark | 1 bit for value pointer mark | 32 bits for checksum |
/// +---------------------+----------------------------------+------------------------------+----------------------+
/// ```
#[derive(Copy, Clone, Eq, PartialEq)]
#[repr(C, align(8))]
pub(crate) struct Meta {
  /// 62 bits for version, 1 bit for value pointer mark, and 1 bit for deletion flag.
  meta: u64,
  cks: u32,
}

impl core::fmt::Debug for Meta {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    let mut f = f.debug_struct("Meta");
    f.field("version", &self.version())
      .field("checksum", &self.cks);
    if self.is_big_value_pointer() || self.is_value_pointer() {
      f.field("pointer", &true).finish()
    } else {
      f.field("pointer", &false).finish()
    }
  }
}

unsafe impl Trailer for Meta {
  #[inline]
  fn version(&self) -> u64 {
    self.meta & Self::VERSION_MASK
  }
}

impl Meta {
  const VERSION_MASK: u64 = 0x3FFFFFFFFFFFFFFF; // 62 bits for version
  const BIG_VALUE_POINTER_FLAG: u64 = 1 << 62; // 63rd bit for big value pointer mark
  const VALUE_POINTER_FLAG: u64 = 1 << 63; // 64th bit for value pointer mark

  /// Create a new metadata with the given version.
  #[inline]
  pub const fn new(version: u64) -> Self {
    assert!(version < (1 << 62), "version is too large");

    Self {
      meta: version,
      cks: 0,
    }
  }

  /// Create a new metadata with the given version and value pointer flag.
  #[inline]
  pub const fn value_pointer(mut version: u64) -> Self {
    assert!(version < (1 << 62), "version is too large");

    version |= Self::VALUE_POINTER_FLAG;
    Self {
      meta: version,
      cks: 0,
    }
  }

  /// Create a new metadata with the given version and big value pointer flag.
  #[inline]
  pub const fn big_value_pointer(mut version: u64) -> Self {
    assert!(version < (1 << 62), "version is too large");

    version |= Self::BIG_VALUE_POINTER_FLAG;
    Self {
      meta: version,
      cks: 0,
    }
  }

  /// Set the checksum of the entry.
  #[inline]
  pub fn set_checksum(&mut self, cks: u32) {
    self.cks = cks;
  }

  /// Set the value pointer flag.
  #[inline]
  pub fn set_value_pointer(&mut self) {
    self.meta |= Self::VALUE_POINTER_FLAG;
  }

  /// Set the big value pointer flag.
  #[inline]
  pub fn set_big_value_pointer(&mut self) {
    self.meta |= Self::BIG_VALUE_POINTER_FLAG;
  }

  /// Returns the checksum of the entry.
  #[inline]
  pub const fn checksum(&self) -> u32 {
    self.cks
  }

  /// Returns `true` if the entry uses a big value pointer.
  #[inline]
  pub const fn is_big_value_pointer(&self) -> bool {
    self.meta & Self::BIG_VALUE_POINTER_FLAG != 0
  }

  /// Returns `true` if the value of the entry is a value pointer.
  #[inline]
  pub const fn is_value_pointer(&self) -> bool {
    self.meta & Self::VALUE_POINTER_FLAG != 0
  }

  /// Returns the metadata as a raw 64-bit value.
  #[inline]
  pub(crate) const fn raw(&self) -> u64 {
    self.meta
  }
}

/// A reference to an entry in the log.
#[derive(Debug, Copy, Clone)]
pub struct VersionedEntryRef<'a> {
  ent: MapVersionedEntryRef<'a, Meta>,
}

impl<'a> VersionedEntryRef<'a> {
  /// Returns the key of the entry.
  #[inline]
  pub const fn key(&self) -> &[u8] {
    self.ent.key()
  }

  /// Returns the value of the entry. `None` means the entry is removed.
  #[inline]
  pub const fn value(&self) -> Option<&[u8]> {
    self.ent.value()
  }

  /// Returns `true` if the value of the entry is a value pointer.
  #[inline]
  pub const fn is_value_pointer(&self) -> bool {
    self.ent.trailer().is_value_pointer()
  }

  /// Returns `true` if the value of the entry is a value pointer.
  #[inline]
  pub const fn is_big_value_pointer(&self) -> bool {
    self.ent.trailer().is_big_value_pointer()
  }

  /// Returns `true` if the value of the entry is removed.
  #[inline]
  pub const fn is_removed(&self) -> bool {
    self.ent.is_removed()
  }

  #[inline]
  pub(crate) const fn new(ent: MapVersionedEntryRef<'a, Meta>) -> Self {
    Self { ent }
  }
}

/// A reference to an entry in the log.
#[derive(Debug, Copy, Clone)]
pub struct EntryRef<'a> {
  ent: MapEntryRef<'a, Meta>,
}

impl<'a> EntryRef<'a> {
  /// Returns the key of the entry.
  #[inline]
  pub const fn key(&self) -> &[u8] {
    self.ent.key()
  }

  /// Returns the value of the entry.
  #[inline]
  pub const fn value(&self) -> &[u8] {
    self.ent.value()
  }

  /// Returns `true` if the value of the entry is a value pointer.
  #[inline]
  pub const fn is_value_pointer(&self) -> bool {
    self.ent.trailer().is_value_pointer()
  }

  /// Returns `true` if the value of the entry is a value pointer.
  #[inline]
  pub const fn is_big_value_pointer(&self) -> bool {
    self.ent.trailer().is_big_value_pointer()
  }

  #[inline]
  pub(crate) const fn new(ent: MapEntryRef<'a, Meta>) -> Self {
    Self { ent }
  }
}

/// An entry in the log.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Entry {
  key: Bytes,
  value: Bytes,
  meta: Meta,
}

impl Entry {
  /// Create a new entry with the given key, value, and metadata.
  #[inline]
  pub const fn new(key: Bytes, value: Bytes, meta: Meta) -> Self {
    Self { key, value, meta }
  }

  /// Returns the key of the entry.
  #[inline]
  pub const fn key(&self) -> &Bytes {
    &self.key
  }

  /// Returns the value of the entry.
  #[inline]
  pub const fn value(&self) -> &Bytes {
    &self.value
  }

  /// Returns the metadata of the entry.
  #[inline]
  pub const fn meta(&self) -> Meta {
    self.meta
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

#[cfg(feature = "std")]
impl std::error::Error for PointerError {}

/// A pointer to the bytes in the log.
pub struct Pointer {
  fid: u32,
  size: u64,
  offset: u64,
}

impl Pointer {
  /// Create a new value pointer with the given file id, size, and offset.
  #[inline]
  pub const fn new(fid: u32, size: u64, offset: u64) -> Self {
    Self { fid, size, offset }
  }

  /// Returns the id of the file which contains the value.
  #[inline]
  pub const fn fid(&self) -> u32 {
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
    1 + encoded_len_varint(self.fid as u64)
      + encoded_len_varint(self.size)
      + encoded_len_varint(self.offset)
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

    offset += encode_varint(self.offset, &mut buf[offset..])?;
    offset += encode_varint(self.size, &mut buf[offset..])?;
    offset += encode_varint(self.fid as u64, &mut buf[offset..])?;

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
    let (read, fid) = decode_varint(&buf[cur..])?;
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

    Ok((
      encoded_size,
      Self {
        fid: fid as u32,
        size,
        offset,
      },
    ))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_meta() {
    let meta = Meta::new(0);
    assert_eq!(meta.version(), 0);
    assert!(!meta.is_value_pointer());
    assert!(!meta.is_big_value_pointer());

    let meta = Meta::new(100);
    assert_eq!(meta.version(), 100);
    assert!(!meta.is_value_pointer());
    assert!(!meta.is_big_value_pointer());

    assert_eq!(
      format!("{:?}", meta),
      "Meta { version: 101, removed: true, pointer: false, checksum: 0 }"
    );

    let meta = Meta::value_pointer(102);
    assert_eq!(meta.version(), 102);
    assert!(meta.is_value_pointer());

    let meta = Meta::big_value_pointer(102);
    assert_eq!(meta.version(), 102);
    assert!(!meta.is_value_pointer());
    assert!(meta.is_big_value_pointer());

    assert_eq!(
      format!("{:?}", meta),
      "Meta { version: 102, removed: false, pointer: true, checksum: 0 }"
    );
  }
}
