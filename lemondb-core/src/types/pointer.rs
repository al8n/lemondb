use core::mem;

use valog::ValuePointer;

use super::fid::Fid;

/// Returned when the encoded buffer is too small to hold the bytes format of the [`Pointer`].
#[derive(Debug)]
pub struct InsufficientBuffer {
  required: usize,
  actual: usize,
}

impl InsufficientBuffer {
  /// Creates a new instance of the error.
  #[inline]
  const fn new(required: usize, actual: usize) -> Self {
    Self { required, actual }
  }
}

impl core::fmt::Display for InsufficientBuffer {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(
      f,
      "Insufficient buffer capacity: required {} bytes, but only {} bytes were provided",
      self.required, self.actual
    )
  }
}

impl core::error::Error for InsufficientBuffer {}

/// Returned when the buffer does not contains engouth bytes for decoding.
#[derive(Debug)]
pub struct IncompleteBuffer {
  required: usize,
  actual: usize,
}

impl IncompleteBuffer {
  /// Creates a new instance of the error.
  #[inline]
  const fn new(required: usize, actual: usize) -> Self {
    Self { required, actual }
  }
}

impl core::fmt::Display for IncompleteBuffer {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(
      f,
      "Incomplete buffer data: expected {} bytes for decoding, but only {} bytes were available",
      self.required, self.actual
    )
  }
}

impl core::error::Error for IncompleteBuffer {}

/// A pointer which points to an entry with a large value in value log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Pointer {
  ptr: ValuePointer<Fid>,
  tombstone: bool,
}

impl Pointer {
  /// The encoded size of the pointer.
  pub const ENCODED_LEN: usize = mem::size_of::<u64>() + mem::size_of::<u32>() * 2;

  pub(crate) const FID_MASK: u64 = !0u64 >> 1; // 0xFFFFFFFFFFFFFFFE // 63 bits for fid.
  pub(crate) const TOMBSTONE_FLAG: u64 = 1 << 63; // 64th bit for tombstone mark

  /// Creates a new `ValuePointer` with the given `offset` and `size`.
  #[inline]
  pub(crate) const fn new(ptr: ValuePointer<Fid>) -> Self {
    Self {
      ptr,
      tombstone: ptr.is_tombstone(),
    }
  }

  /// Returns the file id of the pointer.
  #[inline]
  pub const fn id(&self) -> Fid {
    *self.ptr.id()
  }

  /// Returns the offset of the value.
  #[inline]
  pub const fn offset(&self) -> u32 {
    self.ptr.offset()
  }

  /// Returns the size of the value.
  #[inline]
  pub const fn size(&self) -> u32 {
    self.ptr.size()
  }

  /// Returns `true` if this pointer points to a tombstone entry.
  #[inline]
  pub const fn is_tombstone(&self) -> bool {
    self.tombstone
  }

  /// Encodes the pointer into the given `buf`.
  #[inline]
  pub fn encode(&self, buf: &mut [u8]) -> Result<(), InsufficientBuffer> {
    const ID_SIZE: usize = mem::size_of::<u64>();
    const OFFSET_SIZE: usize = mem::size_of::<u32>();
    const SIZE_SIZE: usize = mem::size_of::<u32>();

    let buf_len = buf.len();
    if buf_len < Self::ENCODED_LEN {
      return Err(InsufficientBuffer::new(Self::ENCODED_LEN, buf_len));
    }

    let mut fid: u64 = self.id().into();
    if self.tombstone {
      fid |= Self::TOMBSTONE_FLAG;
    }

    buf[..ID_SIZE].copy_from_slice(&fid.to_le_bytes());
    buf[ID_SIZE..ID_SIZE + OFFSET_SIZE].copy_from_slice(&self.offset().to_le_bytes());
    buf[ID_SIZE + OFFSET_SIZE..ID_SIZE + OFFSET_SIZE + SIZE_SIZE]
      .copy_from_slice(&self.size().to_le_bytes());

    Ok(())
  }

  /// Decodes a pointer from the given `buf`.
  #[inline]
  pub fn decode(buf: &[u8]) -> Result<Self, IncompleteBuffer> {
    let buf_len = buf.len();
    if buf_len < Self::ENCODED_LEN {
      return Err(IncompleteBuffer::new(Self::ENCODED_LEN, buf_len));
    }

    let mut id = u64::from_le_bytes([
      buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ]);
    let offset = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
    let size = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);

    let tombstone = id & Self::TOMBSTONE_FLAG != 0;

    // clear the tombstone flag
    id &= Self::FID_MASK;

    Ok(Self {
      ptr: ValuePointer::new(id.try_into().unwrap(), offset, size),
      tombstone,
    })
  }
}
