use core::mem;

use dbutils::error::IncompleteBuffer;

use super::{Flags, ImmutableMeta};

/// The metadata with support TTL for the skip log.
///
/// The metadata is in the following layout:
///
///
///   ```text
///   +-----------------+------------------------+
///   |   1 byte flag   | 64 bits for expiration |
///   +-----------------+------------------------+
///   ```
#[derive(Copy, Clone, Eq, PartialEq)]
#[repr(C)]
pub struct Ttl {
  flags: Flags,
  expire_at: [u8; Self::EXPIRES_AT_SIZE],
}

impl core::fmt::Debug for Ttl {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Ttl")
      .field("pointer", &self.is_pointer())
      .field("expire_at", &self.expire_at())
      .finish()
  }
}

impl Ttl {
  /// The size of the metadata.
  pub const SIZE: usize = mem::size_of::<Self>();
  /// The size of the expiration.
  pub const EXPIRES_AT_SIZE: usize = mem::size_of::<u64>();

  /// Create a new metadata with the given version.
  #[inline]
  pub const fn inline(expire_at: u64) -> Self {
    Self {
      flags: Flags::empty(),
      expire_at: expire_at.to_le_bytes(),
    }
  }

  /// Create a new metadata with the given version and toggle the value pointer flag.
  #[inline]
  pub const fn pointer(expire_at: u64) -> Self {
    Self {
      flags: Flags::POINTER,
      expire_at: expire_at.to_le_bytes(),
    }
  }

  /// Set the value pointer flag.
  #[inline]
  pub fn set_pointer(&mut self) {
    self.flags |= Flags::POINTER;
  }

  /// Returns `true` if the value of the entry is a value pointer.
  #[inline]
  pub const fn is_pointer(&self) -> bool {
    self.flags.contains(Flags::POINTER)
  }

  /// Returns the timestamp of the expiration time.
  #[inline]
  pub const fn expire_at(&self) -> u64 {
    u64::from_le_bytes(self.expire_at)
  }

  /// Encode the metadata into a buffer.
  #[inline]
  pub const fn encode(&self) -> [u8; Self::SIZE] {
    let mut buf = [0; Self::SIZE];
    buf[0] = self.flags.bits();

    // SAFETY: the buffer is correctly sized.
    unsafe {
      core::ptr::copy_nonoverlapping(
        self.expire_at.as_ptr(),
        buf.as_mut_ptr().add(1),
        Self::EXPIRES_AT_SIZE,
      );
    }

    buf
  }

  /// Decodes a metadata from the given buffer.
  ///
  /// ## Panics
  /// - If the buffer is less than `Ttl::SIZE`.
  #[inline]
  pub const fn decode(buf: &[u8]) -> Result<Self, IncompleteBuffer> {
    let len = buf.len();
    if len < Self::SIZE {
      return Err(IncompleteBuffer::with_information(Self::SIZE as u64, len as u64));
    }

    let mut expire_at_buf = [0; Self::EXPIRES_AT_SIZE];

    // SAFETY: the buffer is correctly sized.
    unsafe {
      core::ptr::copy_nonoverlapping(
        buf.as_ptr().add(1),
        expire_at_buf.as_mut_ptr(),
        Self::EXPIRES_AT_SIZE,
      );
    }

    Ok(Self {
      flags: Flags::from_bits_truncate(buf[0]),
      expire_at: expire_at_buf,
    })
  }
}

impl ImmutableMeta for Ttl {}
