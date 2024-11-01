use core::mem;

use dbutils::error::IncompleteBuffer;

use super::{Flags, ImmutableMeta};

/// The metadata with support TTL for the skip log.
///
/// The metadata is in the following layout:
///
///
///   ```text
///   +-----------------+
///   |   1 byte flag   |
///   +-----------------+
///   ```
#[derive(Copy, Clone, Eq, PartialEq)]
#[repr(C)]
pub struct Plain {
  flags: Flags,
}

impl core::fmt::Debug for Plain {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Plain")
      .field("pointer", &self.is_pointer())
      .finish()
  }
}

impl Plain {
  /// The size of the metadata.
  pub const SIZE: usize = mem::size_of::<Self>();

  /// Create a new metadata with the given version.
  #[inline]
  pub const fn inline() -> Self {
    Self {
      flags: Flags::empty(),
    }
  }

  /// Create a new metadata with the given version and toggle the value pointer flag.
  #[inline]
  pub const fn pointer() -> Self {
    Self {
      flags: Flags::POINTER,
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

  /// Encode the metadata into a buffer.
  #[inline]
  pub const fn encode(&self) -> [u8; Self::SIZE] {
    [self.flags.bits()]
  }

  /// Decodes a metadata from the given buffer.
  ///
  /// ## Panics
  /// - If the buffer is less than `Plain::SIZE`.
  #[inline]
  pub const fn decode(buf: &[u8]) -> Result<Self, IncompleteBuffer> {
    if buf.is_empty() {
      return Err(IncompleteBuffer::with_information(1, 0));
    }

    Ok(Self {
      flags: Flags::from_bits_truncate(buf[0]),
    })
  }
}

impl ImmutableMeta for Plain {}
