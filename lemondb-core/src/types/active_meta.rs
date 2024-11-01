use core::{mem, ptr};

/// The metadata for the active log.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(C)]
pub struct ActiveMeta {
  pointer: bool,
  #[cfg(feature = "ttl")]
  expire_at: [u8; Self::EXPIRES_AT_SIZE], // use an array to save memory size here.
}

impl ActiveMeta {
  /// The size of the metadata.
  pub const SIZE: usize = mem::size_of::<Self>();
  /// The size of the expiration.
  #[cfg(feature = "ttl")]
  pub const EXPIRES_AT_SIZE: usize = mem::size_of::<u64>();

  #[inline]
  pub(crate) const fn encode(&self) -> [u8; Self::SIZE] {
    let mut buf = [0; Self::SIZE];
    buf[0] = self.pointer as u8;
    #[cfg(feature = "ttl")]
    unsafe {
      ptr::copy_nonoverlapping(
        self.expire_at.as_ptr(),
        buf.as_mut_ptr().add(1),
        Self::EXPIRES_AT_SIZE,
      );
    }
    buf
  }

  /// ## Panics
  /// - If the buffer is less than `ActiveMeta::SIZE`.
  #[inline]
  pub(crate) const fn decode(buf: &[u8]) -> Self {
    assert!(
      buf.len() >= Self::SIZE,
      "incomplete buffer to decode ActiveMeta"
    );

    let pointer = buf[0] != 0;
    #[cfg(feature = "ttl")]
    let mut expire_at = [0; Self::EXPIRES_AT_SIZE];
    #[cfg(feature = "ttl")]
    unsafe {
      ptr::copy_nonoverlapping(
        buf.as_ptr().add(1),
        expire_at.as_mut_ptr(),
        Self::EXPIRES_AT_SIZE,
      );
    }
    Self {
      pointer,
      #[cfg(feature = "ttl")]
      expire_at,
    }
  }
}

impl ActiveMeta {
  /// Create a new metadata with the given version.
  #[inline]
  pub const fn new(pointer: bool, #[cfg(feature = "ttl")] expire_at: u64) -> Self {
    Self {
      pointer,
      #[cfg(feature = "ttl")]
      expire_at: expire_at.to_le_bytes(),
    }
  }

  /// Set the value pointer flag.
  #[inline]
  pub fn set_pointer(&mut self) {
    self.pointer = true;
  }

  /// Returns `true` if the value of the entry is a value pointer.
  #[inline]
  pub const fn pointer(&self) -> bool {
    self.pointer
  }

  /// Returns the timestamp of the expiration time.
  #[cfg(feature = "ttl")]
  #[inline]
  pub const fn expire_at(&self) -> u64 {
    u64::from_le_bytes(self.expire_at)
  }
}
