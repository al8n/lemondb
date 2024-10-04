use skl::Trailer;
use zerocopy::{FromBytes, FromZeroes};

/// The metadata for the skip log.
///
/// The metadata is in the following layout:
///
/// - With `ttl` feature enabled:
///
///   ```text
///   +---------------------+----------------------------------+------------------------+
///   | 63 bits for version |   1 bit for value pointer mark   | 64 bits for expiration |
///   +---------------------+----------------------------------+------------------------+
///   ```
///
/// - Without `ttl` feature enabled:
///
///   ```text
///   +---------------------+----------------------------------+
///   | 63 bits for version |   1 bit for value pointer mark   |
///   +---------------------+----------------------------------+
///   ```
#[derive(Copy, Clone, Eq, PartialEq, FromZeroes, FromBytes)]
#[repr(C, align(8))]
pub struct Meta {
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
  /// The size of the metadata.
  pub const SIZE: usize = core::mem::size_of::<Self>();
  /// The size of the version.
  pub const VERSION_SIZE: usize = core::mem::size_of::<u64>();
  /// The size of the expiration.
  #[cfg(feature = "ttl")]
  pub const EXPIRES_AT_SIZE: usize = core::mem::size_of::<u64>();

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

  /// Decodes a version from the given buffer.
  ///
  /// ## Panics
  /// - If the buffer is less than `Meta::VERSION_SIZE`.
  #[inline]
  pub fn decode_version(buf: &[u8]) -> u64 {
    u64::from_le_bytes(<[u8; Self::VERSION_SIZE]>::try_from(&buf[..Self::VERSION_SIZE]).unwrap())
      & Self::VERSION_MASK
  }

  /// Decodes a metadata from the given buffer.
  ///
  /// ## Panics
  /// - If the buffer is less than `Meta::SIZE`.
  #[inline]
  pub(crate) fn decode(buf: &[u8]) -> Self {
    let raw = u64::from_le_bytes([
      buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ]);
    #[cfg(feature = "ttl")]
    let expire_at = u64::from_le_bytes([
      buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
    ]);

    Self {
      meta: raw,
      #[cfg(feature = "ttl")]
      expire_at,
    }
  }
}

impl Meta {
  /// The maximum version.
  pub const MAX_VERSION: u64 = (1 << 63) - 1;
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

  /// Returns a new meta for lookup.
  #[inline]
  pub(crate) const fn query(version: u64) -> Self {
    assert!(version < (1 << 63), "version is too large");

    Self {
      meta: version,
      #[cfg(feature = "ttl")]
      expire_at: 0,
    }
  }

  /// Create a new metadata with the given version and toggle the value pointer flag.
  #[inline]
  pub const fn pointer(mut version: u64, #[cfg(feature = "ttl")] expire_at: u64) -> Self {
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
  pub fn set_pointer(&mut self) {
    self.meta |= Self::VALUE_POINTER_FLAG;
  }

  /// Returns `true` if the value of the entry is a value pointer.
  #[inline]
  pub const fn is_pointer(&self) -> bool {
    self.meta & Self::VALUE_POINTER_FLAG != 0
  }

  /// Returns the version.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.meta & Self::VERSION_MASK
  }

  /// Returns the timestamp of the expiration time.
  #[cfg(feature = "ttl")]
  #[inline]
  pub const fn expire_at(&self) -> u64 {
    self.expire_at
  }

  /// Returns the value pointer marker bit and version bits as a `u64`.
  #[inline]
  pub const fn raw(&self) -> u64 {
    self.meta
  }
}

impl Trailer for Meta {
  #[cfg(feature = "ttl")]
  #[inline]
  fn is_valid(&self) -> bool {
    // If the expiration time is 0, then it never expires.
    if self.expire_at == 0 {
      return true;
    }

    self.expire_at <= crate::utils::now_timestamp()
  }
}
