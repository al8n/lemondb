use dbutils::buffer::VacantBuffer;
use zerocopy::{FromBytes, FromZeroes};

/// The metadata for the value log.
///
/// The metadata is in the following layout:
///
/// - With `ttl` feature enabled:
///
///   ```text
///   +---------------------+------------------------------+------------------------+
///   | 63 bits for version |   1 bit for tombstone mark   | 64 bits for expiration |
///   +---------------------+------------------------------+------------------------+
///   ```
///
/// - Without `ttl` feature enabled:
///
///   ```text
///   +---------------------+----------------------------------+
///   | 63 bits for version |   1 bit for tombstone mark   |
///   +---------------------+----------------------------------+
///   ```
#[derive(Copy, Clone, Eq, PartialEq, FromZeroes, FromBytes)]
#[repr(C, align(8))]
pub struct Meta {
  /// 63 bits for version, 1 bit for tombstone mark
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

  #[inline]
  fn required_fields<'a, 'b: 'a>(
    &'a self,
    f: &'a mut core::fmt::Formatter<'b>,
  ) -> core::fmt::DebugStruct<'a, 'b> {
    let mut s = f.debug_struct("Meta");
    s.field("version", &self.version())
      .field("pointer", &self.is_tombstone());
    s
  }

  /// Encodes self into the given buffer.
  ///
  /// ## Panics
  /// - If the buffer is less than `Meta::SIZE`.
  #[inline]
  pub(crate) fn encode(&self, buf: &mut [u8]) {
    buf[..Self::VERSION_SIZE].copy_from_slice(&self.meta.to_le_bytes());
    #[cfg(feature = "ttl")]
    buf[Self::VERSION_SIZE..].copy_from_slice(&self.expire_at.to_le_bytes());
  }

  /// Encodes self into the given buffer.
  ///
  /// ## Panics
  /// - If the buffer is less than `Meta::SIZE`.
  #[inline]
  pub(crate) fn encode_to_buffer(&self, buf: &mut VacantBuffer<'_>) {
    buf.put_u64_le_unchecked(self.meta);
    #[cfg(feature = "ttl")]
    buf.put_u64_le_unchecked(self.expire_at);
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
  pub(crate) const VERSION_MASK: u64 = !0u64 >> 1; // 0xFFFFFFFFFFFFFFFE // 63 bits for version
  pub(crate) const TOMBSTONE_FLAG: u64 = 1 << 63; // 64th bit for tombstone mark

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

  /// Set the tombstone flag.
  #[inline]
  pub fn with_tombstone(mut self) -> Self {
    self.meta |= Self::TOMBSTONE_FLAG;
    self
  }

  /// Returns `true` if the value of the entry is a tombstone.
  #[inline]
  pub const fn is_tombstone(&self) -> bool {
    self.meta & Self::TOMBSTONE_FLAG != 0
  }

  /// Returns the version.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.meta & Self::VERSION_MASK
  }
}

impl From<crate::types::meta::Meta> for Meta {
  #[inline]
  fn from(meta: crate::types::meta::Meta) -> Self {
    Self::new(
      meta.version(),
      #[cfg(feature = "ttl")]
      meta.expire_at(),
    )
  }
}
