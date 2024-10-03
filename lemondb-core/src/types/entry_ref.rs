use super::key_ref::KeyRef;

/// A reference to the entry in the database.
pub struct EntryRef<'a, C> {
  key: KeyRef<'a, C>,
  value: &'a [u8],
}

impl<'a, C> EntryRef<'a, C> {
  /// Creates a new entry reference.
  #[inline]
  pub const fn new(key: KeyRef<'a, C>, value: &'a [u8]) -> Self {
    Self { key, value }
  }

  /// Returns the key of this entry reference.
  #[inline]
  pub const fn key(&self) -> &[u8] {
    self.key.key()
  }

  /// Returns the value of this entry reference.
  #[inline]
  pub const fn value(&self) -> &'a [u8] {
    self.value
  }

  /// Returns the version of this entry reference.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.key.version()
  }

  /// Returns the expiration time of this entry reference.
  #[inline]
  #[cfg(feature = "ttl")]
  #[cfg_attr(docsrs, doc(cfg(feature = "ttl")))]
  pub const fn expire_at(&self) -> u64 {
    self.key.expire_at()
  }
}
