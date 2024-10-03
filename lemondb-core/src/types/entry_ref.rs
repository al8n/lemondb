use super::key::Key;
use orderwal::swmr::generic::GenericEntryRef;

/// A reference to the entry in the database.
pub struct EntryRef<'a, C>(GenericEntryRef<'a, Key<C>, [u8]>);

impl<'a, C> EntryRef<'a, C> {
  /// Creates a new entry reference.
  #[inline]
  pub const fn new(ent: GenericEntryRef<'a, Key<C>, [u8]>) -> Self {
    Self(ent)
  }

  /// Returns the key of this entry reference.
  #[inline]
  pub const fn key(&self) -> &[u8] {
    self.0.key().key()
  }

  /// Returns the value of this entry reference.
  #[inline]
  pub fn value(&self) -> &[u8] {
    self.0.value().as_ref()
  }

  /// Returns the version of this entry reference.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.0.key().version()
  }

  /// Returns the expiration time of this entry reference.
  #[inline]
  #[cfg(feature = "ttl")]
  #[cfg_attr(docsrs, doc(cfg(feature = "ttl")))]
  pub const fn expire_at(&self) -> u64 {
    self.0.key().expire_at()
  }
}
