use super::{
  generic_key::Key,
  generic_value::{ValueRef, PhantomValue},
};
use dbutils::types::Type;
use orderwal::swmr::generic::GenericEntryRef as EntryRef;

/// A reference to the entry in the database.
pub struct GenericEntryRef<'a, K, V>(EntryRef<'a, Key<K>, PhantomValue<V>>)
where
  K: Type + ?Sized,
  V: Type + ?Sized;

impl<'a, K, V> GenericEntryRef<'a, K, V>
where
  K: Type + ?Sized,
  V: Type + ?Sized,
{
  /// Creates a new entry reference.
  #[inline]
  pub const fn new(ent: EntryRef<'a, Key<K>, PhantomValue<V>>) -> Self {
    Self(ent)
  }

  /// Returns the version of this entry reference.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.0.key().version()
  }

  /// Returns the key of this entry reference.
  #[inline]
  pub const fn key(&self) -> &K::Ref<'a> {
    self.0.key().key()
  }

  /// Returns the value of this entry reference.
  #[inline]
  pub const fn value(&self) -> &ValueRef<'a, V> {
    self.0.value()
  }

  /// Returns the expiration time of this entry reference.
  #[inline]
  #[cfg(feature = "ttl")]
  #[cfg_attr(docsrs, doc(cfg(feature = "ttl")))]
  pub const fn expire_at(&self) -> u64 {
    self.0.key().expire_at()
  }
}
