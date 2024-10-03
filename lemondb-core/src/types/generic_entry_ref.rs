use dbutils::traits::Type;

use super::meta::Meta;

/// A reference to the entry in the database.
pub struct GenericEntryRef<'a, K, V>
where
  K: Type + ?Sized,
  V: Type + ?Sized,
{
  meta: Meta,
  key: K::Ref<'a>,
  value: V::Ref<'a>,
}

impl<'a, K, V> GenericEntryRef<'a, K, V>
where
  K: Type + ?Sized,
  V: Type + ?Sized,
{
  /// Creates a new entry reference.
  #[inline]
  pub const fn new(meta: Meta, key: K::Ref<'a>, value: V::Ref<'a>) -> Self {
    Self { meta, key, value }
  }

  /// Returns the version of this entry reference.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.meta.version()
  }

  /// Returns the key of this entry reference.
  #[inline]
  pub const fn key(&self) -> &K::Ref<'a> {
    &self.key
  }

  /// Returns the value of this entry reference.
  #[inline]
  pub const fn value(&self) -> &V::Ref<'a> {
    &self.value
  }

  /// Returns the expiration time of this entry reference.
  #[inline]
  #[cfg(feature = "ttl")]
  #[cfg_attr(docsrs, doc(cfg(feature = "ttl")))]
  pub const fn expire_at(&self) -> u64 {
    self.meta.expire_at()
  }
}
