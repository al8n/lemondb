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
}
