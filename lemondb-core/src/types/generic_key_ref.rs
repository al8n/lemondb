use core::{cmp, marker::PhantomData};

use dbutils::{
  equivalent::{Comparable, Equivalent},
  traits::{KeyRef, Type, TypeRef},
  StaticComparator,
};

use super::{
  generic_key::{GenericKey, GenericQueryKey},
  key::Key,
  meta::Meta,
};

/// A reference to a internal key.
pub struct GenericKeyRef<K: ?Sized> {
  meta: Meta,
  data: K,
}

impl<K> GenericKeyRef<K> {
  // /// Creates a new `KeyRef` with the given `meta` and `data`.
  // #[inline]
  // pub fn new(meta: Meta, data: &K) -> Self {
  //   Self {
  //     meta,
  //     data,
  //   }
  // }

  /// Consumes the `GenericKeyRef` and returns the `meta` and the `K`.
  #[inline]
  pub fn into_components(self) -> (Meta, K) {
    (self.meta, self.data)
  }
}

impl<K> PartialEq for GenericKeyRef<K>
where
  K: ?Sized + PartialEq,
{
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.data.eq(&other.data) && self.meta.version() == other.meta.version()
  }
}

impl<K> Eq for GenericKeyRef<K> where K: ?Sized + Eq {}

impl<K> PartialOrd for GenericKeyRef<K>
where
  K: ?Sized + Ord,
{
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<K> Ord for GenericKeyRef<K>
where
  K: ?Sized + Ord,
{
  #[inline]
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    self
      .data
      .cmp(&other.data)
      .then_with(|| self.meta.version().cmp(&other.meta.version()))
  }
}

impl<K> core::fmt::Debug for GenericKeyRef<K>
where
  K: ?Sized + core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct(core::any::type_name::<Self>())
      .field("meta", &self.meta)
      .field("key", &&self.data)
      .finish()
  }
}

impl<'a, K> TypeRef<'a> for GenericKeyRef<K>
where
  K: TypeRef<'a>,
{
  #[inline]
  unsafe fn from_slice(buf: &'a [u8]) -> Self {
    let len = buf.len();
    let key_end = len - Meta::SIZE;
    let meta = Meta::decode(&buf[key_end..]);
    let data = &buf[..key_end];

    Self {
      meta,
      data: K::from_slice(data),
    }
  }
}

impl<'a, K: Type + ?Sized> Equivalent<GenericKeyRef<K::Ref<'a>>> for GenericKey<K> {
  #[inline]
  fn equivalent(&self, key: &GenericKeyRef<K::Ref<'a>>) -> bool {
    todo!()
  }
}

impl<'a, K: Type + ?Sized> Comparable<GenericKeyRef<K::Ref<'a>>> for GenericKey<K> {
  #[inline]
  fn compare(&self, key: &GenericKeyRef<K::Ref<'a>>) -> cmp::Ordering {
    todo!()
  }
}

impl<'a, K: Type + ?Sized> Equivalent<GenericKey<K>> for GenericKeyRef<K::Ref<'a>> {
  #[inline]
  fn equivalent(&self, key: &GenericKey<K>) -> bool {
    todo!()
  }
}

impl<'a, K: Type + ?Sized> Comparable<GenericKey<K>> for GenericKeyRef<K::Ref<'a>> {
  #[inline]
  fn compare(&self, key: &GenericKey<K>) -> cmp::Ordering {
    todo!()
  }
}

impl<'a, K: Type + ?Sized> Equivalent<GenericKeyRef<K::Ref<'a>>> for GenericQueryKey<'a, K> {
  #[inline]
  fn equivalent(&self, key: &GenericKeyRef<K::Ref<'a>>) -> bool {
    todo!()
  }
}

impl<'a, K: Type + ?Sized> Comparable<GenericKeyRef<K::Ref<'a>>> for GenericQueryKey<'a, K> {
  #[inline]
  fn compare(&self, key: &GenericKeyRef<K::Ref<'a>>) -> cmp::Ordering {
    todo!()
  }
}

impl<'a, K> KeyRef<'a, GenericKey<K>> for GenericKeyRef<K::Ref<'a>>
where
  K: ?Sized + Ord + Type + Comparable<K::Ref<'a>>,
  K::Ref<'a>: Comparable<K> + Ord,
{
  #[inline]
  fn compare<Q>(&self, a: &Q) -> cmp::Ordering
  where
    Q: ?Sized + Ord + Comparable<Self>,
  {
    todo!()
  }

  #[inline]
  unsafe fn compare_binary(a: &[u8], b: &[u8]) -> cmp::Ordering {
    todo!()
  }
}
