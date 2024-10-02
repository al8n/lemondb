use core::{cmp, marker::PhantomData};

use dbutils::{
  equivalent::{Comparable, Equivalent},
  traits::{KeyRef, Type},
  StaticComparator,
};
use orderwal::Generic;

use super::{generic_key_ref::GenericKeyRef, meta::Meta};

/// An internal generic key.
pub struct GenericKey<K: ?Sized> {
  pub(super) meta: Meta,
  pub(super) data: K,
}

impl<K> PartialEq for GenericKey<K>
where
  K: ?Sized + PartialEq,
{
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.meta.raw() == other.meta.raw() && self.data.eq(&other.data)
  }
}

impl<K> Eq for GenericKey<K> where K: ?Sized + Eq {}

impl<K> PartialOrd for GenericKey<K>
where
  K: ?Sized + Ord,
{
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<K> Ord for GenericKey<K>
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

impl<K> core::fmt::Debug for GenericKey<K>
where
  K: ?Sized + core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("GenericKey")
      .field("meta", &self.meta)
      .field("data", &&self.data)
      .finish()
  }
}

impl<K> Type for GenericKey<K>
where
  K: ?Sized + Type,
{
  type Ref<'a> = GenericKeyRef<K::Ref<'a>>;

  type Error = K::Error;

  #[inline]
  fn encoded_len(&self) -> usize {
    K::encoded_len(&self.data) + Meta::SIZE
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    let len = K::encode(&self.data, buf)?;
    buf[len..len + Meta::VERSION_SIZE].copy_from_slice(&self.meta.raw().to_le_bytes());
    buf[len + Meta::VERSION_SIZE..len + Meta::SIZE]
      .copy_from_slice(&self.meta.expire_at().to_le_bytes());
    Ok(len + Meta::SIZE)
  }
}

/// An internal generic key for querying.
pub struct GenericQueryKey<'a, K: ?Sized> {
  meta: Meta,
  data: Generic<'a, K>,
}

impl<'a, K: ?Sized> GenericQueryKey<'a, K> {
  /// Creates a new `GenericQueryKey`.
  #[inline]
  pub fn new(meta: Meta, data: Generic<'a, K>) -> Self {
    Self { meta, data }
  }
}

impl<'a, K> PartialEq for GenericQueryKey<'a, K>
where
  K: ?Sized + PartialEq + Type + for<'b> Equivalent<K::Ref<'b>>,
{
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.meta.raw() == other.meta.raw() && self.data.eq(&other.data)
  }
}

impl<'a, K> Eq for GenericQueryKey<'a, K> where
  K: ?Sized + PartialEq + Type + for<'b> Equivalent<K::Ref<'b>>
{
}

impl<'a, K> PartialOrd for GenericQueryKey<'a, K>
where
  K: ?Sized + Ord + Type + for<'b> Comparable<K::Ref<'b>>,
  for<'b> K::Ref<'b>: Comparable<K> + Ord,
{
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<'a, K> Ord for GenericQueryKey<'a, K>
where
  K: ?Sized + Ord + Type + for<'b> Comparable<K::Ref<'b>>,
  for<'b> K::Ref<'b>: Comparable<K> + Ord,
{
  #[inline]
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    self
      .data
      .cmp(&other.data)
      .then_with(|| self.meta.version().cmp(&other.meta.version()))
  }
}

impl<'a, K: ?Sized> Equivalent<GenericQueryKey<'a, K>> for GenericKey<K> {
  #[inline]
  fn equivalent(&self, key: &GenericQueryKey<'a, K>) -> bool {
    todo!()
  }
}

impl<'a, K: ?Sized> Comparable<GenericQueryKey<'a, K>> for GenericKey<K> {
  #[inline]
  fn compare(&self, key: &GenericQueryKey<'a, K>) -> cmp::Ordering {
    todo!()
  }
}

impl<'a, K: ?Sized> Equivalent<GenericKey<K>> for GenericQueryKey<'a, K> {
  #[inline]
  fn equivalent(&self, key: &GenericKey<K>) -> bool {
    todo!()
  }
}

impl<'a, K: ?Sized> Comparable<GenericKey<K>> for GenericQueryKey<'a, K> {
  #[inline]
  fn compare(&self, key: &GenericKey<K>) -> cmp::Ordering {
    todo!()
  }
}

impl<'a, K> KeyRef<'a, GenericKey<K>> for GenericQueryKey<'a, K>
where
  K: ?Sized + Ord + Type + for<'b> Comparable<K::Ref<'b>>,
  for<'b> K::Ref<'b>: Comparable<K> + Ord,
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
