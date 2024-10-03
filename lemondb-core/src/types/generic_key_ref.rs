use core::cmp;

use dbutils::{
  equivalent::{Comparable, Equivalent},
  traits::{KeyRef, Type, TypeRef},
};

use super::{generic_key::GenericKey, meta::Meta, query::Query};

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

  /// Returns the version of this key reference.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.meta.version()
  }

  /// Returns the expiration time of this key reference.
  #[cfg(feature = "ttl")]
  #[cfg_attr(docsrs, doc(cfg(feature = "ttl")))]
  #[inline]
  pub const fn expire_at(&self) -> u64 {
    self.meta.expire_at()
  }

  /// Returns the `key` of the `GenericKeyRef`.
  #[inline]
  pub const fn key(&self) -> &K {
    &self.data
  }

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
      .then_with(|| other.meta.version().cmp(&self.meta.version())) // make sure the latest version is at the front
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

impl<'a, K> Equivalent<GenericKey<K>> for GenericKeyRef<K::Ref<'a>>
where
  K: ?Sized + Ord + Type + Equivalent<K::Ref<'a>>,
  for<'b> K::Ref<'b>: Equivalent<K> + Ord,
{
  #[inline]
  fn equivalent(&self, key: &GenericKey<K>) -> bool {
    self.data.equivalent(&key.data) && self.meta.version() == key.meta.version()
  }
}

impl<'a, K> Comparable<GenericKey<K>> for GenericKeyRef<K::Ref<'a>>
where
  K: ?Sized + Ord + Type + Comparable<K::Ref<'a>>,
  for<'b> K::Ref<'b>: Comparable<K> + Ord,
{
  #[inline]
  fn compare(&self, key: &GenericKey<K>) -> cmp::Ordering {
    Comparable::compare(&self.data, &key.data)
      .then_with(|| key.meta.version().cmp(&self.meta.version())) // make sure latest version at the front
  }
}

impl<'a, K> KeyRef<'a, GenericKey<K>> for GenericKeyRef<K::Ref<'a>>
where
  K: ?Sized + Ord + Type + Comparable<K::Ref<'a>>,
  for<'b> K::Ref<'b>: Comparable<K> + Ord,
{
  #[inline]
  fn compare<Q>(&self, a: &Q) -> cmp::Ordering
  where
    Q: ?Sized + Ord + Comparable<Self>,
  {
    Comparable::compare(a, self).reverse()
  }

  #[inline]
  unsafe fn compare_binary(a: &[u8], b: &[u8]) -> cmp::Ordering {
    let alen = a.len();
    let blen = b.len();

    let ak = &a[..alen - Meta::SIZE];
    let av = Meta::decode_version(&a[alen - Meta::SIZE..]);
    let bk = &b[..blen - Meta::SIZE];
    let bv = Meta::decode_version(&b[blen - Meta::SIZE..]);

    let ak = <K::Ref<'_> as TypeRef<'_>>::from_slice(ak);
    let bk = <K::Ref<'_> as TypeRef<'_>>::from_slice(bk);
    ak.cmp(&bk).then_with(|| bv.cmp(&av)) // make sure latest version at the front
  }
}

impl<'a, 'b: 'a, Q, K> Equivalent<GenericKeyRef<K::Ref<'a>>> for Query<'b, Q, K>
where
  K: Type + Ord + ?Sized,
  Q: ?Sized + Ord + Equivalent<K::Ref<'a>>,
{
  #[inline]
  fn equivalent(&self, key: &GenericKeyRef<K::Ref<'a>>) -> bool {
    self.key.equivalent(&key.data) && self.meta.version() == key.version()
  }
}

impl<'a, 'b: 'a, Q, K> Comparable<GenericKeyRef<K::Ref<'a>>> for Query<'b, Q, K>
where
  K: Type + Ord + ?Sized,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
{
  #[inline]
  fn compare(&self, key: &GenericKeyRef<K::Ref<'a>>) -> cmp::Ordering {
    Comparable::compare(self.key, &key.data)
      .then_with(move || key.meta.version().cmp(&self.meta.version())) // make sure latest version at the front
  }
}
