use core::{cmp, marker::PhantomData};

use dbutils::{
  equivalent::{Comparable, Equivalent},
  traits::TypeRef,
  StaticComparator,
};

use super::{key::Key, meta::Meta, query::Query};

/// A reference to a internal key.
pub struct KeyRef<'a, C> {
  meta: Meta,
  data: &'a [u8],
  _phantom: PhantomData<C>,
}

impl<'a, C> KeyRef<'a, C> {
  /// Creates a new `KeyRef` with the given `meta` and `data`.
  #[inline]
  pub fn new(meta: Meta, data: &'a [u8]) -> Self {
    Self {
      meta,
      data,
      _phantom: PhantomData,
    }
  }

  /// Returns the version of this key reference.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.meta.version()
  }

  /// Returns the expiration time of this key reference.
  #[inline]
  #[cfg(feature = "ttl")]
  #[cfg_attr(docsrs, doc(cfg(feature = "ttl")))]
  pub const fn expire_at(&self) -> u64 {
    self.meta.expire_at()
  }

  /// Returns the `key` of the `KeyRef`.
  #[inline]
  pub const fn key(&self) -> &[u8] {
    self.data
  }
}

impl<'a, C> PartialEq for KeyRef<'a, C> {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.data.eq(other.data) && self.meta.version() == other.meta.version()
  }
}

impl<'a, C> Eq for KeyRef<'a, C> {}

impl<'a, C> PartialOrd for KeyRef<'a, C>
where
  C: StaticComparator,
{
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<'a, C> Ord for KeyRef<'a, C>
where
  C: StaticComparator,
{
  #[inline]
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    C::compare(self.data, other.data).then_with(|| other.meta.version().cmp(&self.meta.version()))
    // make sure latest version at the front
  }
}

impl<'a, C> core::fmt::Debug for KeyRef<'a, C> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct(core::any::type_name::<Self>())
      .field("meta", &self.meta)
      .field("key", &&self.data)
      .finish()
  }
}

impl<'a, C> TypeRef<'a> for KeyRef<'a, C> {
  #[inline]
  unsafe fn from_slice(buf: &'a [u8]) -> Self {
    let len = buf.len();
    let key_end = len - Meta::SIZE;
    let meta = Meta::decode(&buf[key_end..]);
    let data = &buf[..key_end];

    Self {
      meta,
      data,
      _phantom: PhantomData,
    }
  }
}

impl<'a, 'b, C: StaticComparator> dbutils::traits::KeyRef<'b, Key<C>> for KeyRef<'a, C> {
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

    C::compare(ak, bk).then_with(|| bv.cmp(&av)) // make sure latest version at the front
  }
}

impl<'a, C> Equivalent<Key<C>> for KeyRef<'a, C> {
  fn equivalent(&self, key: &Key<C>) -> bool {
    self.meta.version() == key.meta.version() && self.data.eq(&key.data)
  }
}

impl<'a, C> Comparable<Key<C>> for KeyRef<'a, C>
where
  C: StaticComparator,
{
  fn compare(&self, key: &Key<C>) -> std::cmp::Ordering {
    C::compare(self.data, &key.data).then_with(|| key.meta.version().cmp(&self.meta.version()))
    // make sure latest version at the front
  }
}

impl<'a, C> Equivalent<KeyRef<'a, C>> for Key<C> {
  fn equivalent(&self, key: &KeyRef<'a, C>) -> bool {
    key.meta.version() == self.meta.version() && key.data.eq(&self.data)
  }
}

impl<'a, C> Comparable<KeyRef<'a, C>> for Key<C>
where
  C: StaticComparator,
{
  fn compare(&self, key: &KeyRef<'a, C>) -> std::cmp::Ordering {
    C::compare(&self.data, key.data).then_with(|| key.meta.version().cmp(&self.meta.version()))
    // make sure latest version at the front
  }
}

impl<'a, C> Equivalent<KeyRef<'_, C>> for Query<'a, [u8], Key<C>>
where
  C: StaticComparator,
{
  #[inline]
  fn equivalent(&self, key: &KeyRef<'_, C>) -> bool {
    C::compare(self.key, key.data).is_eq() && self.meta.version() == key.version()
  }
}

impl<'a, C> Comparable<KeyRef<'_, C>> for Query<'a, [u8], Key<C>>
where
  C: StaticComparator,
{
  #[inline]
  fn compare(&self, key: &KeyRef<'_, C>) -> cmp::Ordering {
    C::compare(self.key, key.data).then_with(|| key.meta.version().cmp(&self.meta.version()))
    // make sure latest version at the front
  }
}
