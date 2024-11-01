use core::cmp;

use dbutils::{
  equivalent::{Comparable, Equivalent},
  types::{KeyRef as TKeyRef, Type, TypeRef},
};

use super::{active_meta::ActiveMeta, key::Key};

/// A reference to a internal key.
pub struct KeyRef<'a, K>
where
  K: ?Sized + Type,
{
  meta: ActiveMeta,
  data: K::Ref<'a>,
}

impl<K> Clone for KeyRef<'_, K>
where
  K: ?Sized + Type,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      meta: self.meta,
      data: self.data,
    }
  }
}

impl<K> Copy for KeyRef<'_, K> where K: ?Sized + Type {}

impl<'a, K> KeyRef<'a, K>
where
  K: ?Sized + Type,
{
  // /// Creates a new `KeyRef` with the given `meta` and `data`.
  // #[inline]
  // pub fn new(meta: Meta, data: &K) -> Self {
  //   Self {
  //     meta,
  //     data,
  //   }
  // }

  /// Returns the expiration time of this key reference.
  #[cfg(feature = "ttl")]
  #[cfg_attr(docsrs, doc(cfg(feature = "ttl")))]
  #[inline]
  pub const fn expire_at(&self) -> u64 {
    self.meta.expire_at()
  }

  /// Returns the `key` of the `KeyRef`.
  #[inline]
  pub const fn key(&self) -> &K::Ref<'a> {
    &self.data
  }

  /// Consumes the `KeyRef` and returns the `meta` and the `K`.
  #[inline]
  pub fn into_components(self) -> (ActiveMeta, K::Ref<'a>) {
    (self.meta, self.data)
  }
}

impl<'a, K> PartialEq for KeyRef<'a, K>
where
  K: ?Sized + Type,
  K::Ref<'a>: PartialEq,
{
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.data.eq(&other.data)
  }
}

impl<'a, K> Eq for KeyRef<'a, K>
where
  K: ?Sized + Type,
  K::Ref<'a>: Eq,
{
}

impl<'a, K> PartialOrd for KeyRef<'a, K>
where
  K: ?Sized + Type,
  K::Ref<'a>: PartialOrd,
{
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
    self.data.partial_cmp(&other.data)
  }
}

impl<'a, K> Ord for KeyRef<'a, K>
where
  K: ?Sized + Type,
  K::Ref<'a>: Ord,
{
  #[inline]
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    self.data.cmp(&other.data)
  }
}

impl<K> core::fmt::Debug for KeyRef<'_, K>
where
  K: ?Sized + Type,
  for<'a> K::Ref<'a>: core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct(core::any::type_name::<Self>())
      .field("meta", &self.meta)
      .field("key", &&self.data)
      .finish()
  }
}

impl<'a, K> TypeRef<'a> for KeyRef<'a, K>
where
  K: ?Sized + Type,
{
  #[inline]
  unsafe fn from_slice(buf: &'a [u8]) -> Self {
    let meta = ActiveMeta::decode(buf);
    let data = &buf[ActiveMeta::SIZE..];

    Self {
      meta,
      data: <K::Ref<'_> as TypeRef<'_>>::from_slice(data),
    }
  }
}

impl<'a, K> Equivalent<Key<K>> for KeyRef<'a, K>
where
  K: ?Sized + Type + Equivalent<K::Ref<'a>>,
{
  #[inline]
  fn equivalent(&self, key: &Key<K>) -> bool {
    key.data.equivalent(&self.data)
  }
}

impl<'a, K> Comparable<Key<K>> for KeyRef<'a, K>
where
  K: ?Sized + Type + Comparable<K::Ref<'a>>,
{
  #[inline]
  fn compare(&self, key: &Key<K>) -> cmp::Ordering {
    Comparable::compare(&key.data, &self.data).reverse()
  }
}

impl<'a, K> TKeyRef<'a, Key<K>> for KeyRef<'a, K>
where
  K: ?Sized + Type + Comparable<K::Ref<'a>>,
  K::Ref<'a>: TKeyRef<'a, K>,
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
    let ak = &a[ActiveMeta::SIZE..];
    let bk = &b[ActiveMeta::SIZE..];

    <K::Ref<'_> as TKeyRef<'_, K>>::compare_binary(ak, bk)
  }
}

// impl<'a, 'b: 'a, Q, K> Equivalent<KeyRef<K::Ref<'a>>> for Query<'b, Q, K>
// where
//   K: Type + Ord + ?Sized,
//   Q: ?Sized + Ord + Equivalent<K::Ref<'a>>,
// {
//   #[inline]
//   fn equivalent(&self, key: &KeyRef<K::Ref<'a>>) -> bool {
//     self.key.equivalent(&key.data) && self.meta.version() == key.version()
//   }
// }

// impl<'a, 'b: 'a, Q, K> Comparable<KeyRef<K::Ref<'a>>> for Query<'b, Q, K>
// where
//   K: Type + Ord + ?Sized,
//   Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
// {
//   #[inline]
//   fn compare(&self, key: &KeyRef<K::Ref<'a>>) -> cmp::Ordering {
//     Comparable::compare(self.key, &key.data)
//       .then_with(move || key.meta.version().cmp(&self.meta.version())) // make sure latest version at the front
//   }
// }
