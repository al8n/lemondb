use std::{cmp, marker::PhantomData};

use dbutils::{
  equivalent::{Comparable, Equivalent},
  traits::{KeyRef, Type, TypeRef},
  StaticComparator,
};

use super::Meta;

pub(crate) struct Key<C> {
  meta: Meta,
  _phantom: PhantomData<C>,
  data: [u8],
}

impl<C: StaticComparator> PartialEq for Key<C> {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.meta.raw() == other.meta.raw() && self.data.eq(&other.data)
  }
}

impl<C: StaticComparator> Eq for Key<C> {}

impl<C: StaticComparator> PartialOrd for Key<C> {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<C: StaticComparator> Ord for Key<C> {
  #[inline]
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    C::compare(&self.data, &other.data).then_with(|| self.meta.version().cmp(&other.meta.version()))
  }
}

impl<C> core::fmt::Debug for Key<C> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Key")
      .field("meta", &self.meta)
      .field("data", &&self.data)
      .finish()
  }
}

impl<C> Type for Key<C> {
  type Ref<'a> = RefKey<'a, C>;

  type Error = ();

  #[inline]
  fn encoded_len(&self) -> usize {
    self.data.len() + Meta::SIZE
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    let len = self.data.len();
    buf[..len].copy_from_slice(&self.data);
    buf[len..len + Meta::VERSION_SIZE].copy_from_slice(&self.meta.raw().to_le_bytes());
    buf[len + Meta::VERSION_SIZE..len + Meta::SIZE]
      .copy_from_slice(&self.meta.expire_at().to_le_bytes());
    Ok(len + Meta::SIZE)
  }
}

pub(crate) struct RefKey<'a, C> {
  meta: Meta,
  data: &'a [u8],
  _phantom: PhantomData<C>,
}

impl<'a, C> RefKey<'a, C> {
  #[inline]
  pub fn new(meta: Meta, data: &'a [u8]) -> Self {
    Self {
      meta,
      data,
      _phantom: PhantomData,
    }
  }
}

impl<'a, C> PartialEq for RefKey<'a, C> {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.data.eq(other.data) && self.meta.version() == other.meta.version()
  }
}

impl<'a, C> Eq for RefKey<'a, C> {}

impl<'a, C> PartialOrd for RefKey<'a, C>
where
  C: StaticComparator,
{
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<'a, C> Ord for RefKey<'a, C>
where
  C: StaticComparator,
{
  #[inline]
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    C::compare(&self.data, &other.data).then_with(|| self.meta.version().cmp(&other.meta.version()))
  }
}

impl<'a, C> core::fmt::Debug for RefKey<'a, C> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct(core::any::type_name::<Self>())
      .field("meta", &self.meta)
      .field("key", &&self.data)
      .finish()
  }
}

impl<'a, C> TypeRef<'a> for RefKey<'a, C> {
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

impl<'a, C: StaticComparator> KeyRef<'a, Key<C>> for RefKey<'a, C> {
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

    C::compare(ak, bk).then_with(|| av.cmp(&bv))
  }
}

impl<'a, C> Equivalent<Key<C>> for RefKey<'a, C> {
  fn equivalent(&self, key: &Key<C>) -> bool {
    self.meta.version() == key.meta.version() && self.data.eq(&key.data)
  }
}

impl<'a, C> Comparable<Key<C>> for RefKey<'a, C>
where
  C: StaticComparator,
{
  fn compare(&self, key: &Key<C>) -> std::cmp::Ordering {
    C::compare(self.data, &key.data).then_with(|| self.meta.version().cmp(&key.meta.version()))
  }
}

impl<'a, C> Equivalent<RefKey<'a, C>> for Key<C> {
  fn equivalent(&self, key: &RefKey<'a, C>) -> bool {
    key.meta.version() == self.meta.version() && key.data.eq(&self.data)
  }
}

impl<'a, C> Comparable<RefKey<'a, C>> for Key<C>
where
  C: StaticComparator,
{
  fn compare(&self, key: &RefKey<'a, C>) -> std::cmp::Ordering {
    C::compare(&self.data, key.data).then_with(|| self.meta.version().cmp(&key.meta.version()))
  }
}
