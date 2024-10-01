use dbutils::{
  checksum::BuildChecksumer,
  equivalent::{Comparable, Equivalent},
  traits::{KeyRef, Type, TypeRef},
  Ascend, CheapClone, StaticComparator,
};

use orderwal::{
  error::Error as ActiveLogError,
  swmr::{wal::OrderWalReader, GenericOrderWal, OrderWal},
  Crc32, ImmutableWal, KeyBuilder, Wal,
};
use skl::KeySize;
use zerocopy::FromBytes;

use core::mem;
use std::{
  borrow::Borrow,
  cmp,
  marker::PhantomData,
  ops::{Bound, RangeBounds},
};

use crate::Meta;

const META_SIZE: usize = mem::size_of::<Meta>();
const VERSION_SIZE: usize = mem::size_of::<u64>();

struct Key<C> {
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
    C::compare(&self.data, &other.data)
      .then_with(|| self.meta.version().cmp(&other.meta.version()))
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
    todo!()
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    todo!()
  }
}

struct RefKey<'a, C> {
  version: u64,
  data: &'a [u8],
  _phantom: PhantomData<C>,
}

impl<'a, C> PartialEq for RefKey<'a, C> {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.data.eq(other.data) && self.version == other.version
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
    C::compare(&self.data, &other.data)
      .then_with(|| self.version.cmp(&other.version))
  }
}

impl<'a, C> core::fmt::Debug for RefKey<'a, C> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct(core::any::type_name::<Self>())
      .field("version", &self.version)
      .field("key", &&self.data)
      .finish()
  }
}

impl<'a, C> TypeRef<'a> for RefKey<'a, C> {
  #[inline]
  unsafe fn from_slice(buf: &'a [u8]) -> Self {
    let len = buf.len();
    let version = Meta::decode_version(&buf[len - META_SIZE..]);
    let data = &buf[..len - META_SIZE];

    Self {
      version,
      data,
      _phantom: PhantomData,
    }
  }
}

impl<'a, C: StaticComparator> KeyRef<'a, Key<C>> for RefKey<'a, C> {
  #[inline]
  fn compare<Q>(&self, a: &Q) -> cmp::Ordering
  where
    Q: ?Sized + Ord + dtlog::Comparable<Self>,
  {
    todo!()
  }

  #[inline]
  unsafe fn compare_binary(a: &[u8], b: &[u8]) -> cmp::Ordering {
    todo!()
  }
}

// impl<'a, C> Borrow<RefKey<'a, C>> for Key<C> {
//   #[inline]
//   fn borrow(&self) -> &RefKey<'a, C> {
//     RefKey {
//       version: self.meta.version(),
//       data: &self.data,
//       _phantom: PhantomData,
//     }
//   }
// }

impl<'a, C> Equivalent<Key<C>> for RefKey<'a, C> {
  fn equivalent(&self, key: &Key<C>) -> bool {
    self.version == key.meta.version() && self.data.eq(&key.data)
  }
}

impl<'a, C> Comparable<Key<C>> for RefKey<'a, C>
where
  C: StaticComparator,
{
  fn compare(&self, key: &Key<C>) -> std::cmp::Ordering {
    C::compare(self.data, &key.data)
      .then_with(|| self.version.cmp(&key.meta.version()))
  }
}

impl<'a, C> Equivalent<RefKey<'a, C>> for Key<C> {
  fn equivalent(&self, key: &RefKey<'a, C>) -> bool {
    key.version == self.meta.version() && key.data.eq(&self.data)
  }
}

impl<'a, C> Comparable<RefKey<'a, C>> for Key<C>
where
  C: StaticComparator,
{
  fn compare(&self, key: &RefKey<'a, C>) -> std::cmp::Ordering {
    C::compare(&self.data, key.data)
      .then_with(|| self.meta.version().cmp(&key.version))
  }
}

#[derive(Debug)]
pub(crate) struct ComparatorWrapper<C>(C);

impl<C: StaticComparator> StaticComparator for ComparatorWrapper<C> {
  #[inline]
  fn compare(a: &[u8], b: &[u8]) -> cmp::Ordering {
    let alen = a.len();
    let blen = b.len();

    let ak = &a[..alen - META_SIZE];
    let av = Meta::decode_version(&a[alen - META_SIZE..]);
    let bk = &b[..blen - META_SIZE];
    let bv = Meta::decode_version(&b[blen - META_SIZE..]);

    C::compare(ak, bk).then_with(|| av.cmp(&bv))
  }

  #[inline]
  fn contains(start_bound: Bound<&[u8]>, end_bound: Bound<&[u8]>, key: &[u8]) -> bool {
    let (start_bound, start_bound_version) = match start_bound {
      Bound::Included(b) => {
        let len = b.len();
        let k = &b[..len - META_SIZE];
        let meta_buf = &b[len - META_SIZE..];
        (
          Bound::Included(k),
          Bound::Included(Meta::decode_version(meta_buf)),
        )
      }
      Bound::Excluded(b) => {
        let len = b.len();
        let k = &b[..len - META_SIZE];
        let meta_buf = &b[len - META_SIZE..];
        (
          Bound::Included(k),
          Bound::Excluded(Meta::decode_version(meta_buf)),
        )
      }
      Bound::Unbounded => (Bound::Unbounded, Bound::Unbounded),
    };

    let (end_bound, end_bound_version) = match end_bound {
      Bound::Included(b) => {
        let len = b.len();
        let k = &b[..len - META_SIZE];
        let meta_buf = &b[len - META_SIZE..];
        (
          Bound::Included(k),
          Bound::Included(Meta::decode_version(meta_buf)),
        )
      }
      Bound::Excluded(b) => {
        let len = b.len();
        let k = &b[..len - META_SIZE];
        let meta_buf = &b[len - META_SIZE..];
        (
          Bound::Included(k),
          Bound::Excluded(Meta::decode_version(meta_buf)),
        )
      }
      Bound::Unbounded => (Bound::Unbounded, Bound::Unbounded),
    };

    let len = key.len();
    let k = &key[..len - META_SIZE];
    let meta_buf = &key[len - META_SIZE..];
    let key_version = Meta::decode_version(meta_buf);

    C::contains(start_bound, end_bound, k)
      && (start_bound_version, end_bound_version).contains(&key_version)
  }
}

// pub struct ActiveLogFileReader<C = Ascend, S = Crc32>(OrderWalReader<C, S>);

// impl<C, S> ActiveLogFileReader<C, S>
// where
//   C: Comparator + CheapClone + Send + 'static,
// {
//   #[inline]
//   fn get(&self, version: u64, key: &[u8]) -> Option<&[u8]> {
//     // self.0.get()
//     todo!()
//   }
// }

pub struct ActiveLogFile<C = Ascend, S = Crc32> {
  wal: GenericOrderWal<Key<C>, [u8], S>,
  max_key_size: u32,
  max_value_size: u32,
}

impl<C, S> ActiveLogFile<C, S>
where
  C: StaticComparator + CheapClone + Send + 'static,
{
  // #[inline]
  // pub fn reader(&self) -> ActiveLogFileReader<C, S> {
  //   ActiveLogFileReader(self.wal.reader())
  // }
}

impl<C, S> ActiveLogFile<C, S>
where
  C: StaticComparator + CheapClone + Send + 'static,
  S: BuildChecksumer,
{
  pub fn insert(&mut self, meta: Meta, key: &[u8], value: &[u8]) -> Result<(), ActiveLogError> {
    let klen = mem::size_of::<Meta>() + key.len();
    if klen > self.max_key_size as usize {
      return Err(ActiveLogError::KeyTooLarge {
        size: klen as u64,
        maximum_key_size: self.max_key_size,
      });
    }

    let vlen = value.len();
    if vlen > self.max_value_size as usize {
      return Err(ActiveLogError::ValueTooLarge {
        size: vlen as u64,
        maximum_value_size: self.max_value_size,
      });
    }

    let kb = KeyBuilder::once(klen as u32, |buf| {
      buf.put_slice_unchecked(key);
      buf.put_u64_le_unchecked(meta.raw());
      buf.put_u64_le_unchecked(meta.expire_at());

      Ok(())
    });

    unsafe {
      self
        .wal
        .insert_with_key_builder::<()>(kb, value)
        .map_err(|e| e.unwrap_right())
    }
  }
}
