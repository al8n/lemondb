use dbutils::{
  checksum::BuildChecksumer,
  equivalent::{Comparable, Equivalent},
  traits::{KeyRef, Type, TypeRef},
  Ascend, CheapClone, StaticComparator,
};

use orderwal::{
  error::Error as ActiveLogError,
  swmr::{generic::GenericWalReader, wal::OrderWalReader, GenericOrderWal, OrderWal},
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

use crate::{
  key::{Key, RefKey},
  EntryRef, Meta,
};

#[derive(Debug)]
pub(crate) struct ComparatorWrapper<C>(C);

impl<C: StaticComparator> StaticComparator for ComparatorWrapper<C> {
  #[inline]
  fn compare(a: &[u8], b: &[u8]) -> cmp::Ordering {
    let alen = a.len();
    let blen = b.len();

    let ak = &a[..alen - Meta::SIZE];
    let av = Meta::decode_version(&a[alen - Meta::SIZE..]);
    let bk = &b[..blen - Meta::SIZE];
    let bv = Meta::decode_version(&b[blen - Meta::SIZE..]);

    C::compare(ak, bk).then_with(|| av.cmp(&bv))
  }

  #[inline]
  fn contains(start_bound: Bound<&[u8]>, end_bound: Bound<&[u8]>, key: &[u8]) -> bool {
    let (start_bound, start_bound_version) = match start_bound {
      Bound::Included(b) => {
        let len = b.len();
        let k = &b[..len - Meta::SIZE];
        let meta_buf = &b[len - Meta::SIZE..];
        (
          Bound::Included(k),
          Bound::Included(Meta::decode_version(meta_buf)),
        )
      }
      Bound::Excluded(b) => {
        let len = b.len();
        let k = &b[..len - Meta::SIZE];
        let meta_buf = &b[len - Meta::SIZE..];
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
        let k = &b[..len - Meta::SIZE];
        let meta_buf = &b[len - Meta::SIZE..];
        (
          Bound::Included(k),
          Bound::Included(Meta::decode_version(meta_buf)),
        )
      }
      Bound::Excluded(b) => {
        let len = b.len();
        let k = &b[..len - Meta::SIZE];
        let meta_buf = &b[len - Meta::SIZE..];
        (
          Bound::Included(k),
          Bound::Excluded(Meta::decode_version(meta_buf)),
        )
      }
      Bound::Unbounded => (Bound::Unbounded, Bound::Unbounded),
    };

    let len = key.len();
    let k = &key[..len - Meta::SIZE];
    let meta_buf = &key[len - Meta::SIZE..];
    let key_version = Meta::decode_version(meta_buf);

    C::contains(start_bound, end_bound, k)
      && (start_bound_version, end_bound_version).contains(&key_version)
  }
}

pub struct ActiveLogFileReader<C = Ascend, S = Crc32>(GenericWalReader<Key<C>, [u8], S>);

impl<C, S> ActiveLogFileReader<C, S>
where
  C: StaticComparator,
{
  #[inline]
  pub fn contains_key(&self, version: u64, key: &[u8]) -> bool {
    self.0.contains_key(&RefKey::new(Meta::query(version), key))
  }

  #[inline]
  pub fn get<'a>(&'a self, version: u64, key: &'a [u8]) -> Option<EntryRef<'a, C>> {
    let k = RefKey::new(Meta::query(version), key);
    self.0.get(&k).map(|ent| {
      let k = ent.key();
      let v = ent.value();

      // Safety: the actual lifetime of the key and value is reference to the self.
      unsafe {
        EntryRef::new(
          mem::transmute::<RefKey<'_, C>, RefKey<'a, C>>(k),
          mem::transmute::<&[u8], &'a [u8]>(v.as_ref()),
        )
      }
    })
  }
}

pub struct ActiveLogFile<C = Ascend, S = Crc32> {
  wal: GenericOrderWal<Key<C>, [u8], S>,
  max_key_size: u32,
  max_value_size: u32,
}

impl<C, S> ActiveLogFile<C, S>
where
  C: StaticComparator + CheapClone + Send + 'static,
{
  #[inline]
  pub fn reader(&self) -> ActiveLogFileReader<C, S> {
    ActiveLogFileReader(self.wal.reader())
  }
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
