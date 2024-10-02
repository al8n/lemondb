use dbutils::{checksum::BuildChecksumer, Ascend, CheapClone, StaticComparator};

use orderwal::{
  error::Error as ActiveLogError,
  swmr::{generic::GenericWalReader, GenericOrderWal},
  Crc32, KeyBuilder,
};

use core::{
  cmp, mem,
  ops::{Bound, RangeBounds},
};

use super::types::{entry_ref::EntryRef, key::Key, key_ref::KeyRef, meta::Meta};

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

/// The reader of the active log file.
pub struct ActiveLogFileReader<C = Ascend, S = Crc32>(GenericWalReader<Key<C>, [u8], S>);

impl<C, S> ActiveLogFileReader<C, S>
where
  C: StaticComparator,
{
  /// Returns `true` if the active log contains the key.
  #[inline]
  pub fn contains_key(&self, version: u64, key: &[u8]) -> bool {
    self.0.contains_key(&KeyRef::new(Meta::query(version), key))
  }

  /// Get the entry by the key and version.
  #[inline]
  pub fn get<'a>(&'a self, version: u64, key: &'a [u8]) -> Option<EntryRef<'a, C>> {
    let k = KeyRef::new(Meta::query(version), key);
    self.0.get(&k).map(|ent| {
      let k = ent.key();
      let v = ent.value();

      // Safety: the actual lifetime of the key and value is reference to the self.
      unsafe {
        EntryRef::new(
          mem::transmute::<KeyRef<'_, C>, KeyRef<'a, C>>(k),
          mem::transmute::<&[u8], &'a [u8]>(v.as_ref()),
        )
      }
    })
  }
}

/// The active log file.
pub struct ActiveLogFile<C = Ascend, S = Crc32> {
  wal: GenericOrderWal<Key<C>, [u8], S>,
  max_key_size: u32,
  max_value_size: u32,
}

impl<C, S> ActiveLogFile<C, S>
where
  C: StaticComparator + CheapClone + Send + 'static,
{
  /// Returns a reader of the active log file.
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
  /// Inserts the key-value pair into the active log file.
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
