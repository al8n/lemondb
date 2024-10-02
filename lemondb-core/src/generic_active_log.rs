use among::Among;
use dbutils::{checksum::BuildChecksumer, equivalent::Comparable, traits::Type};
use orderwal::{
  error::Error as ActiveLogError,
  swmr::{generic::GenericWalReader, GenericOrderWal},
  Crc32, Generic, KeyBuilder,
};

use core::mem;

use super::types::{
  generic_entry_ref::GenericEntryRef, generic_key::GenericKey, meta::Meta, query::Query,
};

/// The reader of the active log file.
pub struct ActiveLogFileReader<K: ?Sized, V: ?Sized, S = Crc32>(
  GenericWalReader<GenericKey<K>, V, S>,
);

impl<K, V, S> ActiveLogFileReader<K, V, S>
where
  K: ?Sized + Ord + Type + for<'b> Comparable<K::Ref<'b>> + 'static,
  for<'b> K::Ref<'b>: Comparable<K> + Ord,
  V: ?Sized,
{
  /// Returns `true` if the active log contains the key.
  #[inline]
  pub fn contains_key<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Ord + for<'b> Comparable<K::Ref<'b>>,
  {
    let k = Query::<Q, K>::new(Meta::query(version), key);
    self.0.contains_key(&k)
  }
}

impl<K, V, S> ActiveLogFileReader<K, V, S>
where
  K: ?Sized + Ord + Type + for<'b> Comparable<K::Ref<'b>> + 'static,
  for<'b> K::Ref<'b>: Comparable<K> + Ord,
  V: ?Sized + Type,
{
  /// Get the entry by the key and version.
  #[inline]
  pub fn get<'a, Q>(&'a self, version: u64, key: &Q) -> Option<GenericEntryRef<'a, K, V>>
  where
    Q: ?Sized + Ord + for<'b> Comparable<K::Ref<'b>>,
  {
    let k = Query::<'_, Q, K>::new(Meta::query(version), key);
    self.0.get(&k).map(|ent| {
      let (meta, k) = ent.key().into_components();
      let v = ent.value();

      // Safety: the actual lifetime of the key and value is reference to the self.
      unsafe {
        GenericEntryRef::new(
          meta,
          mem::transmute::<K::Ref<'_>, K::Ref<'_>>(k),
          mem::transmute::<V::Ref<'_>, V::Ref<'_>>(v),
        )
      }
    })
  }
}

/// The active log file.
pub struct ActiveLogFile<K: ?Sized, V: ?Sized, S = Crc32> {
  wal: GenericOrderWal<GenericKey<K>, V, S>,
  max_key_size: u32,
  max_value_size: u32,
}

impl<K: ?Sized, V: ?Sized, S> ActiveLogFile<K, V, S> {
  /// Returns a reader of the active log file.
  #[inline]
  pub fn reader(&self) -> ActiveLogFileReader<K, V, S> {
    ActiveLogFileReader(self.wal.reader())
  }
}

impl<K, V, S> ActiveLogFile<K, V, S>
where
  K: ?Sized + Ord + Type + for<'b> Comparable<K::Ref<'b>> + 'static,
  for<'b> K::Ref<'b>: Comparable<K> + Ord,
  V: ?Sized + Type + 'static,
  S: BuildChecksumer,
{
  /// Inserts the key-value pair into the active log file.
  pub fn insert(
    &mut self,
    meta: Meta,
    key: Generic<'_, K>,
    value: Generic<'_, V>,
  ) -> Result<(), Among<K::Error, V::Error, ActiveLogError>> {
    let klen = mem::size_of::<Meta>() + key.encoded_len();
    if klen > self.max_key_size as usize {
      return Err(Among::Right(ActiveLogError::KeyTooLarge {
        size: klen as u64,
        maximum_key_size: self.max_key_size,
      }));
    }

    let vlen = value.encoded_len();
    if vlen > self.max_value_size as usize {
      return Err(Among::Right(ActiveLogError::ValueTooLarge {
        size: vlen as u64,
        maximum_value_size: self.max_value_size,
      }));
    }

    let kb = KeyBuilder::once(klen as u32, |buf| {
      buf.set_len(klen);
      key.encode(buf)?;
      buf.put_u64_le_unchecked(meta.raw());
      buf.put_u64_le_unchecked(meta.expire_at());

      Ok(())
    });

    unsafe { self.wal.insert_with_key_builder::<K::Error>(kb, value) }
  }
}
