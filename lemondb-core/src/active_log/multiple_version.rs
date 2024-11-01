use core::{mem, ops::Bound};
use std::sync::Arc;

use among::Among;
use dbutils::{
  checksum::BuildChecksumer,
  equivalent::Comparable,
  types::{KeyRef, MaybeStructured, Type},
};
use either::Either;
use orderwal::{
  error::Error,
  multiple_version::{AlternativeTable, OrderWal, OrderWalReader, Reader, Writer},
  types::{KeyBuilder, ValueBuilder},
  Crc32,
};
use ref_cast::RefCast;

use crate::types::{
  active_meta::ActiveMeta,
  key::{EncodeError, Key},
  pointer::Pointer,
  query::Query,
  value::{PhantomValue, Value},
};

/// The error for the active log file.
pub type ActiveLogError<K, V> = Error<AlternativeTable<Key<K>, PhantomValue<V>>>;

/// The reader of the active log file.
pub struct ActiveLogFileReader<K: ?Sized, V: ?Sized, S = Crc32>(
  Arc<OrderWalReader<Key<K>, PhantomValue<V>, AlternativeTable<Key<K>, PhantomValue<V>>, S>>,
);

impl<K, V, S> ActiveLogFileReader<K, V, S>
where
  K: ?Sized + Type + Ord + for<'b> Comparable<K::Ref<'b>> + 'static,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized + Type + 'static,
  S: 'static,
{
  /// Returns the maximum version in the active log.
  #[inline]
  pub fn max_version(&self) -> u64 {
    self.0.maximum_version()
  }

  /// Returns the minimum version in the active log.
  #[inline]
  pub fn min_version(&self) -> u64 {
    self.0.minimum_version()
  }

  /// Returns `true` if the active log may contain an entry less or equal to the version.
  #[inline]
  pub fn may_contain_version(&self, version: u64) -> bool {
    self.0.may_contain_version(version)
  }
}

impl<K, V, S> ActiveLogFileReader<K, V, S>
where
  K: ?Sized + Type + Ord + for<'b> Comparable<K::Ref<'b>> + 'static,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized + Type + 'static,
  S: 'static,
{
  /// Returns `true` if the active log contains the key.
  #[inline]
  pub fn contains_key<'a, Q>(&'a self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  {
    self.0.contains_key(version, Query::ref_cast(key))
  }

  // /// Get the entry by the key and version.
  // #[inline]
  // pub fn get<'a, 'b: 'a, Q>(&'a self, version: u64, key: &'b Q) -> Option<GenericEntryRef<'a, K, V>>
  // where
  //   Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  // {
  //   self.0.get(version, key)
  // }

  // /// Returns the first entry in the active log.
  // #[inline]
  // pub fn first(&self, version: u64) -> Option<GenericEntryRef<'_, K, V>> {
  //   self.0.first(version)
  // }

  // /// Returns the last entry in the active log.
  // #[inline]
  // pub fn last(&self, version: u64) -> Option<GenericEntryRef<'_, K, V>> {
  //   self.0.last(version)
  // }

  // /// Returns a value associated to the highest element whose key is below the given bound. If no such element is found then `None` is returned.
  // #[inline]
  // pub fn upper_bound<'a, 'b: 'a, Q>(
  //   &'a self,
  //   version: u64,
  //   bound: Bound<&'b Q>,
  // ) -> Option<GenericEntryRef<'a, K, V>>
  // where
  //   Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  // {
  //   self.0.upper_bound(version, bound)
  // }

  // /// Returns a value associated to the lowest element whose key is above the given bound. If no such element is found then `None` is returned.
  // #[inline]
  // pub fn lower_bound<'a, 'b: 'a, Q>(
  //   &'a self,
  //   version: u64,
  //   bound: Bound<&'b Q>,
  // ) -> Option<GenericEntryRef<'a, K, V>>
  // where
  //   Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  // {
  //   self.0.lower_bound(version, bound)
  // }
}

/// Active log file writer
pub struct ActiveLogWriter<K, V, S = Crc32>
where
  K: ?Sized,
  V: ?Sized,
{
  inner: Arc<OrderWalReader<Key<K>, PhantomValue<V>, AlternativeTable<Key<K>, PhantomValue<V>>, S>>,
  writer: OrderWal<Key<K>, PhantomValue<V>, AlternativeTable<Key<K>, PhantomValue<V>>, S>,
}

impl<K, V, S> ActiveLogWriter<K, V, S>
where
  K: ?Sized + Type + Ord + for<'b> Comparable<K::Ref<'b>> + 'static,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized + Type + 'static,
  S: BuildChecksumer + 'static,
{
  /// Inserts the key-value pair into the active log file.
  #[inline]
  pub fn insert(
    &mut self,
    version: u64,
    meta: ActiveMeta,
    key: MaybeStructured<'_, K>,
    value: Either<MaybeStructured<'_, V>, Pointer>,
  ) -> Result<(), Among<EncodeError<K>, V::Error, ActiveLogError<K, V>>> {
    self.update(version, meta, key, Some(value))
  }

  /// Removes the key from the active log file, fake delete operation (append with a tombstone entry).
  #[inline]
  pub fn remove(
    &mut self,
    version: u64,
    meta: ActiveMeta,
    key: MaybeStructured<'_, K>,
  ) -> Result<(), Either<EncodeError<K>, ActiveLogError<K, V>>> {
    self
      .update(version, meta, key, None)
      .map_err(Among::into_left_right)
  }

  fn update(
    &mut self,
    version: u64,
    meta: ActiveMeta,
    key: MaybeStructured<'_, K>,
    value: Option<Either<MaybeStructured<'_, V>, Pointer>>,
  ) -> Result<(), Among<EncodeError<K>, V::Error, ActiveLogError<K, V>>> {
    let klen = ActiveMeta::SIZE + key.encoded_len();

    let kb = KeyBuilder::once(klen, |buf| {
      buf.put_slice_unchecked(meta.encode().as_ref());
      key.encode(buf).map_err(EncodeError::Key).map(|written| {
        debug_assert_eq!(
          written,
          klen - ActiveMeta::SIZE,
          "expected write {} bytes but actual write {} bytes",
          klen - ActiveMeta::SIZE,
          written
        );
        klen
      })
    });

    match value {
      None => self
        .writer
        .remove_with_builder(version, kb)
        .map_err(Among::from_either_to_left_right),
      Some(value) => {
        let value = Value::new(value);
        let vlen = value.encoded_len();

        let vb = ValueBuilder::once(vlen, |buf| {
          buf.set_len(vlen);
          value.encode(buf)
        });

        self.writer.insert_with_builders(version, kb, vb)
      }
    }
  }
}
