use among::Among;
use dbutils::{checksum::BuildChecksumer, equivalent::Comparable, traits::Type};
use orderwal::{
  error::Error as ActiveLogError,
  swmr::{generic::GenericOrderWalReader, GenericOrderWal},
  Crc32, Generic, KeyBuilder,
};

use core::{
  mem,
  ops::Bound,
  sync::atomic::{AtomicU64, Ordering},
};
use std::sync::Arc;

use super::types::{
  generic_entry_ref::GenericEntryRef, generic_key::GenericKey, meta::Meta, query::Query,
};

/// The reader of the active log file.
pub struct ActiveLogFileReader<K: ?Sized, V: ?Sized, S = Crc32>(Arc<Inner<K, V, S>>);

impl<K: ?Sized, V: ?Sized, S> ActiveLogFileReader<K, V, S> {
  /// Returns the maximum version in the active log.
  #[inline]
  pub fn max_version(&self) -> u64 {
    self.0.max_version.load(Ordering::Acquire)
  }

  /// Returns the minimum version in the active log.
  #[inline]
  pub fn min_version(&self) -> u64 {
    self.0.min_version.load(Ordering::Acquire)
  }

  /// Returns `true` if the active log contains the version.
  #[inline]
  pub fn contains_version(&self, version: u64) -> bool {
    let min = self.min_version();
    let max = self.max_version();

    min <= version && version <= max
  }
}

impl<K, V, S> ActiveLogFileReader<K, V, S>
where
  K: ?Sized + Ord + Type + for<'b> Comparable<K::Ref<'b>> + 'static,
  for<'b> K::Ref<'b>: Comparable<K> + Ord,
  V: ?Sized + Type,
{
  /// Returns `true` if the active log contains the key.
  #[inline]
  pub fn contains_key<'a, 'b: 'a, Q>(&'a self, version: u64, key: &'b Q) -> bool
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  {
    if !self.contains_version(version) {
      return false;
    }

    let mut ent = self.0.lower_bound(
      Bound::Included(Query::<'_, Q, K>::new(Meta::query(Meta::MAX_VERSION), key)).as_ref(),
    );

    while let Some(e) = ent {
      if e.key().version() <= version {
        return true;
      }

      ent = e.next();
    }

    false
  }

  /// Get the entry by the key and version.
  #[inline]
  pub fn get<'a, 'b: 'a, Q>(&'a self, version: u64, key: &'b Q) -> Option<GenericEntryRef<'a, K, V>>
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  {
    self
      .contains_version(version)
      .then(|| {
        let mut ent = self.0.lower_bound(
          Bound::Included(Query::<'_, Q, K>::new(Meta::query(Meta::MAX_VERSION), key)).as_ref(),
        );

        while let Some(e) = ent {
          if e.key().version() <= version {
            return Some(GenericEntryRef::new(e));
          }

          ent = e.next();
        }

        None
      })
      .flatten()
  }

  /// Returns the first entry in the active log.
  #[inline]
  pub fn first(&self, version: u64) -> Option<GenericEntryRef<'_, K, V>> {
    self
      .contains_version(version)
      .then(|| {
        let mut first = self.0.first();

        while let Some(ent) = first {
          if ent.key().version() <= version {
            return Some(GenericEntryRef::new(ent));
          }

          first = ent.next();
        }

        None
      })
      .flatten()
  }

  /// Returns the last entry in the active log.
  #[inline]
  pub fn last(&self, version: u64) -> Option<GenericEntryRef<'_, K, V>> {
    self
      .contains_version(version)
      .then(|| {
        let mut last = self.0.last();

        while let Some(ent) = last {
          if ent.key().version() <= version {
            return Some(GenericEntryRef::new(ent));
          }

          last = ent.prev();
        }

        None
      })
      .flatten()
  }

  /// Returns a value associated to the highest element whose key is below the given bound. If no such element is found then `None` is returned.
  #[inline]
  pub fn upper_bound<'a, 'b: 'a, Q>(
    &'a self,
    version: u64,
    bound: Bound<&'b Q>,
  ) -> Option<GenericEntryRef<'a, K, V>>
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  {
    self
      .contains_version(version)
      .then(|| {
        let mut upper_bound = self.0.upper_bound(
          bound
            .map(|b| Query::<'_, Q, K>::new(Meta::query(Meta::MAX_VERSION), b))
            .as_ref(),
        );

        while let Some(ent) = upper_bound {
          if ent.key().version() <= version {
            return Some(GenericEntryRef::new(ent));
          }

          upper_bound = ent.next();
        }

        None
      })
      .flatten()
  }

  /// Returns a value associated to the lowest element whose key is above the given bound. If no such element is found then `None` is returned.
  #[inline]
  pub fn lower_bound<'a, 'b: 'a, Q>(
    &'a self,
    version: u64,
    bound: Bound<&'b Q>,
  ) -> Option<GenericEntryRef<'a, K, V>>
  where
    Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  {
    self
      .contains_version(version)
      .then(|| {
        let mut lower_bound = self.0.lower_bound(
          bound
            .map(|b| Query::<'_, Q, K>::new(Meta::query(Meta::MAX_VERSION), b))
            .as_ref(),
        );

        while let Some(ent) = lower_bound {
          if ent.key().version() <= version {
            return Some(GenericEntryRef::new(ent));
          }

          lower_bound = ent.next();
        }

        None
      })
      .flatten()
  }
}

struct Inner<K: ?Sized, V: ?Sized, S = Crc32> {
  reader: GenericOrderWalReader<GenericKey<K>, V, S>,
  max_version: AtomicU64,
  min_version: AtomicU64,
}

impl<K: ?Sized, V: ?Sized, S> core::ops::Deref for Inner<K, V, S> {
  type Target = GenericOrderWalReader<GenericKey<K>, V, S>;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.reader
  }
}

/// The active log file.
pub struct ActiveLogFile<K: ?Sized, V: ?Sized, S = Crc32> {
  inner: Arc<Inner<K, V, S>>,
  writer: GenericOrderWal<GenericKey<K>, V, S>,
  max_key_size: u32,
  max_value_size: u32,
}

impl<K: ?Sized, V: ?Sized, S> ActiveLogFile<K, V, S> {
  /// Returns a reader of the active log file.
  #[inline]
  pub fn reader(&self) -> ActiveLogFileReader<K, V, S> {
    ActiveLogFileReader(self.inner.clone())
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

    unsafe { self.writer.insert_with_key_builder::<K::Error>(kb, value) }
  }
}
