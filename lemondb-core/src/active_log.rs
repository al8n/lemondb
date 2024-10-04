use dbutils::{checksum::BuildChecksumer, Ascend, CheapClone, StaticComparator};

use orderwal::{
  error::Error as ActiveLogError,
  swmr::{generic::GenericOrderWalReader, GenericOrderWal},
  Crc32, KeyBuilder,
};

use core::{
  mem,
  ops::Bound,
  sync::atomic::{AtomicU64, Ordering},
};
use std::sync::Arc;

use super::types::{entry_ref::EntryRef, key::Key, meta::Meta, query::Query};

/// The reader of the active log file.
pub struct ActiveLogFileReader<C = Ascend, S = Crc32>(Arc<Inner<C, S>>);

impl<C, S> ActiveLogFileReader<C, S> {
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

impl<C, S> ActiveLogFileReader<C, S>
where
  C: StaticComparator,
{
  /// Returns `true` if the active log contains the key.
  #[inline]
  pub fn contains_key(&self, version: u64, key: &[u8]) -> bool {
    if !self.contains_version(version) {
      return false;
    }

    let mut ent = self
      .0
      .lower_bound(Bound::Included(Query::new(Meta::query(Meta::MAX_VERSION), key)).as_ref());

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
  pub fn get<'a>(&'a self, version: u64, key: &[u8]) -> Option<EntryRef<'a, C>> {
    self
      .contains_version(version)
      .then(|| {
        let mut ent = self
          .0
          .lower_bound(Bound::Included(Query::new(Meta::query(Meta::MAX_VERSION), key)).as_ref());

        while let Some(e) = ent {
          if e.key().version() <= version {
            return Some(EntryRef::new(e));
          }

          ent = e.next();
        }

        None
      })
      .flatten()
  }

  /// Returns the first entry in the active log.
  #[inline]
  pub fn first(&self, version: u64) -> Option<EntryRef<'_, C>> {
    self
      .contains_version(version)
      .then(|| {
        let mut first = self.0.first();

        while let Some(ent) = first {
          if ent.key().version() <= version {
            return Some(EntryRef::new(ent));
          }

          first = ent.next();
        }

        None
      })
      .flatten()
  }

  /// Returns the last entry in the active log.
  #[inline]
  pub fn last(&self, version: u64) -> Option<EntryRef<'_, C>> {
    self
      .contains_version(version)
      .then(|| {
        let mut last = self.0.last();

        while let Some(ent) = last {
          if ent.key().version() <= version {
            return Some(EntryRef::new(ent));
          }

          last = ent.prev();
        }

        None
      })
      .flatten()
  }

  /// Returns a value associated to the highest element whose key is below the given bound. If no such element is found then `None` is returned.
  #[inline]
  pub fn upper_bound(&self, version: u64, bound: Bound<&[u8]>) -> Option<EntryRef<'_, C>> {
    self
      .contains_version(version)
      .then(|| {
        let mut upper_bound = self.0.upper_bound(
          bound
            .map(|b| Query::new(Meta::query(Meta::MAX_VERSION), b))
            .as_ref(),
        );

        while let Some(ent) = upper_bound {
          if ent.key().version() <= version {
            return Some(EntryRef::new(ent));
          }

          upper_bound = ent.next();
        }

        None
      })
      .flatten()
  }

  /// Returns a value associated to the lowest element whose key is above the given bound. If no such element is found then `None` is returned.
  #[inline]
  pub fn lower_bound(&self, version: u64, bound: Bound<&[u8]>) -> Option<EntryRef<'_, C>> {
    self
      .contains_version(version)
      .then(|| {
        let mut lower_bound = self.0.lower_bound(
          bound
            .map(|b| Query::new(Meta::query(Meta::MAX_VERSION), b))
            .as_ref(),
        );

        while let Some(ent) = lower_bound {
          if ent.key().version() <= version {
            return Some(EntryRef::new(ent));
          }

          lower_bound = ent.next();
        }

        None
      })
      .flatten()
  }
}

struct Inner<C, S> {
  reader: GenericOrderWalReader<Key<C>, [u8], S>,

  max_version: AtomicU64,
  min_version: AtomicU64,
}

impl<C, S> core::ops::Deref for Inner<C, S> {
  type Target = GenericOrderWalReader<Key<C>, [u8], S>;

  #[inline]
  fn deref(&self) -> &GenericOrderWalReader<Key<C>, [u8], S> {
    &self.reader
  }
}

/// The active log file.
pub struct ActiveLogFile<C = Ascend, S = Crc32> {
  inner: Arc<Inner<C, S>>,
  writer: GenericOrderWal<Key<C>, [u8], S>,
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
    ActiveLogFileReader(self.inner.clone())
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
      #[cfg(feature = "ttl")]
      buf.put_u64_le_unchecked(meta.expire_at());

      Ok(())
    });

    unsafe {
      self
        .writer
        .insert_with_key_builder::<()>(kb, value)
        .map_err(|e| e.unwrap_right())
    }
  }
}
