use core::marker::PhantomData;

use among::Among;
use dbutils::{checksum::Crc32, traits::Type};
use generic_entry::{GenericEntry, PhantomGenericEntry};
use valog::{checksum::BuildChecksumer, error::Error, sync::ValueLog as VaLog, LogWriterExt, VacantBuffer, ValueBuilder};

use crate::types::pointer::Pointer;

use super::types::meta::Meta;
use meta::Meta as VMeta;

mod generic_entry;

mod entry;

/// The meta type for an entry in the value log.
mod meta;

/// The value log
pub struct ValueLog<E, C = Crc32> {
  log: VaLog<u64, C>,
  _phantom: PhantomData<E>,
}

impl<E, C> core::ops::Deref for ValueLog<E, C> {
  type Target = VaLog<u64, C>;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.log
  }
}

/// A generic value log that is lock-free, concurrent safe, and can be used in multi-threaded environments.
pub struct GenericValueLog<K: ?Sized, V: ?Sized, C = Crc32> {
  log: ValueLog<PhantomGenericEntry<K, V>, C>
}

impl<K, V, C> GenericValueLog<K, V, C>
where
  K: core::fmt::Debug + Type + ?Sized,
  V: core::fmt::Debug + Type + ?Sized,
  C: BuildChecksumer,
{
  /// Inserts a key-value pair into the value log.
  pub fn insert(&self, meta: Meta, key: &K, value: &V) -> Result<Pointer, Among<K::Error, V::Error, Error>> {
    let ent = GenericEntry::new(meta.into(), key, Some(value));
    let encoded_len = ent.encoded_len();
    self.log.log.insert_with(ValueBuilder::new(encoded_len as u32, |buf: &mut VacantBuffer<'_>| {
      buf.set_len(encoded_len);
      ent.encode(buf).map(|_| ())
    }))
    .map(Pointer::new)
    .map_err(Into::into)
  }

  /// Removes a key from the value log.
  /// 
  /// **Note:** This is a fake delete operation, the key-value pair is not actually removed from the value log, just appended with a tombstone entry.
  pub fn remove(&self, meta: Meta, key: &K) -> Result<Pointer, Among<K::Error, V::Error, Error>> {
    let ent = GenericEntry::<'_, K, V>::new(VMeta::from(meta).with_tombstone(), key, None);
    let encoded_len = ent.encoded_len();
    self.log.log.insert_with(ValueBuilder::new(encoded_len as u32, |buf: &mut VacantBuffer<'_>| {
      buf.set_len(encoded_len);
      ent.encode(buf).map(|_| ())
    }))
    .map(Pointer::new)
    .map_err(Into::into)
  }
}

/// Merge two `u32` into a `u64`.
///
/// - high 32 bits: `a`
/// - low 32 bits: `b`
#[inline]
const fn merge_lengths(a: u32, b: u32) -> u64 {
  (a as u64) << 32 | b as u64
}

/// Split a `u64` into two `u32`.
///
/// - high 32 bits: the first `u32`
/// - low 32 bits: the second `u32`
#[inline]
const fn split_lengths(len: u64) -> (u32, u32) {
  ((len >> 32) as u32, len as u32)
}
