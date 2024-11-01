use crate::types::pointer::Pointer;

use super::{Meta, VMeta, ValueLogCore};

use dbutils::{
  buffer::VacantBuffer,
  checksum::{BuildChecksumer, Crc32},
  types::Type,
};
use valog::{error::Error, LogReaderExt, LogWriterExt, ValueBuilder};

mod entry;
use entry::{Entry, EntryRef, PhantomEntry};

/// A generic value log that is lock-free, concurrent safe, and can be used in multi-threaded environments.
pub struct ValueLog<C = Crc32> {
  log: ValueLogCore<PhantomEntry, C>,
}

impl<C> ValueLog<C>
where
  C: BuildChecksumer,
{
  /// Reads a entry from the value log at the given offset with size.
  pub fn read(&self, pointer: Pointer) -> Result<EntryRef<'_>, Error> {
    unsafe {
      self
        .log
        .log
        .read_generic::<Entry<'_>>(&pointer.id(), pointer.offset(), pointer.size())
    }
  }

  /// Inserts a key-value pair into the value log.
  pub fn insert(&self, meta: Meta, key: &[u8], value: &[u8]) -> Result<Pointer, Error> {
    let ent = Entry::new(meta.into(), key, Some(value));
    let encoded_len = ent.encoded_len();
    self
      .log
      .log
      .insert_with(ValueBuilder::new(
        encoded_len as u32,
        |buf: &mut VacantBuffer<'_>| {
          buf.set_len(encoded_len);
          ent.encode(buf).map(|_| ())
        },
      ))
      .map(Pointer::new)
      .map_err(|e| e.unwrap_right())
  }

  /// Removes a key from the value log.
  ///
  /// **Note:** This is a fake delete operation, the key-value pair is not actually removed from the value log, just appended with a tombstone entry.
  pub fn remove(&self, meta: Meta, key: &[u8]) -> Result<Pointer, Error> {
    let ent = Entry::new(VMeta::from(meta).with_tombstone(), key, None);
    let encoded_len = ent.encoded_len();
    self
      .log
      .log
      .insert_tombstone_with(ValueBuilder::new(
        encoded_len as u32,
        |buf: &mut VacantBuffer<'_>| {
          buf.set_len(encoded_len);
          ent.encode(buf).map(|_| ())
        },
      ))
      .map(Pointer::new)
      .map_err(|e| e.unwrap_right())
  }
}
