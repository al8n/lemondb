use super::{
  error::ValueLogError,
  options::{CreateOptions, OpenOptions},
  *,
};

use core::cell::UnsafeCell;

mod memory;
use memory::*;

#[cfg(feature = "std")]
mod mmap;
#[cfg(feature = "std")]
use mmap::*;

#[derive(derive_more::From)]
enum ValueLogKind {
  Memory(MemoryValueLog),
  #[cfg(feature = "std")]
  Mmap(MmapValueLog),
}

// ValueLog is not thread safe and cannot be used concurrently.
pub struct ValueLog {
  kind: UnsafeCell<ValueLogKind>,
}

impl ValueLog {
  pub fn create(opts: CreateOptions) -> Result<Self, ValueLogError> {
    match opts.in_memory {
      #[cfg(feature = "std")]
      false => Ok(Self {
        kind: UnsafeCell::new(ValueLogKind::Mmap(MmapValueLog::create(opts)?)),
      }),
      _ => Ok(Self {
        kind: UnsafeCell::new(ValueLogKind::Memory(MemoryValueLog::new(
          opts.fid,
          opts.size as usize,
        ))),
      }),
    }
  }

  #[cfg(feature = "std")]
  pub fn open(opts: OpenOptions) -> Result<Self, ValueLogError> {
    Ok(Self {
      kind: UnsafeCell::new(ValueLogKind::Mmap(MmapValueLog::open(opts)?)),
    })
  }

  pub fn write(&mut self, data: &[u8]) -> Result<ValuePointer, ValueLogError> {
    match self.kind_mut() {
      ValueLogKind::Memory(vlf) => vlf.write(data),
      #[cfg(feature = "std")]
      ValueLogKind::Mmap(vlf) => vlf.write(data),
    }
  }

  pub fn read(&self, offset: usize, size: usize) -> Result<&[u8], ValueLogError> {
    match self.kind() {
      ValueLogKind::Memory(vlf) => vlf.read(offset, size),
      #[cfg(feature = "std")]
      ValueLogKind::Mmap(vlf) => vlf.read(offset, size),
    }
  }

  #[inline]
  pub fn rewind(&mut self, size: usize) -> Result<(), ValueLogError> {
    match self.kind_mut() {
      ValueLogKind::Memory(vlf) => {
        vlf.rewind(size);
        Ok(())
      }
      #[cfg(feature = "std")]
      ValueLogKind::Mmap(vlf) => vlf.rewind(size),
    }
  }

  #[inline]
  pub fn len(&self) -> usize {
    match self.kind() {
      ValueLogKind::Memory(vlf) => vlf.len(),
      #[cfg(feature = "std")]
      ValueLogKind::Mmap(vlf) => vlf.len(),
    }
  }

  #[inline]
  pub fn capacity(&self) -> u64 {
    match self.kind() {
      ValueLogKind::Memory(vlf) => vlf.capacity(),
      #[cfg(feature = "std")]
      ValueLogKind::Mmap(vlf) => vlf.capacity(),
    }
  }

  #[inline]
  pub fn remaining(&self) -> u64 {
    match self.kind() {
      ValueLogKind::Memory(vlf) => vlf.remaining(),
      #[cfg(feature = "std")]
      ValueLogKind::Mmap(vlf) => vlf.remaining(),
    }
  }
  #[inline]
  pub fn fid(&self) -> u32 {
    match self.kind() {
      ValueLogKind::Memory(vlf) => vlf.fid(),
      #[cfg(feature = "std")]
      ValueLogKind::Mmap(vlf) => vlf.fid(),
    }
  }

  #[allow(clippy::mut_from_ref)]
  #[inline]
  fn kind_mut(&self) -> &mut ValueLogKind {
    // Safety: ValueLog is not thread safe and will not be used concurrently in this crate.
    unsafe { &mut *self.kind.get() }
  }

  #[inline]
  fn kind(&self) -> &ValueLogKind {
    // Safety: ValueLog is not thread safe and will not be used concurrently in this crate.
    unsafe { &*self.kind.get() }
  }
}
