use bytes::{BufMut, BytesMut};

use crate::ValuePointer;

use super::error::Error;

pub struct MemoryValueLog {
  fid: u32,
  buf: BytesMut,
  cap: usize,
}

impl MemoryValueLog {
  #[inline]
  pub fn new(fid: u32, cap: usize) -> Self {
    Self {
      fid,
      buf: BytesMut::with_capacity(cap),
      cap,
    }
  }

  #[inline]
  pub fn write(&mut self, data: &[u8]) -> Result<ValuePointer, Error> {
    let offset = self.buf.len();

    if offset + data.len() > self.cap {
      return Err(Error::NotEnoughSpace {
        required: data.len() as u64,
        remaining: (self.cap - offset) as u64,
      });
    }

    self.buf.put_slice(data);
    Ok(ValuePointer::new(
      self.fid,
      data.len() as u64,
      offset as u64,
    ))
  }

  #[inline]
  pub fn read(&self, offset: usize, size: usize) -> Result<&[u8], Error> {
    if offset + size <= self.buf.len() {
      Ok(&self.buf[offset..offset + size])
    } else {
      Err(Error::OutOfBound {
        offset,
        len: size,
        size: self.buf.len() as u64,
      })
    }
  }

  #[inline]
  pub fn rewind(&mut self, size: usize) {
    self.buf.truncate(size);
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.buf.len()
  }

  #[inline]
  pub fn capacity(&self) -> u64 {
    self.cap as u64
  }

  #[inline]
  pub fn remaining(&self) -> u64 {
    (self.buf.capacity() - self.buf.len()) as u64
  }

  #[inline]
  pub const fn fid(&self) -> u32 {
    self.fid
  }
}
