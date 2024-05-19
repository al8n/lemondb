use bytes::{BufMut, BytesMut};

use crate::{error::ValueLogError, ValuePointer};

use super::Header;

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
  pub fn write(
    &mut self,
    version: u64,
    key: &[u8],
    val: &[u8],
    cks: u32,
  ) -> Result<ValuePointer, ValueLogError> {
    let kl = key.len();
    let vl = val.len();
    let h = Header::new(version, kl, vl, cks);
    let encoded_len = h.encoded_len() + kl + vl;

    let offset = self.buf.len() as usize;
    if offset + encoded_len > self.cap {
      return Err(ValueLogError::NotEnoughSpace {
        required: encoded_len as u64,
        remaining: (self.cap - offset) as u64,
      });
    }

    let header = h.encode()?;

    self.buf.put_slice(&header);
    self.buf.put_slice(key);
    self.buf.put_slice(val);

    Ok(ValuePointer::new(
      self.fid,
      encoded_len as u64,
      offset as u64,
    ))
  }

  #[inline]
  pub fn read(&self, offset: usize, size: usize) -> Result<&[u8], ValueLogError> {
    if offset + size <= self.buf.len() {
      Ok(&self.buf[offset..offset + size])
    } else {
      Err(ValueLogError::OutOfBound {
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
