use bytes::{BufMut, BytesMut};

use crate::ValuePointer;

pub struct MemoryValueLog {
  fid: u32,
  buf: BytesMut,
}

impl MemoryValueLog {
  #[inline]
  pub fn new(fid: u32, cap: usize) -> Self {
    Self {
      fid,
      buf: BytesMut::with_capacity(cap),
    }
  }

  #[inline]
  pub fn write(&mut self, data: &[u8]) -> ValuePointer {
    let offset = self.buf.len();
    self.buf.put_slice(data);
    ValuePointer::new(self.fid, data.len() as u64, offset as u64)
  }

  #[inline]
  pub fn read(&self, offset: usize, size: usize) -> Option<&[u8]> {
    if offset + size <= self.buf.len() {
      Some(&self.buf[offset..offset + size])
    } else {
      None
    }
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.buf.len()
  }

  #[inline]
  pub const fn fid(&self) -> u32 {
    self.fid
  }
}
