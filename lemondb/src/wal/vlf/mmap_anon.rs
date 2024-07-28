use memmap2::{MmapMut, MmapOptions};

use super::*;

pub struct MmapAnonValueLog {
  fid: Fid,
  buf: Option<MmapMut>,
  len: u64,
  cap: u64,
}

impl MmapAnonValueLog {
  #[inline]
  pub fn create(opts: CreateOptions) -> Result<Self, ValueLogError> {
    let mmap = MmapOptions::new().len(opts.size as usize).map_anon()?;

    Ok(Self {
      fid: opts.fid,
      buf: Some(mmap),
      len: 0,
      cap: opts.size,
    })
  }

  #[inline]
  pub fn write(
    &mut self,
    version: u64,
    key: &[u8],
    val: &[u8],
    cks: u32,
  ) -> Result<Pointer, ValueLogError> {
    if let Some(mmap) = self.buf.as_mut() {
      let kl = key.len();
      let vl = val.len();
      let h = Header::new(version, kl, vl, cks);
      let encoded_len = h.encoded_len() + kl + vl;

      let offset = self.len as usize;
      if offset as u64 + encoded_len as u64 > self.cap {
        return Err(ValueLogError::NotEnoughSpace {
          required: encoded_len as u64,
          remaining: self.cap - offset as u64,
        });
      }

      let mut cur = offset;
      let header = h.encode()?;

      mmap[cur..cur + header.len].copy_from_slice(&header);
      cur += header.len;
      mmap[cur..cur + kl].copy_from_slice(key);
      cur += kl;
      mmap[cur..cur + vl].copy_from_slice(val);
      cur += vl;

      self.len += cur as u64;

      return Ok(Pointer::new(self.fid, encoded_len as u64, offset as u64));
    }

    Err(ValueLogError::Closed)
  }

  /// Returns a byte slice which contains header, key and value.
  #[inline]
  pub(crate) fn read(&self, offset: usize, size: usize) -> Result<&[u8], ValueLogError> {
    match self.buf.as_ref() {
      None => Err(ValueLogError::Closed),
      Some(buf) => {
        if offset as u64 + size as u64 <= self.len {
          Ok(&buf[offset..offset + size])
        } else {
          Err(ValueLogError::OutOfBound {
            offset,
            len: size,
            size: self.len,
          })
        }
      }
    }
  }

  #[inline]
  pub fn rewind(&mut self, size: usize) -> Result<(), ValueLogError> {
    self.len = self.len.saturating_sub(size as u64);
    Ok(())
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.len as usize
  }

  #[inline]
  pub fn capacity(&self) -> u64 {
    self.cap
  }

  #[inline]
  pub fn remaining(&self) -> u64 {
    self.cap - self.len
  }

  #[inline]
  pub const fn fid(&self) -> Fid {
    self.fid
  }

  #[inline]
  pub fn remove(&mut self) -> Result<(), ValueLogError> {
    self.buf.take();
    Ok(())
  }
}
