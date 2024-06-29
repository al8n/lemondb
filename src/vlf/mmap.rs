use core::{cell::RefCell, mem};
use std::{fmt::Write, fs::File, io::Write as _};

use fs4::FileExt;
use memmap2::{Mmap, MmapMut, MmapOptions};

use super::*;

enum Memmap {
  Unmap,
  Map {
    backed: File,
    mmap: Mmap,
    lock: bool,
  },
  MapMut {
    backed: File,
    mmap: MmapMut,
    lock: bool,
  },
}

pub struct MmapValueLog {
  fid: u32,
  buf: Memmap,
  len: u64,
  cap: u64,
  ro: bool,
}

impl MmapValueLog {
  #[inline]
  pub fn create(opts: CreateOptions) -> Result<Self, ValueLogError> {
    LOG_FILENAME_BUFFER.with(|buf| {
      let mut buf = buf.borrow_mut();
      buf.clear();
      write!(buf, "{:010}.{}", opts.fid, VLOG_EXTENSION).unwrap();
      let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(buf.as_str())?;

      file.set_len(opts.size)?;

      if opts.lock {
        file.lock_exclusive()?;
      }

      let mmap = unsafe { MmapOptions::new().map_mut(&file)? };

      Ok(Self {
        fid: opts.fid,
        buf: Memmap::MapMut {
          backed: file,
          mmap,
          lock: opts.lock,
        },
        len: 0,
        cap: opts.size,
        ro: false,
      })
    })
  }

  pub fn open(opts: OpenOptions) -> Result<Self, ValueLogError> {
    LOG_FILENAME_BUFFER.with(|buf| {
      let mut buf = buf.borrow_mut();
      buf.clear();
      write!(buf, "{:010}.{}", opts.fid, VLOG_EXTENSION).unwrap();
      let file = std::fs::OpenOptions::new().read(true).open(buf.as_str())?;

      if opts.lock {
        file.lock_exclusive()?;
      }

      let cap = file.metadata()?.len();

      let mmap = unsafe { MmapOptions::new().map(&file)? };

      Ok(Self {
        fid: opts.fid,
        buf: Memmap::Map {
          backed: file,
          mmap,
          lock: opts.lock,
        },
        len: cap,
        cap,
        ro: true,
      })
    })
  }

  /// Write a new entry to the value log.
  #[inline]
  pub fn write(&mut self, version: u64, kp: Pointer, val: &[u8]) -> Result<Pointer, ValueLogError> {
    if self.ro {
      return Err(ValueLogError::ReadOnly);
    }

    let vl = val.len();
    let h = Header::new(version, kp, vl);
    let encoded_len = h.encoded_len() + vl;

    match self.buf {
      Memmap::MapMut { ref mut mmap, .. } => {
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

        Ok(Pointer::new(self.fid, encoded_len as u64, offset as u64))
      }
      Memmap::Map { .. } => Err(ValueLogError::ReadOnly),
      _ => Err(ValueLogError::Closed),
    }
  }

  #[inline]
  pub fn read(&self, offset: usize, size: usize) -> Result<&[u8], ValueLogError> {
    Ok(if offset as u64 + size as u64 <= self.cap {
      match self.buf {
        Memmap::Map { ref mmap, .. } => &mmap[offset..offset + size],
        Memmap::MapMut { ref mmap, .. } => &mmap[offset..offset + size],
        _ => return Err(ValueLogError::Closed),
      }
    } else {
      return Err(ValueLogError::OutOfBound {
        offset,
        len: size,
        size: self.len,
      });
    })
  }

  #[inline]
  pub fn rewind(&mut self, size: usize) -> Result<(), ValueLogError> {
    if self.ro {
      return Err(ValueLogError::ReadOnly);
    }

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
  pub const fn fid(&self) -> u32 {
    self.fid
  }

  #[inline]
  pub fn remove(&self) -> Result<(), ValueLogError> {
    LOG_FILENAME_BUFFER.with(|buf| {
      let mut buf = buf.borrow_mut();
      buf.clear();
      write!(buf, "{:010}.{}", self.fid, VLOG_EXTENSION).unwrap();
      std::fs::remove_file(buf.as_str()).map_err(Into::into)
    })
  }
}
