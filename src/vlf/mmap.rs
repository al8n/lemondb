use core::{cell::RefCell, mem};
use std::{fmt::Write, fs::File, io::Write as _};

use fs4::FileExt;
use memmap2::{Mmap, MmapMut, MmapOptions};

use super::*;

const EXTENSION: &str = "vlog";
const CHECKSUM_OVERHEAD: u64 = 4;

std::thread_local! {
  static BUF: RefCell<std::string::String> = RefCell::new(std::string::String::with_capacity(11));
}

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

impl Memmap {
  fn unmount(&mut self, size: u64) {
    match self {
      Memmap::Map { backed, lock, .. } => {
        if *lock {
          let _ = backed.unlock();
        }
      }
      Memmap::MapMut {
        backed,
        lock,
        ref mut mmap,
      } => {
        let cks = crc32fast::hash(&mmap[..size as usize]);
        mmap[size as usize..size as usize + CHECKSUM_OVERHEAD as usize]
          .copy_from_slice(&cks.to_le_bytes());

        if let Err(e) = backed.set_len(size + CHECKSUM_OVERHEAD) {
          tracing::error!(err=%e, "failed to truncate value log");
        }

        if let Err(e) = backed.flush() {
          tracing::error!(err=%e, "failed to flush value log");
        }

        if let Err(e) = backed.sync_all() {
          tracing::error!(err=%e, "failed to sync value log");
        }

        if *lock {
          let _ = backed.unlock();
        }
      }
      _ => {}
    }
  }
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
    BUF.with(|buf| {
      let mut buf = buf.borrow_mut();
      buf.clear();
      write!(buf, "{:06}.{}", opts.fid, EXTENSION).unwrap();
      let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(buf.as_str())?;

      file.set_len(opts.size.saturating_add(CHECKSUM_OVERHEAD))?;

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
    BUF.with(|buf| {
      let mut buf = buf.borrow_mut();
      buf.clear();
      write!(buf, "{:06}.{}", opts.fid, EXTENSION).unwrap();
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
        len: cap - CHECKSUM_OVERHEAD,
        cap: cap - CHECKSUM_OVERHEAD,
        ro: true,
      })
    })
  }

  #[inline]
  pub fn write(
    &mut self,
    version: u64,
    key: &[u8],
    val: &[u8],
    cks: u32,
  ) -> Result<ValuePointer, ValueLogError> {
    if self.ro {
      return Err(ValueLogError::ReadOnly);
    }

    let kl = key.len();
    let vl = val.len();
    let h = Header::new(version, kl, vl, cks);
    let encoded_len = h.encoded_len() + kl + vl;

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

        Ok(ValuePointer::new(
          self.fid,
          encoded_len as u64,
          offset as u64,
        ))
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
}
