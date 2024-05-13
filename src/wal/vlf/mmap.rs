use core::cell::RefCell;
use std::{fmt::Write, fs::File, io::Write as _};

use fs4::FileExt;
use memmap2::{Mmap, MmapMut, MmapOptions};

use super::{error::Error, options::*, *};

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
  pub fn create(opts: CreateOptions) -> Result<Self, Error> {
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

  pub fn open(opts: OpenOptions) -> Result<Self, Error> {
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
  pub fn write(&mut self, data: &[u8]) -> Result<ValuePointer, Error> {
    if self.ro {
      return Err(Error::ReadOnly);
    }

    match self.buf {
      Memmap::MapMut { ref mut mmap, .. } => {
        let len = data.len();
        let offset = self.len as usize;
        if offset as u64 + len as u64 + CHECKSUM_OVERHEAD > self.cap {
          return Err(Error::NotEnoughSpace {
            required: len as u64,
            remaining: self.cap - offset as u64,
          });
        }

        mmap[offset..offset + len].copy_from_slice(data);
        let cks = crc32fast::hash(&mmap[offset..offset + len]);
        mmap[offset + len..offset + len + CHECKSUM_OVERHEAD as usize].copy_from_slice(&cks.to_le_bytes());
        self.len += len as u64 + CHECKSUM_OVERHEAD;
        Ok(ValuePointer::new(self.fid, len as u64, offset as u64))
      }
      Memmap::Map { .. } => Err(Error::ReadOnly),
      _ => Err(Error::Closed),
    }
  }

  #[inline]
  pub fn read(&self, offset: usize, size: usize) -> Result<&[u8], Error> {
    Ok(if offset as u64 + size as u64 <= self.cap {
      match self.buf {
        Memmap::Map { ref mmap, .. } => &mmap[offset..offset + size],
        Memmap::MapMut { ref mmap, .. } => &mmap[offset..offset + size],
        _ => return Err(Error::Closed),
      }
    } else {
      return Err(Error::OutOfBound {
        offset,
        len: size,
        size: self.len,
      });
    })
  }

  #[inline]
  pub fn rewind(&mut self, size: usize) -> Result<(), Error> {
    if self.ro {
      return Err(Error::ReadOnly);
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
