use std::fs::File;

use fs4::fs_std::FileExt;
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

// impl Memmap {
//   fn truncate(&mut self, len: u64) -> Result<(), ValueLogError> {
//     match self {
//       Memmap::Map { .. } => Err(ValueLogError::ReadOnly),
//       Memmap::MapMut { backed, mmap, .. } => {
//         backed.set_len(len)?;

//         unsafe { ptr::drop_in_place(mmap) };

//         *mmap = unsafe { MmapOptions::new().map_mut(&*backed)? };
//         Ok(())
//       }
//       _ => Err(ValueLogError::Closed),
//     }
//   }
// }

pub struct MmapValueLog {
  fid: Fid,
  buf: Memmap,
  len: u64,
  cap: u64,
  ro: bool,
}

impl MmapValueLog {
  #[inline]
  pub fn create<P: AsRef<std::path::Path>>(
    path: P,
    opts: CreateOptions,
  ) -> Result<Self, ValueLogError> {
    with_filename(path, opts.fid, VLOG_EXTENSION, |path| {
      let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)?;

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

  pub fn open<P: AsRef<std::path::Path>>(
    path: P,
    opts: OpenOptions,
  ) -> Result<Self, ValueLogError> {
    with_filename(path, opts.fid, VLOG_EXTENSION, |path| {
      let file = std::fs::OpenOptions::new().read(true).open(path)?;

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

  #[inline]
  pub fn write(
    &mut self,
    version: u64,
    key: &[u8],
    val: &[u8],
    cks: u32,
  ) -> Result<Pointer, ValueLogError> {
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
        cur += vl;

        self.len += cur as u64;

        Ok(Pointer::new(self.fid, encoded_len as u64, offset as u64))
      }
      Memmap::Map { .. } => Err(ValueLogError::ReadOnly),
      _ => Err(ValueLogError::Closed),
    }
  }

  /// Returns a byte slice which contains header, key and value.
  #[inline]
  pub(crate) fn read(&self, offset: usize, size: usize) -> Result<&[u8], ValueLogError> {
    Ok(if offset as u64 + size as u64 <= self.len {
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
  pub const fn fid(&self) -> Fid {
    self.fid
  }

  #[inline]
  pub fn remove<P: AsRef<std::path::Path>>(&self, dir: P) -> Result<(), ValueLogError> {
    with_filename(dir, self.fid, VLOG_EXTENSION, |path| {
      std::fs::remove_file(path).map_err(Into::into)
    })
  }
}
