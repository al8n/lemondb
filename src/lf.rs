use super::{error::LogFileError, options::*, *};

use core::{
  cell::RefCell,
  ops::{Bound, RangeBounds},
};
use std::io;

use bytes::Bytes;
use skl::SkipMap;

pub use skl::{
  Ascend, Comparator, Descend, MmapOptions, OccupiedValue, OpenOptions as SklOpenOptions,
};

use either::Either;

mod iterator;
pub use iterator::*;

const EXTENSION: &str = "klog";

std::thread_local! {
  static BUF: RefCell<std::string::String> = RefCell::new(std::string::String::with_capacity(11));
}

/// A append-only log based on on-disk [`SkipMap`] for key-value databases based on bitcask model.
pub struct LogFile<C = Ascend> {
  map: SkipMap<Meta, C>,
  fid: u32,
  sync_on_write: bool,
  ro: bool,
  minimum: Option<Bytes>,
  maximum: Option<Bytes>,
  max_version: Option<u64>,
  min_version: Option<u64>,
}

impl<C> LogFile<C> {
  /// Flushes outstanding memory map modifications to disk.
  #[cfg(feature = "std")]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  #[inline]
  pub fn flush(&self) -> io::Result<()> {
    self.map.flush()
  }

  /// Asynchronously flushes outstanding memory map modifications to disk.
  #[cfg(feature = "std")]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  #[inline]
  pub fn flush_async(&self) -> io::Result<()> {
    self.map.flush_async()
  }

  /// Returns the file ID of the log.
  #[inline]
  pub const fn fid(&self) -> u32 {
    self.fid
  }

  /// Returns `true` if the log is read only.
  #[inline]
  pub const fn read_only(&self) -> bool {
    self.ro
  }

  /// Returns the current size of the log.
  #[inline]
  pub fn size(&self) -> usize {
    self.map.size()
  }

  /// Returns the capacity of the log.
  #[inline]
  pub fn capacity(&self) -> usize {
    self.map.capacity()
  }
}

impl<C: Comparator> LogFile<C> {
  /// Create a new log with the given options.
  #[cfg(feature = "std")]
  pub fn create(cmp: C, opts: CreateOptions) -> Result<Self, LogFileError> {
    use std::fmt::Write;

    if opts.in_memory {
      return SkipMap::<Meta, C>::with_comparator(opts.size as usize, cmp)
        .map(|map| Self {
          map,
          fid: opts.fid,
          sync_on_write: opts.sync_on_write,
          ro: false,
          minimum: None,
          maximum: None,
          max_version: None,
          min_version: None,
        })
        .map_err(Into::into);
    }

    BUF.with(|buf| {
      let mut buf = buf.borrow_mut();
      buf.clear();
      write!(buf, "{:06}.{}", opts.fid, EXTENSION).unwrap();
      let open_opts = SklOpenOptions::new()
        .create_new(Some(opts.size))
        .read(true)
        .write(true)
        .lock_exclusive(opts.lock)
        .shrink_on_drop(true);
      SkipMap::<Meta, C>::mmap_mut_with_comparator(buf.as_str(), open_opts, MmapOptions::new(), cmp)
        .map(|map| Self {
          map,
          fid: opts.fid,
          sync_on_write: opts.sync_on_write,
          ro: false,
          minimum: None,
          maximum: None,
          max_version: None,
          min_version: None,
        })
        .map_err(Into::into)
    })
  }

  #[cfg(not(feature = "std"))]
  pub fn create(cmp: C, opts: CreateOptions) -> Result<Self, LogFileError> {
    SkipMap::<Meta, C>::with_comparator(opts.size, cmp)
      .map(|map| Self {
        map,
        fid: opts.fid,
        sync_on_write: opts.sync_on_write,
        ro: false,
        minimum: None,
        maximum: None,
        max_version: None,
        min_version: None,
      })
      .map_err(Into::into)
  }

  /// Open an existing log with the given options.
  ///
  /// **Note**: `LogFile` constructed with this method is read only.
  #[cfg(feature = "std")]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  pub fn open(cmp: C, opts: OpenOptions) -> Result<Self, LogFileError> {
    use std::fmt::Write;

    BUF.with(|buf| {
      let mut buf = buf.borrow_mut();
      buf.clear();
      write!(buf, "{:06}.{}", opts.fid, EXTENSION).unwrap();
      let open_opts = SklOpenOptions::new()
        .read(true)
        .lock_shared(opts.lock)
        .shrink_on_drop(true);
      SkipMap::<Meta, C>::mmap_with_comparator(buf.as_str(), open_opts, MmapOptions::new(), cmp)
        .map(|map| {
          let max_version = map.max_version();
          let min_version = map.min_version();
          let minimum = map
            .first(max_version)
            .map(|ent| Bytes::copy_from_slice(ent.key()));
          let maximum = map
            .last(max_version)
            .map(|ent| Bytes::copy_from_slice(ent.key()));
          Self {
            map,
            fid: opts.fid,
            sync_on_write: false,
            ro: true,
            minimum,
            maximum,
            max_version: Some(max_version),
            min_version: Some(min_version),
          }
        })
        .map_err(Into::into)
    })
  }

  #[inline]
  pub fn max_version(&self) -> u64 {
    self.max_version.unwrap_or_else(|| self.map.max_version())
  }

  #[inline]
  pub fn min_version(&self) -> u64 {
    self.min_version.unwrap_or_else(|| self.map.min_version())
  }

  /// Inserts the given key and value to the log.
  #[inline]
  pub fn insert<'a, 'b: 'a>(
    &'a self,
    meta: Meta,
    key: &'b [u8],
    value: &'b [u8],
  ) -> Result<Option<EntryRef<'a, C>>, LogFileError> {
    match self.map.insert(meta, key, value) {
      Ok(ent) => {
        if self.sync_on_write {
          self.flush()?;
        }
        Ok(ent.map(EntryRef::new))
      }
      Err(e) => Err(LogFileError::Log(e)),
    }
  }

  /// Inserts a new key if it does not yet exist. Returns `Ok(())` if the key was successfully inserted.
  ///
  /// This method is useful when you want to insert a key and you know the value size but you do not have the value
  /// at this moment.
  ///
  /// A placeholder value will be inserted first, then you will get an [`OccupiedValue`],
  /// and you must fully fill the value with bytes later in the closure.
  #[inline]
  pub fn insert_with<'a, 'b: 'a, E>(
    &'a self,
    meta: Meta,
    key: &'b [u8],
    value_size: u32,
    f: impl FnOnce(OccupiedValue<'a>) -> Result<(), E>,
  ) -> Result<Option<EntryRef<'a, C>>, Either<E, LogFileError>> {
    match self.map.insert_with(meta, key, value_size, f) {
      Ok(ent) => {
        if self.sync_on_write {
          self.flush().map_err(|e| Either::Right(e.into()))?;
        }
        Ok(ent.map(EntryRef::new))
      }
      Err(e) => Err(e.map_right(LogFileError::Log)),
    }
  }

  /// Inserts a batch of key-value pairs to the log.
  ///
  /// ## Warning
  /// This method does not guarantee atomicity, which means that if the method fails in the middle of writing the batch,
  /// some of the key-value pairs may be written to the log.
  #[inline]
  pub fn insert_many(&self, batch: &[Entry]) -> Result<(), LogFileError> {
    for (idx, ent) in batch.iter().enumerate() {
      self
        .map
        .insert(ent.meta(), ent.key(), ent.value())
        .map_err(|e| LogFileError::WriteBatch { idx, source: e })?;
    }

    if self.sync_on_write {
      self.flush()?;
    }

    Ok(())
  }

  /// Gets the value associated with the given key.
  #[inline]
  pub fn get<'a, 'b: 'a>(&'a self, version: u64, key: &'b [u8]) -> Option<EntryRef<'a, C>> {
    // fast path
    if version < self.min_version() {
      return None;
    }

    if let Some(maximum) = &self.maximum {
      if self.map.comparator().compare(key, maximum) == core::cmp::Ordering::Greater {
        return None;
      }
    }

    if let Some(minimum) = &self.minimum {
      if self.map.comparator().compare(key, minimum) == core::cmp::Ordering::Less {
        return None;
      }
    }

    // fallback to slow path
    self.map.get(version, key).and_then(|ent| {
      if ent.trailer().is_removed() {
        None
      } else {
        Some(EntryRef::new(ent))
      }
    })
  }

  /// Returns `true` if the log contains the given key.
  #[inline]
  pub fn contains_key(&self, version: u64, key: &[u8]) -> bool {
    self.get(version, key).is_some()
  }

  /// Returns the first (minimum) key in the log.
  #[inline]
  pub fn first(&self, version: u64) -> Option<EntryRef<C>> {
    self.map.first(version).map(EntryRef::new)
  }

  /// Returns the last (maximum) key in the log.
  #[inline]
  pub fn last(&self, version: u64) -> Option<EntryRef<C>> {
    self.map.last(version).map(EntryRef::new)
  }

  /// Returns an iterator over the entries less or equal to the given version in the log.
  #[inline]
  pub fn iter(&self, version: u64) -> LogFileIterator<C> {
    LogFileIterator {
      iter: self.map.iter(version),
      all_versions: false,
      yield_: self.min_version() <= version,
    }
  }

  /// Returns an iterator over all versions of the entries less or equal to the given version in the log.
  #[inline]
  pub fn iter_all_versions(&self, version: u64) -> LogFileIterator<C> {
    LogFileIterator {
      iter: self.map.iter_all_versions(version),
      all_versions: true,
      yield_: self.min_version() <= version,
    }
  }

  /// Returns a iterator that within the range, this iterator will yield the latest version of all entries in the range less or equal to the given version.
  #[inline]
  pub fn range<'a, Q, R>(&'a self, version: u64, range: R) -> LogFileIterator<'a, C, Q, R>
  where
    &'a [u8]: PartialOrd<Q>,
    Q: ?Sized + PartialOrd<&'a [u8]>,
    R: RangeBounds<Q> + 'a,
  {
    LogFileIterator {
      iter: self.map.range(version, range),
      all_versions: false,
      yield_: self.min_version() <= version,
    }
  }

  /// Returns a iterator that within the range, this iterator will yield all versions of all entries in the range less or equal to the given version.
  #[inline]
  pub fn range_all_versions<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> LogFileIterator<'a, C, Q, R>
  where
    &'a [u8]: PartialOrd<Q>,
    Q: ?Sized + PartialOrd<&'a [u8]>,
    R: RangeBounds<Q> + 'a,
  {
    LogFileIterator {
      iter: self.map.range_all_versions(version, range),
      all_versions: true,
      yield_: self.min_version() <= version,
    }
  }
}
