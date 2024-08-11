use super::{error::LogFileError, options::*, util::validate_checksum, *};

use core::{
  ops::{Bound, RangeBounds},
  sync::atomic::Ordering,
};
use std::io;

use bytes::Bytes;
use skl::{
  map::{EntryRef, VersionedEntryRef},
  Options, SkipMap, Trailer, UnlinkedNode,
};

pub use skl::{
  Ascend, Comparator, Descend, MmapOptions, OpenOptions as SklOpenOptions, VacantBuffer,
};

use either::Either;

mod iter;
pub use iter::*;
mod all_versions_iter;
pub use all_versions_iter::*;

/// A append-only log based on on-disk [`SkipMap`] for key-value databases based on bitcask model.
pub struct LogFile<C = Ascend> {
  pub(super) map: SkipMap<Meta, Arc<C>>,
  fid: Fid,
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
  pub const fn fid(&self) -> Fid {
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
    self.map.allocated()
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
  pub fn create<P: AsRef<std::path::Path>>(
    dir: P,
    cmp: Arc<C>,
    opts: CreateOptions,
  ) -> Result<Self, LogFileError> {
    if let Some(mode) = opts.in_memory {
      let res = match mode {
        MemoryMode::Memory => SkipMap::<Meta, _>::with_options_and_comparator(
          Options::new()
            .with_capacity(opts.size as u32)
            .with_magic_version(CURRENT_VERSION),
          cmp,
        )
        .map_err(Into::into),
        MemoryMode::MmapAnonymous => SkipMap::<Meta, _>::map_anon_with_options_and_comparator(
          Options::new()
            .with_capacity(opts.size as u32)
            .with_magic_version(CURRENT_VERSION),
          skl::MmapOptions::new().len(opts.size as u32),
          cmp,
        )
        .map_err(Into::into),
      };

      return res.map(|map| Self {
        map,
        fid: opts.fid,
        sync_on_write: opts.sync_on_write,
        ro: false,
        minimum: None,
        maximum: None,
        max_version: None,
        min_version: None,
      });
    }

    let open_opts = SklOpenOptions::new()
      .create_new(Some(opts.size as u32))
      .read(true)
      .write(true);
    SkipMap::<Meta, _>::map_mut_with_options_and_comparator_and_path_builder::<
      _,
      core::convert::Infallible,
    >(
      || Ok(filename(dir, opts.fid, LOG_EXTENSION)),
      Options::new().with_magic_version(CURRENT_VERSION),
      open_opts,
      MmapOptions::new(),
      cmp,
    )
    .map(|map| {
      map.allocator().shrink_on_drop(true);

      Self {
        map,
        fid: opts.fid,
        sync_on_write: opts.sync_on_write,
        ro: false,
        minimum: None,
        maximum: None,
        max_version: None,
        min_version: None,
      }
    })
    .map_err(|e| e.unwrap_right().into())
  }

  #[cfg(not(feature = "std"))]
  pub fn create(cmp: C, opts: CreateOptions) -> Result<Self, LogFileError> {
    SkipMap::<Meta, C>::with_options_and_comparator(
      Options::new()
        .with_capacity(opts.size as u32)
        .with_magic_version(CURRENT_VERSION),
      cmp,
    )
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
  pub fn open<P: AsRef<std::path::Path>>(
    path: P,
    cmp: Arc<C>,
    opts: OpenOptions,
  ) -> Result<Self, LogFileError> {
    let open_opts = SklOpenOptions::new().read(true);
    SkipMap::<Meta, _>::map_with_comparator_and_path_builder::<_, core::convert::Infallible>(
      || Ok(filename(path, opts.fid, LOG_EXTENSION)),
      open_opts,
      MmapOptions::new(),
      cmp,
      CURRENT_VERSION,
    )
    .map(|map| {
      map.allocator().shrink_on_drop(true);

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
    .map_err(|e| e.unwrap_right().into())
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
  ) -> Result<Option<EntryRef<'a, Meta>>, LogFileError> {
    match self.map.insert(meta, key, value) {
      Ok(ent) => {
        if self.sync_on_write {
          self.flush()?;
        }
        Ok(ent)
      }
      Err(e) => Err(LogFileError::Log(e)),
    }
  }

  /// Inserts the given key and value to the log.
  #[inline]
  pub fn insert_at_height<'a, 'b: 'a>(
    &'a self,
    meta: Meta,
    height: skl::u5,
    key: &'b [u8],
    value: &'b [u8],
  ) -> Result<Option<EntryRef<'a, Meta>>, LogFileError> {
    match self.map.insert_at_height(meta, height, key, value) {
      Ok(ent) => {
        if self.sync_on_write {
          self.flush()?;
        }
        Ok(ent)
      }
      Err(e) => Err(LogFileError::Log(e)),
    }
  }

  /// Attach an unlinked node to the log.
  #[inline]
  pub fn attach<'a>(&'a self, node: UnlinkedNode<'a, Meta>) -> Result<(), LogFileError> {
    match self.map.link(node) {
      Ok(_) => {
        if self.sync_on_write {
          self.flush()?;
        }
        Ok(())
      }
      Err(e) => Err(LogFileError::Log(e)),
    }
  }

  /// Returns a random height for underlying skip list.
  #[inline]
  pub fn random_height(&self) -> skl::u5 {
    self.map.random_height()
  }

  /// Returns if the log has space for the given key and value.
  #[inline]
  pub fn has_space(&self, height: skl::u5, key: u32, value: u32) -> bool {
    self.map.remaining() >= SkipMap::<Meta, C>::estimated_node_size(height, key, value)
  }

  /// Allocates the given key and value to the log.
  #[inline]
  pub fn allocate<'a, 'b: 'a>(
    &'a self,
    meta: Meta,
    key: &'b [u8],
    value: &'b [u8],
  ) -> Result<UnlinkedNode<'a, Meta>, LogFileError> {
    self
      .map
      .allocate(meta, key, value)
      .map_err(LogFileError::Log)
  }

  /// Allocates the given key and value to the log at the height.
  #[inline]
  pub fn allocate_at_height<'a, 'b: 'a>(
    &'a self,
    meta: Meta,
    height: skl::u5,
    key: &'b [u8],
    value: &'b [u8],
  ) -> Result<UnlinkedNode<'a, Meta>, LogFileError> {
    self
      .map
      .allocate_at_height(meta, height, key, value)
      .map_err(LogFileError::Log)
  }

  /// Inserts a new key if it does not yet exist. Returns `Ok(())` if the key was successfully inserted.
  ///
  /// This method is useful when you want to insert a key and you know the value size but you do not have the value
  /// at this moment.
  ///
  /// A placeholder value will be inserted first, then you will get an [`VacantBuffer`],
  /// and you must fully fill the value with bytes later in the closure.
  #[inline]
  pub fn insert_with_value<'a, 'b: 'a, E>(
    &'a self,
    meta: Meta,
    key: &'b [u8],
    value_size: u32,
    f: impl Fn(&mut VacantBuffer<'a>) -> Result<(), E>,
  ) -> Result<Option<EntryRef<'a, Meta>>, Either<E, LogFileError>> {
    match self.map.insert_with_value(meta, key, value_size, f) {
      Ok(ent) => {
        if self.sync_on_write {
          self.flush().map_err(|e| Either::Right(e.into()))?;
        }
        Ok(ent)
      }
      Err(e) => Err(e.map_right(LogFileError::Log)),
    }
  }

  // /// Inserts a batch of key-value pairs to the log.
  // ///
  // /// ## Warning
  // /// This method does not guarantee atomicity, which means that if the method fails in the middle of writing the batch,
  // /// some of the key-value pairs may be written to the log.
  // #[inline]
  // pub fn insert_many(&self, batch: &[Entry]) -> Result<(), LogFileError> {
  //   for (idx, ent) in batch.iter().enumerate() {
  //     self
  //       .map
  //       .insert(ent.meta(), ent.key(), ent.value())
  //       .map_err(|e| LogFileError::WriteBatch { idx, source: e })?;
  //   }

  //   if self.sync_on_write {
  //     self.flush()?;
  //   }

  //   Ok(())
  // }

  #[inline]
  pub(crate) fn remove(&self, meta: Meta, key: &[u8]) -> Result<(), LogFileError> {
    self
      .map
      .compare_remove(meta, key, Ordering::AcqRel, Ordering::Relaxed)
      .map(|_| ())
      .map_err(Into::into)
  }

  #[inline]
  pub(crate) fn remove_at_height(
    &self,
    meta: Meta,
    height: skl::u5,
    key: &[u8],
  ) -> Result<(), LogFileError> {
    self
      .map
      .compare_remove_at_height(meta, height, key, Ordering::AcqRel, Ordering::Relaxed)
      .map(|_| ())
      .map_err(Into::into)
  }

  #[inline]
  pub(crate) fn allocate_remove_entry_at_height<'a, 'b: 'a>(
    &'a self,
    meta: Meta,
    height: skl::u5,
    key: &'b [u8],
  ) -> Result<UnlinkedNode<'a, Meta>, LogFileError> {
    self
      .map
      .allocate_remove_entry_at_height(meta, height, key)
      .map_err(Into::into)
  }

  /// # Safety
  /// - must ensure that there is only one copy of the log file.
  #[inline]
  pub(crate) unsafe fn remove_file(self) -> Result<(), LogFileError> {
    let path = self.map.allocator().path().cloned().unwrap();
    drop(self);
    std::fs::remove_file(path.as_path()).map_err(Into::into)
  }

  #[inline]
  pub(crate) fn clear(&mut self) -> Result<(), LogFileError> {
    unsafe { self.map.clear().map_err(Into::into) }
  }

  /// Gets the value associated with the given key.
  #[inline]
  pub fn get<'a, 'b: 'a>(
    &'a self,
    version: u64,
    key: &'b [u8],
  ) -> Result<Option<VersionedEntryRef<'a, Meta>>, LogFileError> {
    // fast path
    if !self.contains_version(version) {
      return Ok(None);
    }

    if let Some(maximum) = &self.maximum {
      if self.map.comparator().compare(key, maximum) == core::cmp::Ordering::Greater {
        return Ok(None);
      }
    }

    if let Some(minimum) = &self.minimum {
      if self.map.comparator().compare(key, minimum) == core::cmp::Ordering::Less {
        return Ok(None);
      }
    }

    // fallback to slow path
    match self.map.get_versioned(version, key) {
      Some(ent) => {
        let trailer = ent.trailer();
        validate_checksum(
          trailer.version(),
          ent.key(),
          ent.value(),
          trailer.checksum(),
        )?;
        Ok(Some(ent))
      }
      None => Ok(None),
    }
  }

  /// Returns `true` if the given version is `min_version <= version <= max_version` of the log.
  #[inline]
  pub(crate) fn contains_version(&self, version: u64) -> bool {
    self.min_version() <= version && version <= self.max_version()
  }

  /// Returns `true` if the log contains the given key.
  #[inline]
  pub fn contains_key(&self, version: u64, key: &[u8]) -> Result<bool, LogFileError> {
    self.get(version, key).map(|v| v.is_some())
  }

  /// Returns the first (minimum) key in the log.
  #[inline]
  pub fn first(&self, version: u64) -> Result<Option<EntryRef<Meta>>, LogFileError> {
    match self.map.first(version) {
      Some(ent) => {
        let trailer = ent.trailer();
        validate_checksum(
          trailer.version(),
          ent.key(),
          Some(ent.value()),
          trailer.checksum(),
        )?;
        Ok(Some(ent))
      }
      None => Ok(None),
    }
  }

  /// Returns the last (maximum) key in the log.
  #[inline]
  pub fn last(&self, version: u64) -> Result<Option<EntryRef<Meta>>, LogFileError> {
    match self.map.last(version) {
      Some(ent) => {
        let trailer = ent.trailer();
        validate_checksum(
          trailer.version(),
          ent.key(),
          Some(ent.value()),
          trailer.checksum(),
        )?;
        Ok(Some(ent))
      }
      None => Ok(None),
    }
  }

  /// Returns an iterator over the entries less or equal to the given version in the log.
  #[inline]
  pub fn iter(&self, version: u64) -> LogFileIter<C> {
    LogFileIter {
      iter: self.map.iter(version),
      yield_: self.min_version() <= version,
    }
  }

  /// Returns an iterator over all versions of the entries less or equal to the given version in the log.
  #[inline]
  pub fn iter_all_versions(&self, version: u64) -> LogFileAllVersionsIter<C> {
    LogFileAllVersionsIter {
      iter: self.map.iter_all_versions(version),
      yield_: self.min_version() <= version,
    }
  }

  /// Returns a iterator that within the range, this iterator will yield the latest version of all entries in the range less or equal to the given version.
  #[inline]
  pub fn range<'a, Q, R>(&'a self, version: u64, range: R) -> LogFileIter<'a, C, Q, R>
  where
    &'a [u8]: PartialOrd<Q>,
    Q: ?Sized + PartialOrd<&'a [u8]>,
    R: RangeBounds<Q> + 'a,
  {
    LogFileIter {
      iter: self.map.range(version, range),
      yield_: self.min_version() <= version,
    }
  }

  /// Returns a iterator that within the range, this iterator will yield all versions of all entries in the range less or equal to the given version.
  #[inline]
  pub fn range_all_versions<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> LogFileAllVersionsIter<'a, C, Q, R>
  where
    &'a [u8]: PartialOrd<Q>,
    Q: ?Sized + PartialOrd<&'a [u8]>,
    R: RangeBounds<Q> + 'a,
  {
    LogFileAllVersionsIter {
      iter: self.map.range_all_versions(version, range),
      yield_: self.min_version() <= version,
    }
  }
}
