use core::ops::Deref;
use std::sync::Arc;

use bytes::Bytes;
use cache::ValueLogCache;
use crossbeam_skiplist::{map::Entry as CMapEntry, SkipMap};
use either::Either;
use error::ValueLogError;
use lf::LogFile;
use manifest::{ManifestFile, ManifestRecord};
use once_cell::unsync::OnceCell;

use options::OpenOptions;
use skl::{
  map::{
    Entry as MapEntry, EntryRef as MapEntryRef, VersionedEntry,
    VersionedEntryRef as MapVersionedEntryRef,
  },
  Ascend, Trailer,
};

use smallvec_wrapper::SmallVec;
use util::TryMap;
#[cfg(feature = "std")]
pub(crate) use vlf::ValueLog;

use crate::options::CreateOptions;

use self::util::checksum;

use super::{
  error::{Error, LogFileError},
  options::WalOptions,
  *,
};

mod lf;
#[cfg(feature = "std")]
mod vlf;

#[cfg(not(feature = "parking_lot"))]
use std::sync::Mutex;

use aol::CustomFlags;
use manifest::TableManifest;
#[cfg(feature = "parking_lot")]
use parking_lot::Mutex;

enum EntryKind {
  Inlined(VersionedEntry<Meta>),
  Pointer {
    ent: VersionedEntry<Meta>,
    pointer: Pointer,
    log: Arc<ValueLog>,
  },
}

impl EntryKind {
  #[inline]
  const fn from_pointer(pointer: Pointer, ent: VersionedEntry<Meta>, log: Arc<ValueLog>) -> Self {
    Self::Pointer { pointer, log, ent }
  }

  #[inline]
  const fn from_inlined(val: VersionedEntry<Meta>) -> Self {
    Self::Inlined(val)
  }

  #[inline]
  fn trailer(&self) -> &Meta {
    match self {
      Self::Inlined(ent) => ent.trailer(),
      Self::Pointer { ent, .. } => ent.trailer(),
    }
  }

  #[inline]
  fn key(&self) -> &[u8] {
    match self {
      Self::Inlined(ent) => ent.key(),
      Self::Pointer { ent, .. } => ent.key(),
    }
  }

  #[inline]
  fn value(&self) -> &[u8] {
    match self {
      Self::Inlined(ent) => ent.value().unwrap(),
      // TODO: optimize read
      Self::Pointer { pointer, log, .. } => log
        .read(pointer.offset() as usize, pointer.size() as usize)
        .unwrap(),
    }
  }
}

/// A reference to an entry in the log.
pub struct EntryRef<'a, C> {
  ent: EntryKind,
  parent: CMapEntry<'a, Fid, LogFile<C>>,
}

impl<'a, C> core::fmt::Debug for EntryRef<'a, C> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("EntryRef")
      .field("key", &self.ent.key())
      .field("value", &self.ent.value())
      .finish()
  }
}

impl<'a, C> EntryRef<'a, C> {
  /// Returns the key of the entry.
  #[inline]
  pub fn key(&self) -> &[u8] {
    self.ent.key()
  }

  /// Returns the value of the entry.
  #[inline]
  pub fn value(&self) -> &[u8] {
    self.ent.value()
  }
}

enum LazyEntryKind {
  Inlined(VersionedEntry<Meta>),
  Cached {
    ent: VersionedEntry<Meta>,
    pointer: Pointer,
    vlog: Arc<ValueLog>,
  },
  Pointer {
    ent: VersionedEntry<Meta>,
    pointer: Pointer,
    vlog: OnceCell<Arc<ValueLog>>,
    cache: Option<Arc<ValueLogCache>>,
    opts: OpenOptions,
  },
}

/// A lazy reference to an entry in the log.
///
/// For a lazy reference, it may in two states:
/// - The value of the entry is inlined in the log file.
/// - The value of the entry is stored in the value log file.
///
/// For the first state, you can directly access the value of the entry through the [`value`](#method.value) method.
///
/// For the second state, if you want to access the value, you need to call [`init`](#method.init) before calling [`value`](#method.value),
/// or call [`value_or_init`](#method.value_or_init).
/// After the first call to [`init`](#method.init) or [`value_or_init`](#method.value_or_init),
/// you can directly call [`value`](#method.value) to access the value.
///
/// You can use the [`should_init`](#method.should_init) method to determine whether the entry needs to be initialized.
pub struct LazyEntryRef<'a, C> {
  parent: CMapEntry<'a, Fid, LogFile<C>>,
  kind: LazyEntryKind,
}

impl<'a, C: Comparator> LazyEntryRef<'a, C> {
  /// Returns the key of the entry.
  #[inline]
  pub fn key(&self) -> &[u8] {
    match &self.kind {
      LazyEntryKind::Inlined(ent) => ent.key(),
      LazyEntryKind::Pointer { ent, .. } => ent.key(),
      LazyEntryKind::Cached { ent, .. } => ent.key(),
    }
  }

  /// Returns `true` if this entry needs to be initialized before accessing the value.
  #[inline]
  pub fn should_init(&self) -> bool {
    match &self.kind {
      LazyEntryKind::Pointer { .. } => true,
      _ => false,
    }
  }

  /// Returns the value of the entry.
  ///
  /// See [`value_or_init`](#method.value_or_init) for more information.
  ///
  /// # Panic
  /// - If this entry's value is stored in value log file and before calling this method, the value has not been read yet.
  #[inline]
  pub fn value(&self) -> &[u8] {
    match &self.kind {
      LazyEntryKind::Inlined(ent) => ent.value().unwrap(),
      LazyEntryKind::Cached { vlog, pointer, .. } => vlog.read(pointer.offset() as usize, pointer.size() as usize).unwrap(),
      LazyEntryKind::Pointer { vlog, pointer, .. } => {
        vlog.get().expect("value log file has not been loaded yet, please invoke `init` or `value_or_init` before using this method directly.").read(pointer.offset() as usize, pointer.size() as usize).unwrap()
      }
    }
  }

  /// Returns the value of the entry, if it is inlined it will return the value directly,
  /// otherwise it will read the value from the value log file.
  #[inline]
  pub fn value_or_init(&self) -> Result<&[u8], Error> {
    match &self.kind {
      LazyEntryKind::Inlined(ent) => Ok(ent.value().unwrap()),
      LazyEntryKind::Cached { pointer, vlog, .. } => Ok(
        vlog
          .read(pointer.offset() as usize, pointer.size() as usize)
          .unwrap(),
      ),
      LazyEntryKind::Pointer {
        vlog,
        pointer,
        cache,
        opts,
        ..
      } => {
        vlog.get_or_try_init(|| {
          let vlog = ValueLog::open(*opts).map(Arc::new)?;
          if let Some(cache) = cache.as_ref() {
            cache.insert(pointer.fid(), vlog.clone());
          }
          Result::<_, Error>::Ok(vlog)
        })?;

        let fid = pointer.fid();

        let vlog = vlog.get_or_try_init(|| {
          let vlog = ValueLog::open(*opts).map(Arc::new)?;
          if let Some(cache) = cache.as_ref() {
            cache.insert(fid, vlog.clone());
          }
          Result::<_, Error>::Ok(vlog)
        })?;

        vlog
          .read(pointer.offset() as usize, pointer.size() as usize)
          .map_err(Into::into)
      }
    }
  }

  /// Initializes the value log file of this entry.
  ///
  /// Not necessary if the value of this entry is inlined in the log file. Use [`should_init`](#method.should_init) to determine whether initialization is required.
  #[inline]
  pub fn init(&self) -> Result<(), Error> {
    match &self.kind {
      LazyEntryKind::Pointer {
        vlog,
        pointer,
        cache,
        opts,
        ..
      } => {
        vlog.get_or_try_init(|| {
          let vlog = ValueLog::open(*opts).map(Arc::new)?;
          if let Some(cache) = cache.as_ref() {
            cache.insert(pointer.fid(), vlog.clone());
          }
          Result::<_, Error>::Ok(vlog)
        })?;

        Ok(())
      }
      _ => Ok(()),
    }
  }

  #[inline]
  const fn from_inlined(ent: VersionedEntry<Meta>, parent: CMapEntry<'a, Fid, LogFile<C>>) -> Self {
    Self {
      parent,
      kind: LazyEntryKind::Inlined(ent),
    }
  }

  #[inline]
  fn from_cache(
    ent: VersionedEntry<Meta>,
    parent: CMapEntry<'a, Fid, LogFile<C>>,
    pointer: Pointer,
    vlog: Arc<ValueLog>,
  ) -> Self {
    Self {
      parent,
      kind: LazyEntryKind::Cached { ent, pointer, vlog },
    }
  }

  #[inline]
  fn from_pointer(
    ent: VersionedEntry<Meta>,
    parent: CMapEntry<'a, Fid, LogFile<C>>,
    pointer: Pointer,
    opts: OpenOptions,
    cache: Option<Arc<ValueLogCache>>,
  ) -> Self {
    Self {
      parent,
      kind: LazyEntryKind::Pointer {
        ent,
        pointer,
        vlog: OnceCell::new(),
        cache,
        opts,
      },
    }
  }
}

pub(crate) struct Wal<C = Ascend> {
  fid_generator: Arc<AtomicFid>,

  /// All of the log files.
  lfs: SkipMap<Fid, LogFile<C>>,

  /// The value log files, the last one is the active value log file.
  /// not all value log files are stored in this map.
  vlfs: SkipMap<Fid, Arc<ValueLog>>,

  /// Cache for value log files.
  #[cfg(feature = "std")]
  vcache: Option<Arc<ValueLogCache>>,

  manifest: Arc<Mutex<ManifestFile>>,
  opts: WalOptions,

  cmp: Arc<C>,
}

impl<C: Comparator + Send + Sync + 'static> Wal<C> {
  // TODO: support mmap anon and memory create
  pub(crate) fn create(
    fid: Fid,
    fid_generator: Arc<AtomicFid>,
    manifest: Arc<Mutex<ManifestFile>>,
    #[cfg(feature = "std")] cache: Option<Arc<ValueLogCache>>,
    cmp: Arc<C>,
    opts: WalOptions,
  ) -> Result<Self, Error> {
    let map = SkipMap::new();
    map.insert(
      fid,
      LogFile::create(
        cmp.clone(),
        CreateOptions::new(fid)
          .with_size(opts.log_size)
          .with_sync_on_write(opts.sync_on_write),
      )?,
    );

    Ok(Self {
      fid_generator,
      lfs: map,
      vlfs: SkipMap::new(),
      #[cfg(feature = "std")]
      vcache: cache,
      manifest,
      opts,
      cmp,
    })
  }

  pub(crate) fn open(
    table_manifest: &TableManifest,
    fid_generator: Arc<AtomicFid>,
    manifest: Arc<Mutex<ManifestFile>>,
    cmp: Arc<C>,
    opts: WalOptions,
  ) -> Result<Self, Error> {
    let lfs = SkipMap::new();
    for fid in table_manifest.logs.iter() {
      let l = LogFile::open(cmp.clone(), opts.open_options(*fid))?;
      lfs.insert(*fid, l);
    }

    let vlfs = if let Some(fid) = table_manifest.vlogs.last() {
      let map = SkipMap::new();
      map.insert(*fid, Arc::new(ValueLog::open(opts.open_options(*fid))?));
      map
    } else {
      SkipMap::new()
    };

    Ok(Self {
      fid_generator,
      lfs,
      vlfs,
      #[cfg(feature = "std")]
      vcache: None,
      manifest,
      opts,
      cmp,
    })
  }

  pub(crate) fn get<'a, 'b: 'a>(
    &'a self,
    version: u64,
    key: &'b [u8],
  ) -> Result<Option<EntryRef<'a, C>>, Error> {
    for file in self.lfs.iter().rev() {
      let lf = file.value();

      if !lf.contains_version(version) {
        continue;
      }

      match lf.get(version, key) {
        Ok(Some(ent)) => {
          if ent.is_removed() {
            return Ok(None);
          }

          let ent = if ent.trailer().is_pointer() {
            let vp_buf = ent.value().unwrap();
            let (_, vp) = Pointer::decode(vp_buf)?;

            if let Some(cache) = self.vcache.as_ref() {
              if let Some(vlog) = cache.get(&vp.fid()) {
                EntryKind::from_pointer(vp, ent.to_owned(), vlog)
              } else {
                let vlog = Arc::new(ValueLog::open(self.opts.open_options(vp.fid()))?);
                cache.insert(vp.fid(), vlog.clone());
                EntryKind::from_pointer(vp, ent.to_owned(), vlog)
              }
            } else {
              let vlog = Arc::new(ValueLog::open(self.opts.open_options(vp.fid()))?);
              EntryKind::from_pointer(vp, ent.to_owned(), vlog)
            }
          } else {
            EntryKind::from_inlined(ent.to_owned())
          };

          return Ok(Some(EntryRef { ent, parent: file }));
        }
        Ok(None) => continue,
        Err(e) => return Err(e.into()),
      }
    }

    Ok(None)
  }

  pub(crate) fn lazy_get<'a, 'b: 'a>(
    &'a self,
    version: u64,
    key: &'b [u8],
  ) -> Result<Option<LazyEntryRef<'a, C>>, Error> {
    for file in self.lfs.iter().rev() {
      let lf = file.value();

      if !lf.contains_version(version) {
        continue;
      }

      match lf.get(version, key) {
        Ok(Some(ent)) => {
          if ent.is_removed() {
            return Ok(None);
          }

          let ent = if ent.trailer().is_pointer() {
            let vp_buf = ent.value().unwrap();
            let (_, vp) = Pointer::decode(vp_buf)?;
            let fid = vp.fid();

            if let Some(cache) = self.vcache.as_ref() {
              if let Some(vlog) = cache.get(&fid) {
                LazyEntryRef::from_cache(ent.to_owned(), file, vp, vlog)
              } else {
                LazyEntryRef::from_pointer(
                  ent.to_owned(),
                  file,
                  vp,
                  self.opts.open_options(fid),
                  self.vcache.clone(),
                )
              }
            } else {
              LazyEntryRef::from_pointer(
                ent.to_owned(),
                file,
                vp,
                self.opts.open_options(fid),
                self.vcache.clone(),
              )
            }
          } else {
            LazyEntryRef::from_inlined(ent.to_owned(), file)
          };

          return Ok(Some(ent));
        }
        Ok(None) => continue,
        Err(e) => return Err(e.into()),
      }
    }

    Ok(None)
  }

  pub(crate) fn contains<'a, 'b: 'a>(&'a self, version: u64, key: &'b [u8]) -> Result<bool, Error> {
    for file in self.lfs.iter().rev() {
      let lf = file.value();

      if !lf.contains_version(version) {
        continue;
      }

      if lf.contains_key(version, key)? {
        return Ok(true);
      }
    }

    Ok(false)
  }

  pub(crate) fn remove(&mut self, tid: TableId, version: u64, key: &[u8]) -> Result<(), Error> {
    let mut meta = Meta::new(version);
    let cks = checksum(meta.raw(), key, None);
    meta.set_checksum(cks);

    {
      let active_lf = self.lfs.back().expect("no active log file");
      match active_lf.value().remove(meta, key) {
        Ok(_) => return Ok(()),
        Err(LogFileError::Log(skl::map::Error::Arena(skl::ArenaError::InsufficientSpace {
          ..
        }))) => {}
        Err(e) => return Err(e.into()),
      }
    }

    let new_fid = self.fid_generator.increment();
    let new_lf = LogFile::create(self.cmp.clone(), self.opts.create_options(new_fid))?;
    self
      .manifest
      .lock_me()
      .append(aol::Entry::creation(ManifestRecord::log(new_fid, tid)))?;
    new_lf.remove(meta, key)?;
    self.lfs.insert(new_fid, new_lf);
    Ok(())
  }

  pub(crate) fn insert_batch(
    &mut self,
    tid: TableId,
    version: u64,
    mut batch: Batch,
  ) -> Result<(), Error> {
    if batch.len() == 1 {
      let (k, v) = batch.pairs.pop_first().unwrap();
      return match &v.val {
        Some(val) => self.insert(tid, version, &k, val),
        None => self.remove(tid, version, &k),
      };
    }

    struct LogicalValueLog<'a> {
      fid: Fid,
      remaining: u64,
      len: u64,
      vlf: Either<(u64, &'a ValueLog), Option<ValueLog>>,
    }

    struct LogicalLog<'a, COMP = Ascend> {
      fid: Fid,
      remaining: usize,
      num_new_entries: usize,
      lf: Either<(usize, &'a LogFile<COMP>), LogFile<COMP>>,
    }

    impl<'a, COMP> core::ops::Deref for LogicalLog<'a, COMP> {
      type Target = LogFile<COMP>;

      fn deref(&self) -> &Self::Target {
        match &self.lf {
          Either::Left((_, lf)) => lf,
          Either::Right(lf) => lf,
        }
      }
    }

    let mut expected_log_size = 0;
    let mut expected_value_log_size = 0;
    let last = self.lfs.back().expect("no active log file");
    let lf = last.value();
    let log_remaining = lf.map.remaining();
    let log_allocated = lf.map.allocated();

    let active_vlf_entry = match self.vlfs.back() {
      Some(entry) => entry,
      None => {
        let new_fid = self.fid_generator.increment();
        let vlog = ValueLog::create(CreateOptions::new(new_fid))?;
        self
          .manifest
          .lock_me()
          .append(aol::Entry::creation(ManifestRecord::log(new_fid, tid)))?;
        self.vlfs.insert(new_fid, Arc::new(vlog));
        self.vlfs.back().unwrap()
      }
    };

    let active_vlf = active_vlf_entry.value();
    let vlog_remaining = active_vlf.remaining();
    let vlog_len = active_vlf.len();
    let mut logs = SmallVec::new();
    logs.push(LogicalLog {
      fid: lf.fid(),
      lf: Either::Left((log_allocated, lf)),
      remaining: log_remaining,
      num_new_entries: 0,
    });

    let mut vlogs = SmallVec::new();
    vlogs.push(LogicalValueLog {
      fid: active_vlf.fid(),
      remaining: vlog_remaining,
      len: vlog_len as u64,
      vlf: Either::Left((vlog_remaining, active_vlf)),
    });

    // prepare
    let res = batch.pairs.iter_mut().try_for_each(|(k, v)| {
      let height = lf.map.random_height();
      let value_size = match &v.val {
        Some(val) => {
          let raw_val_size = val.len();
          if raw_val_size as u64 > self.opts.value_threshold {
            let mut meta = Meta::value_pointer(version);
            let cks = checksum(meta.raw(), k, Some(val));
            meta.set_checksum(cks);
            v.meta = Some(meta);

            let last_vlog = vlogs.last_mut().unwrap();
            let encoded_size = ValueLog::encoded_entry_size(version, k, val, cks) as u64;
            expected_value_log_size += encoded_size;

            let vp = if last_vlog.remaining >= encoded_size {
              last_vlog.remaining -= encoded_size;
              last_vlog.len += encoded_size;

              Pointer::new(last_vlog.fid, encoded_size, last_vlog.len)
            } else {
              let new_vlog_fid = self.fid_generator.increment();
              // TODO: check if encoded_size less than the largest value log file size
              vlogs.push(LogicalValueLog {
                fid: new_vlog_fid,
                remaining: self.opts.vlog_size - encoded_size,
                len: encoded_size,
                vlf: Either::Right(None)
              });
              Pointer::new(new_vlog_fid, encoded_size, 0)
            };

            v.pointer = Some(BatchValuePointer {
              index: vlogs.len() - 1,
              pointer: vp,
              buf: [0; Pointer::MAX_ENCODING_SIZE],
            });
            vp.encoded_size()
          } else {
            let mut meta = Meta::new(version);
            let cks = checksum(meta.raw(), k, Some(val));
            meta.set_checksum(cks);
            v.meta = Some(meta);

            val.len()
          }
        }
        None => {
          let mut meta = Meta::new(version);
          let cks = checksum(meta.raw(), k, None);
          meta.set_checksum(cks);
          v.meta = Some(meta);

          0
        }
      };
      v.height = height;

      let mut last_lf = logs.last_mut().unwrap();
      let need = skl::SkipMap::<Meta, C>::estimated_node_size(height, k.len() as u32, value_size as u32);
      if last_lf.remaining >= need {
        last_lf.num_new_entries += 1;
        last_lf.remaining -= need;
      } else {
        let fid = self.fid_generator.increment();
        let new_lf = LogFile::create(self.cmp.clone(), self.opts.create_options(fid).with_size(self.opts.log_size))?;
        logs.push(LogicalLog {
          fid,
          remaining: new_lf.map.remaining().checked_sub(need).ok_or(Error::EntryTooLarge)?,
          lf: Either::Right(new_lf),
          num_new_entries: 1,
        });
      }
      expected_log_size += need;

      Ok(())
    });

    // TODO: optimize

    // the current active log file has enough space
    if log_remaining >= expected_log_size {
      let mut unlinked_nodes = SmallVec::with_capacity(batch.pairs.len());
      let mut failure = None;
      for (k, v) in batch.pairs.iter() {
        let meta = v.meta.unwrap();
        let res = match &v.val {
          Some(val) => {
            match &mut v.pointer {
              Some(bvp) => {
                let mut lvl = &mut vlogs[bvp.index];
                let encoded_size = bvp.pointer.encode(bvp.buf.as_mut()).expect("failed to encode value pointer");

                match &lvl.vlf {
                  Either::Left(vlf) => {
                    if let Err(e) = vlf.write(version, k, &bvp.buf[..encoded_size], v.meta.unwrap().checksum()) {
                      failure = Some(Error::ValueLog(e));
                      break;
                    }
                  },
                  Either::Right(Some(vlf)) => {
                    if let Err(e) = vlf.write(version, k, &bvp.buf[..encoded_size], v.meta.unwrap().checksum()) {
                      failure = Some(Error::ValueLog(e));
                      break;
                    }
                  },
                  Either::Right(None) => {
                    let res = ValueLog::create(CreateOptions::new(lvl.fid).with_size(self.opts.vlog_size)).and_then(|vlog| {
                      vlog.write(version, k, &bvp.buf[..encoded_size], v.meta.unwrap().checksum())?;
                      Ok(vlog)
                    });

                    match res {
                      Ok(vlog) => {
                        lvl.vlf = Either::Right(Some(vlog));
                      },
                      Err(e) => {
                        failure = Some(Error::ValueLog(e));
                        break;
                      }
                    }
                  }
                }
                
                lf.allocate_at_height(meta, v.height, k, &bvp.buf[..encoded_size])
              }
              None => lf.allocate_at_height(meta, v.height, k, val),
            }
          }
          None => lf.allocate_remove_entry_at_height(meta, v.height, k),
        };

        match res {
          Ok(node) => unlinked_nodes.push(node),
          Err(e) => {
            failure = Some(e.into());
            break;
          }
        }
      }

      fn cleanup_vlogs_on_failure(logical_vlogs: SmallVec<LogicalValueLog<'_>>) {
        // rewind or remove the value log file
        for lvl in logical_vlogs {
          match lvl.vlf {
            Either::Left((original, vlf)) => {
              if let Err(_e) = vlf.rewind(original as usize) {
                #[cfg(feature = "tracing")]
                tracing::error!(fid = %vlf.fid(), err=%_e, "failed to rewind value log file");
              }
            }
            Either::Right(Some(vlf)) => {
              if let Err(_e) = vlf.remove() {
                #[cfg(feature = "tracing")]
                tracing::error!(fid = %vlf.fid(), err=%_e, "failed to remove unregistered value log file");
              }
            }
            Either::Right(None) => continue,
          }
        }
      }

      if let Some(e) = failure {
        // SAFETY: we are the only one can access the log file, all the nodes are unlinked
        // so it is safe to rewind the allocator
        unsafe { lf.map.rewind(skl::ArenaPosition::Start(log_allocated as u32)) };
        cleanup_vlogs_on_failure(vlogs);
        return Err(e.into());
      }

      // we do not have failure, so we can safely register the value log files

      // TODO: optimize aol crate and avoid allocation here
      let ents = vlogs.iter().skip(1).map(|lvl| aol::Entry::creation_with_custom_flags(CustomFlags::empty().with_bit1(), ManifestRecord::log(lvl.fid, tid))).collect::<Vec<_>>();
      let res = self.manifest.lock_me().append_batch(ents);

      if let Err(e) = res {
        // SAFETY: we are the only one can access the log file, all the nodes are unlinked
        // so it is safe to rewind the allocator
        unsafe { lf.map.rewind(skl::ArenaPosition::Start(log_allocated as u32)) };

        cleanup_vlogs_on_failure(vlogs); 

        return Err(e.into());
      }

      // Happy! we successfully registered all of the value log files

      // link the nodes to the log file
      unlinked_nodes.into_iter().for_each(|node| {
        // SAFETY: we know that the log file is not read-only
        unsafe { lf.map.link_unchecked(node); }
      });

      // update the value log files
      // TODO: update all of the value log files directly
      vlogs.into_iter().skip(1).for_each(|lvl| {
        match lvl.vlf {
          Either::Right(Some(vlf)) => {
            self.update_active_vlog(lvl.fid, vlf);
          },
          Either::Right(None) => {},
          _ => unreachable!(),
        }
      });

      return Ok(());
    }

    // the current active log file does not have enough space
    // so create new log files
    let size = if expected_log_size as u64 > self.opts.log_size {
      expected_log_size as u64
    } else {
      self.opts.log_size
    };

    let new_fid = self.fid_generator.increment();
    let mut new_lf = LogFile::create(
      self.cmp.clone(),
      self.opts.create_options(new_fid).with_size(size),
    )?;



    // Calculate we need how many new value logs to hold the entries
    // the current active value log file has enough space
    if vlog_remaining >= expected_value_log_size as u64 {
      let log_remaining = lf.map.remaining();
      let log_allocated = lf.map.allocated();
      // the current active log file has enough space
      if log_remaining >= expected_log_size {
        let mut unlinked_nodes = SmallVec::with_capacity(batch.pairs.len());
        let mut failure = None;
        for (k, v) in batch.pairs.iter_mut() {
          let meta = v.meta.unwrap();
          let res = match &v.val {
            Some(val) => {
              match &mut v.vp_buf {
                Some(vp_buf) => {
                  let vp = active_vlf.write(meta.version(), k, val, meta.checksum()).map_err(|e| {
                    let _ = active_vlf.rewind(0);
                    e
                  })?;

                  let encoded_size = vp.encode(vp_buf).expect("failed to encode value pointer");
                  lf.allocate_at_height(meta, v.height, k, &vp_buf[..encoded_size])
                },
                None => {
                  lf.allocate_at_height(meta, v.height, k, val)
                }
              }
            }
            None => lf.allocate_remove_entry_at_height(meta, v.height, k),
          };

          match res {
            Ok(node) => unlinked_nodes.push(node),
            Err(e) => {
              failure = Some(e);
              break;
            }
          }
        }

        if let Some(e) = failure {
          // SAFETY: we are the only one can access the log file, all the nodes are unlinked
          // so it is safe to rewind the allocator
          unsafe { lf.map.rewind(skl::ArenaPosition::Start(log_allocated as u32)); }

          // rewind the value log file
          if let Err(_e) = active_vlf.rewind(vlog_allocated) {
            #[cfg(feature = "tracing")]
            tracing::error!(err=%_e, "failed to rewind value log file");
          }
          return Err(e.into());
        }

        return Ok(());
      }
    
      // the current active log file does not have enough space
      // so create a new log file
      let size = if expected_log_size as u64 > self.opts.log_size {
        expected_log_size as u64
      } else {
        self.opts.log_size
      };

      let new_fid = self.fid_generator.increment();
      let mut new_lf = LogFile::create(
        self.cmp.clone(),
        self.opts.create_options(new_fid).with_size(size),
      )?;

      // insert the entires to the unregistered log file first, so that if there is an error, we do not need
      // to create new files for the big value entries
      let res: Result<_, Error> = batch.pairs.iter_mut().try_for_each(|(k, v)| {
        let meta = v.meta.unwrap();
        match &v.val {
          Some(val) => {
            match &mut v.vp_buf {
              Some(vp_buf) => {
                let vp = active_vlf.write(meta.version(), k, val, meta.checksum())?;

                let encoded_size = vp.encode(vp_buf).expect("failed to encode value pointer");
                new_lf.insert_at_height(meta, v.height, k, &vp_buf[..encoded_size])?;
                Ok(())
              },
              None => {
                new_lf.insert_at_height(meta, v.height, k, val)?;
                Ok(())
              }
            }
          },
          None => new_lf.remove_at_height(meta, v.height, k).map_err(Into::into),
        }
      });

      return match res {
        Err(e) => {
          // if there is an error, we can still try to register the log file
          if let Err(_e) = new_lf.clear() {
            #[cfg(feature = "tracing")]
            tracing::error!(fid=%new_lf.fid(), err=%_e, "failed to clear log file");

            // if we fail to clear the log file, we must remove it to avoid intermediate state
            // Safety: we are the only one can access the log file
            if let Err(_e) = unsafe { new_lf.remove_file() } {
              #[cfg(feature = "tracing")]
              tracing::error!(fid=%new_fid, err=%e, "failed to remove log file");
            }

            if let Err(_e) = active_vlf.rewind(vlog_allocated) {
              #[cfg(feature = "tracing")]
              tracing::error!(fid=%active_vlf.fid(), err=%_e, "failed to rewind value log file");
            }

            return Err(e);
          }

          // try to register the log file
          let res = self
            .manifest
            .lock_me()
            .append(aol::Entry::creation(ManifestRecord::log(new_fid, tid)));

          if let Err(_e) = res {
            #[cfg(feature = "tracing")]
            tracing::error!(fid=%new_fid, err=%_e, "failed to register log file");

            // if we fail to register the log file, we must remove it to avoid intermediate state
            // Safety: we are the only one can access the log file
            if let Err(_e) = unsafe { new_lf.remove_file() } {
              #[cfg(feature = "tracing")]
              tracing::error!(fid=%new_fid, err=%_e, "failed to remove unregisted log file");
            }

            if let Err(_e) = active_vlf.rewind(vlog_allocated) {
              #[cfg(feature = "tracing")]
              tracing::error!(fid=%active_vlf.fid(), err=%_e, "failed to rewind value log file");
            }

            return Err(e);
          }

          // we successfully registered the log file, so update the current active log file
          self.lfs.insert(new_fid, new_lf);

          Err(e)
        }
        Ok(_) => {
          // if we reach here, which means all entries are successfully written
          // try to register the log file
          let res = self
            .manifest
            .lock_me()
            .append(aol::Entry::creation(ManifestRecord::log(new_fid, tid)));

          match res {
            Ok(_) => {
              // we successfully registered the log file, so update the current active log file.
              self.lfs.insert(new_fid, new_lf);
              Ok(())
            }
            Err(e) => {
              // if we fail to register the log file, we must remove it to avoid intermediate state
              // Safety: we are the only one can access the log file
              if let Err(_e) = unsafe { new_lf.remove_file() } {
                #[cfg(feature = "tracing")]
                tracing::error!(fid=%new_fid, err=%e, "failed to remove unregistered log file");
              }

              if let Err(_e) = active_vlf.rewind(vlog_allocated) {
                #[cfg(feature = "tracing")]
                tracing::error!(fid=%active_vlf.fid(), err=%_e, "failed to rewind value log file");
              }

              Err(e.into())
            }
          }
        }
      };
    }

    // the current active value log file does not have enough space

    todo!()
  }

  pub(crate) fn insert(
    &mut self,
    tid: TableId,
    version: u64,
    key: &[u8],
    val: &[u8],
  ) -> Result<(), Error> {
    let val_len = val.len();

    // First, check if the value is big enough to be written to the value log file
    if val_len as u64 >= self.opts.value_threshold {
      let mut meta = Meta::value_pointer(version);
      let cks = checksum(meta.raw(), key, Some(val));
      meta.set_checksum(cks);

      return self.insert_entry_to_vlog(tid, meta, key, val);
    }

    let mut meta = Meta::new(version);
    let cks = checksum(meta.raw(), key, Some(val));
    meta.set_checksum(cks);

    self.insert_to_log(tid, meta, key, val)
  }

  #[inline]
  fn insert_to_log(
    &mut self,
    tid: TableId,
    meta: Meta,
    key: &[u8],
    val: &[u8],
  ) -> Result<(), Error> {
    {
      let active_lf = self.lfs.back().expect("no active log file");
      match active_lf.value().insert(meta, key, val) {
        Ok(_) => return Ok(()),
        Err(LogFileError::Log(skl::map::Error::Arena(skl::ArenaError::InsufficientSpace {
          ..
        }))) => {}
        Err(e) => return Err(e.into()),
      }
    }

    let new_fid = self.fid_generator.increment();
    let mut new_lf = LogFile::create(self.cmp.clone(), self.opts.create_options(new_fid))?;
    self
      .manifest
      .lock_me()
      .append(aol::Entry::creation(ManifestRecord::log(new_fid, tid)))?;
    match new_lf.insert(meta, key, val) {
      Ok(_) => {
        self.lfs.insert(new_fid, new_lf);
        Ok(())
      }
      Err(e) => {
        if let Err(_e) = new_lf.clear() {
          #[cfg(feature = "tracing")]
          tracing::error!(err=%_e, "failed to clear log file");
        }

        self.lfs.insert(new_fid, new_lf);
        Err(e.into())
      }
    }
  }

  fn insert_entry_to_vlog(
    &mut self,
    tid: TableId,
    mut meta: Meta,
    key: &[u8],
    val: &[u8],
  ) -> Result<(), Error> {
    meta.set_value_pointer();

    let mut buf = [0; Pointer::MAX_ENCODING_SIZE];
    let active_vlf_entry = match self.vlfs.back() {
      Some(entry) => entry,
      None => {
        let new_fid = self.fid_generator.increment();
        let vlog = ValueLog::create(CreateOptions::new(new_fid))?;
        self
          .manifest
          .lock_me()
          .append(aol::Entry::creation(ManifestRecord::log(new_fid, tid)))?;
        self.vlfs.insert(new_fid, Arc::new(vlog));
        self.vlfs.back().unwrap()
      }
    };
    let active_vlf = active_vlf_entry.value();
    let woffset = active_vlf.len();
    match active_vlf.write(meta.version(), key, val, meta.checksum()) {
      Ok(vp) => {
        // This will never fail because the buffer is big enough
        let encoded_size = vp.encode(&mut buf).expect("failed to encode value pointer");
        let vp_buf = &buf[..encoded_size];
        drop(active_vlf_entry);

        match self.insert_to_log(tid, meta, key, vp_buf) {
          Ok(_) => Ok(()),
          Err(e) => {
            // rewind the value log file
            if let Err(_e) = self.vlfs.back().unwrap().value().rewind(woffset) {
              #[cfg(feature = "tracing")]
              tracing::error!(err=%_e, "failed to rewind value log file");
            }
            Err(e)
          }
        }
      }
      Err(ValueLogError::NotEnoughSpace { .. }) => {
        let new_vlf_fid = self.fid_generator.increment();
        let vlog = ValueLog::create(CreateOptions::new(new_vlf_fid))?;
        let vp = vlog
          .write(meta.version(), key, val, meta.checksum())
          .map_err(|e| {
            let _ = vlog.remove();
            e
          })?;

        // This will never fail because the buffer is big enough
        let encoded_size = vp.encode(&mut buf).expect("failed to encode value pointer");
        let vp_buf = &buf[..encoded_size];

        let rewind = |res: Error, vlf: &ValueLog| {
          if let Err(e) = vlf.rewind(0) {
            #[cfg(feature = "tracing")]
            tracing::error!(err=%e, "failed to rewind value log file");
          }

          res
        };

        let active_lf = self.lfs.back().expect("no active log file");
        let active_lf = active_lf.value();
        let height = active_lf.random_height();
        let has_space = active_lf.has_space(height, key.len() as u32, vp_buf.len() as u32);

        if has_space {
          // register value log file to manifest file
          self
            .manifest
            .lock_me()
            .append(aol::Entry::creation_with_custom_flags(
              CustomFlags::empty().with_bit1(),
              ManifestRecord::log(new_vlf_fid, tid),
            ))
            .map_err(|e| {
              if let Err(_e) = vlog.rewind(0) {
                #[cfg(feature = "tracing")]
                tracing::error!(err=%_e, "failed to remove unregistered value log file");
              }

              e
            })?;

          // update the current value log file
          self.update_active_vlog(new_vlf_fid, vlog);

          // insert the key and value pointer to the active log file
          return active_lf
            .insert_at_height(meta, height, key, vp_buf)
            .map(|_| ())
            .map_err(|e| rewind(e.into(), self.vlfs.back().unwrap().value()));
        }

        // log file does not have enough space, create a new log file
        let new_lf_fid = self.fid_generator.increment();
        let new_lf = LogFile::create(self.cmp.clone(), self.opts.create_options(new_lf_fid));

        match new_lf {
          Err(e) => {
            // we failed to create a new log file,
            // but we can still update the active value log file
            // rewind the value log file first
            let res = rewind(e.into(), &vlog);

            // register new value log file to manifest file
            self
              .manifest
              .lock_me()
              .append(aol::Entry::creation_with_custom_flags(
                CustomFlags::empty().with_bit1(),
                ManifestRecord::log(new_vlf_fid, tid),
              ))
              .map_err(|e| {
                // if we failed to register the new value log file to manifest file
                // then we need to remove the unregistered value log file to avoid intermediate state
                if let Err(_e) = vlog.remove() {
                  #[cfg(feature = "tracing")]
                  tracing::error!(err=%_e, "failed to remove unregistered value log file");
                }

                e
              })?;

            // update the current value log file
            self.update_active_vlog(new_vlf_fid, vlog);

            Err(res)
          }
          Ok(new_lf) => {
            // we successfully created a new log file
            // now register the new value log file and new log file to manifest file
            let res = self.manifest.lock_me().append_batch(vec![
              aol::Entry::creation_with_custom_flags(
                CustomFlags::empty().with_bit1(),
                ManifestRecord::log(new_vlf_fid, tid),
              ),
              aol::Entry::creation(ManifestRecord::log(new_lf_fid, tid)),
            ]);

            match res {
              Ok(_) => {
                // update the active value log file and log file
                self.update_active_vlog(new_vlf_fid, vlog);
                new_lf
                  .insert(meta, key, vp_buf)
                  .map_err(|e| rewind(e.into(), self.vlfs.back().unwrap().value()))?;

                self.lfs.insert(new_lf_fid, new_lf);

                Ok(())
              }
              Err(e) => {
                // if we failed to register the new value log file and new log file to manifest file
                // then we need to remove the unregistered value log file and new log file to avoid intermediate state
                if let Err(_e) = vlog.remove() {
                  #[cfg(feature = "tracing")]
                  tracing::error!(err=%_e, "failed to remove unregistered value log file");
                }

                // SAFETY: we just created the new log file, so it is safe to remove it
                if let Err(_e) = unsafe { new_lf.remove_file() } {
                  #[cfg(feature = "tracing")]
                  tracing::error!(err=%_e, "failed to remove unregistered log file");
                }

                Err(e.into())
              }
            }
          }
        }
      }
      Err(e) => Err(e.into()),
    }
  }

  #[inline]
  fn update_active_vlog(&self, fid: Fid, vlog: ValueLog) {
    // update the current value log file
    self.vlfs.insert(fid, Arc::new(vlog));
    if self.vlfs.len() > self.opts.max_immutable_vlogs as usize + 1 {
      if let Some(old_vlf) = self.vlfs.pop_front() {
        let old_vlf = old_vlf.value();
        if !old_vlf.is_placeholder() {
          // if we have a cache, insert the oldest value log file to the cache
          #[cfg(feature = "std")]
          if let Some(ref vcache) = self.vcache {
            vcache.insert(old_vlf.fid(), old_vlf.clone());
          }
        }
      }
    }
  }
}
