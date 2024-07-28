use std::sync::Arc;

use crossbeam_skiplist::{SkipMap, map::Entry as CMapEntry};
use either::Either;
use error::ValueLogError;
use lf::LogFile;
use manifest::{ManifestFile, ManifestRecord};
#[cfg(feature = "std")]
use quick_cache::sync::Cache;

use skl::{map::{Entry as MapEntry, EntryRef as MapEntryRef, VersionedEntry, VersionedEntryRef as MapVersionedEntryRef}, Ascend, Trailer};

#[cfg(feature = "std")]
use vlf::ValueLog;

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
      Self::Pointer { pointer, log, .. } => log.read(pointer.offset() as usize, pointer.size() as usize).unwrap(),
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

pub(crate) struct Wal<C = Ascend> {
  fid_generator: Arc<AtomicFid>,

  /// All of the log files.
  lfs: SkipMap<Fid, LogFile<C>>,

  /// The active value log files.
  vlf: Arc<ValueLog>,

  /// Cache for value log files.
  #[cfg(feature = "std")]
  vcache: Option<Arc<Cache<Fid, Arc<ValueLog>>>>,

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
      vlf: Arc::new(ValueLog::placeholder(Fid::MAX)),
      #[cfg(feature = "std")]
      vcache: None,
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

    let vlf = if let Some(fid) = table_manifest.vlogs.last() {
      ValueLog::open(opts.open_options(*fid))?
    } else {
      ValueLog::placeholder(Fid::MAX)
    };

    Ok(Self {
      fid_generator,
      lfs,
      vlf: Arc::new(vlf),
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

          return Ok(Some(EntryRef {
            ent,
            parent: file, 
          }));
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

  pub(crate) fn insert(
    &mut self,
    tid: TableId,
    version: u64,
    key: &[u8],
    val: &[u8],
  ) -> Result<(), Error> {
    let val_len = val.len();

    // First, check if the value is big enough to be written to a standalone value log file
    if val_len as u64 >= self.opts.big_value_threshold {
      let mut meta = Meta::big_value_pointer(version);
      let cks = checksum(meta.raw(), key, Some(val));
      meta.set_checksum(cks);

      return self.insert_entry_to_standalone_vlog(tid, meta, key, val);
    }

    // Second, check if the value is big enough to be written to the shared value log file
    if val_len as u64 >= self.opts.value_threshold {
      let mut meta = Meta::value_pointer(version);
      let cks = checksum(meta.raw(), key, Some(val));
      meta.set_checksum(cks);

      return self.insert_entry_to_shared_vlog(tid, meta, key, val);
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
    let new_lf = LogFile::create(self.cmp.clone(), self.opts.create_options(new_fid))?;
    self
      .manifest
      .lock_me()
      .append(aol::Entry::creation(ManifestRecord::log(new_fid, tid)))?;
    new_lf.insert(meta, key, val)?;
    self.lfs.insert(new_fid, new_lf);
    Ok(())
  }

  fn insert_entry_to_shared_vlog(
    &mut self,
    tid: TableId,
    mut meta: Meta,
    key: &[u8],
    val: &[u8],
  ) -> Result<(), Error> {
    meta.set_value_pointer();

    let mut buf = [0; Pointer::MAX_ENCODING_SIZE];
    let woffset = self.vlf.len();
    match self.vlf.write(meta.version(), key, val, meta.checksum()) {
      Ok(vp) => {
        // This will never fail because the buffer is big enough
        let encoded_size = vp.encode(&mut buf).expect("failed to encode value pointer");
        let vp_buf = &buf[..encoded_size];

        self.insert_to_log(tid, meta, key, vp_buf).map_err(|e| {
          // rewind the value log file
          if let Err(_e) = self.vlf.rewind(woffset) {
            #[cfg(feature = "tracing")]
            tracing::error!(err=%_e, "failed to rewind value log file");
          }
          e
        })
      }
      Err(ValueLogError::NotEnoughSpace { .. }) => {
        let new_fid = self.fid_generator.increment();
        let vlog = ValueLog::create(CreateOptions::new(new_fid))?;
        let vp = vlog
          .write(meta.version(), key, val, meta.checksum())
          .map_err(|e| {
            let _ = vlog.remove();
            e
          })?;

        // This will never fail because the buffer is big enough
        let encoded_size = vp.encode(&mut buf).expect("failed to encode value pointer");
        let vp_buf = &buf[..encoded_size];

        // write new fid to manifest file
        let mut manifest_file = self.manifest.lock_me();

        manifest_file
          .append(aol::Entry::creation_with_custom_flags(
            CustomFlags::empty().with_bit1(),
            ManifestRecord::log(new_fid, tid),
          ))
          .map_err(|e| {
            if let Err(_e) = vlog.remove() {
              #[cfg(feature = "tracing")]
              tracing::error!(err=%_e, "failed to remove unregistered value log file");
            }

            e.into()
          })
          .and_then(|_| {
            self.vlf = Arc::new(vlog);

            let rewind = |res: Error| {
              if let Err(e) = self.vlf.rewind(0) {
                #[cfg(feature = "tracing")]
                tracing::error!(err=%e, "failed to rewind value log file");
              }

              res
            };

            {
              let active_lf = self.lfs.back().expect("no active log file");
              match active_lf.value().insert(meta, key, vp_buf) {
                Ok(_) => return Ok(()),
                Err(LogFileError::Log(skl::map::Error::Arena(
                  skl::ArenaError::InsufficientSpace { .. },
                ))) => {}
                Err(e) => return Err(rewind(e.into())),
              }
            }

            let new_fid = self.fid_generator.increment();
            let new_lf = LogFile::create(self.cmp.clone(), self.opts.create_options(new_fid))
              .map_err(|e| rewind(e.into()))?;
            manifest_file
              .append(aol::Entry::creation(ManifestRecord::log(new_fid, tid)))
              .map_err(|e| rewind(e.into()))?;
            new_lf
              .insert(meta, key, vp_buf)
              .map_err(|e| rewind(e.into()))?;
            self.lfs.insert(new_fid, new_lf);
            Ok(())
          })
      }
      Err(e) => Err(e.into()),
    }
  }

  fn insert_entry_to_standalone_vlog(
    &mut self,
    tid: TableId,
    mut meta: Meta,
    key: &[u8],
    val: &[u8],
  ) -> Result<(), Error> {
    meta.set_big_value_pointer();

    let encoded_entry_size =
      ValueLog::encoded_entry_size(meta.version(), key, val, meta.checksum());

    let new_fid = self.fid_generator.increment();
    let vlog = ValueLog::create(CreateOptions::new(new_fid).with_size(encoded_entry_size as u64))?;
    let vp = vlog
      .write(meta.version(), key, val, meta.checksum())
      .map_err(|e| {
        let _ = vlog.remove();
        e
      })?;

    let mut buf = [0; Pointer::MAX_ENCODING_SIZE];
    // This will never fail because the buffer is big enough
    let encoded_size = vp.encode(&mut buf).expect("failed to encode value pointer");
    let vp_buf = &buf[..encoded_size];

    // write new fid to manifest file
    let mut manifest_file = self.manifest.lock_me();
    manifest_file
      .append(aol::Entry::creation_with_custom_flags(
        CustomFlags::empty().with_bit1(),
        ManifestRecord::log(new_fid, tid),
      ))
      .map_err(|e| {
        if let Err(_e) = vlog.remove() {
          #[cfg(feature = "tracing")]
          tracing::error!(err=%_e, "failed to remove unregistered value log file");
        }

        e.into()
      })
      .and_then(|_| {
        let remove = |res: Error| {
          if let Err(_e) = vlog.remove() {
            #[cfg(feature = "tracing")]
            tracing::error!(err=%_e, "failed to remove unregistered value log file");
          }

          res
        };

        {
          let active_lf = self.lfs.back().expect("no active log file");
          match active_lf.value().insert(meta, key, vp_buf) {
            Ok(_) => return Ok(()),
            Err(LogFileError::Log(skl::map::Error::Arena(
              skl::ArenaError::InsufficientSpace { .. },
            ))) => {}
            Err(e) => return Err(remove(e.into())),
          }
        }

        let new_fid = self.fid_generator.increment();
        let new_lf = LogFile::create(self.cmp.clone(), self.opts.create_options(new_fid))
          .map_err(|e| remove(e.into()))?;
        manifest_file
          .append(aol::Entry::creation(ManifestRecord::log(new_fid, tid)))
          .map_err(|e| remove(e.into()))?;
        new_lf
          .insert(meta, key, vp_buf)
          .map_err(|e| remove(e.into()))?;
        self.lfs.insert(new_fid, new_lf);
        Ok(())
      })
  }
}

