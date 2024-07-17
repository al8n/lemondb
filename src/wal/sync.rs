use super::*;

#[cfg(not(feature = "parking_lot"))]
use std::sync::Mutex;

use aol::CustomFlags;
use manifest::TableManifest;
#[cfg(feature = "parking_lot")]
use parking_lot::Mutex;

pub(crate) struct Wal<C = Ascend> {
  fid_generator: Arc<AtomicFid>,

  /// All of the log files.
  lfs: SkipMap<Fid, LogFile<C>>,

  /// The active value log files.
  vlf: Arc<ValueLog>,

  /// Cache for value log files.
  #[cfg(feature = "std")]
  vcache: Option<Arc<Cache<u32, Arc<ValueLog>>>>,

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
