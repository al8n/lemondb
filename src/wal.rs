use std::sync::Arc;

use aol::CustomFlags;
use crossbeam_skiplist::SkipMap;
use error::ValueLogError;
use lf::LogFile;
use manifest::{ManifestFile, ManifestRecord};
#[cfg(feature = "std")]
use quick_cache::sync::Cache;
use skl::{Ascend, Trailer};

#[cfg(feature = "std")]
use vlf::ValueLog;

use crate::options::CreateOptions;

use self::util::checksum;

use super::{
  error::{Error, LogFileError},
  options::LogManagerOptions,
  *,
};

mod lf;
#[cfg(feature = "std")]
mod vlf;

pub(crate) struct LogManager<C = Ascend> {
  /// All of the log files.
  lfs: SkipMap<Fid, LogFile<C>>,

  /// The active value log files.
  #[cfg(feature = "std")]
  vlf: Arc<ValueLog>,

  /// Cache for value log files.
  #[cfg(feature = "std")]
  vcache: Option<Arc<Cache<u32, Arc<ValueLog>>>>,

  manifest: ManifestFile,
  opts: LogManagerOptions,

  cmp: Arc<C>,
}

impl<C: Comparator + Send + Sync + 'static> LogManager<C> {
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

    let last_fid = self.manifest.last_fid();
    let new_fid = last_fid.next();
    let new_lf = LogFile::create(self.cmp.clone(), self.opts.create_options(new_fid))?;
    self
      .manifest
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
        let last_fid = self.manifest.last_fid();
        let new_fid = last_fid.next();
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
        self
          .manifest
          .append(aol::Entry::creation_with_custom_flags(
            CustomFlags::empty().with_bit1(),
            ManifestRecord::log(self.vlf.fid(), tid),
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
            self.insert_to_log(tid, meta, key, vp_buf).map_err(|e| {
              // rewind the value log file
              if let Err(_e) = self.vlf.rewind(0) {
                #[cfg(feature = "tracing")]
                tracing::error!(err=%_e, "failed to rewind value log file");
              }
              e
            })
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

    let last_fid = self.manifest.last_fid();
    let new_fid = last_fid.next();

    let vlog = ValueLog::create(
      CreateOptions::new(new_fid).with_size(self.vlf.encoded_entry_size(
        meta.version(),
        key,
        val,
        meta.checksum(),
      ) as u64),
    )?;

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
    self
      .manifest
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
        self.insert_to_log(tid, meta, key, vp_buf).map_err(|e| {
          if let Err(_e) = vlog.remove() {
            #[cfg(feature = "tracing")]
            tracing::error!(err=%_e, "failed to remove unregistered value log file");
          }
          e
        })
      })
  }
}
