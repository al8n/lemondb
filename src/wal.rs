use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
use lf::LogFile;
use manifest::ManifestFile;
#[cfg(feature = "std")]
use quick_cache::sync::Cache;
use skl::{Ascend, Trailer};

#[cfg(feature = "std")]
use vlf::ValueLog;

use crate::options::CreateOptions;

use self::{manifest::ManifestEvent, util::checksum};

use super::{
  error::{Error, LogFileError},
  options::LogManagerOptions,
  *,
};

pub struct LogManager<C = Ascend> {
  /// All of the log files.
  lfs: SkipMap<u32, LogFile<C>>,

  /// The active value log files.
  #[cfg(feature = "std")]
  vlfs: SkipMap<u32, Arc<ValueLog>>,

  /// Cache for value log files.
  #[cfg(feature = "std")]
  vcache: Option<Arc<Cache<u32, Arc<ValueLog>>>>,

  manifest: ManifestFile,
  opts: LogManagerOptions,

  cmp: C,
}

impl<C: Comparator + Clone + Send + 'static> LogManager<C> {
  pub fn insert_bytes(&mut self, version: u64, key: &[u8], val: &[u8]) -> Result<(), Error> {
    let val_len = val.len();

    // First, check if the value is big enough to be written to a standalone value log file
    if val_len as u64 >= self.opts.big_value_threshold {
      let mut meta = Meta::big_value_pointer(version);
      let cks = checksum(meta.raw(), key, Some(val));
      meta.set_checksum(cks);

      return self.insert_entry_to_standalone_vlog(meta, key, val);
    }

    // Second, check if the value is big enough to be written to the shared value log file
    if val_len as u64 >= self.opts.value_threshold {
      let mut meta = Meta::value_pointer(version);
      let cks = checksum(meta.raw(), key, Some(val));
      meta.set_checksum(cks);

      return self.insert_entry_to_shared_vlog(meta, key, val);
    }

    let mut meta = Meta::new(version);
    let cks = checksum(meta.raw(), key, Some(val));
    meta.set_checksum(cks);

    self.insert_to_log(meta, key, val)
  }

  #[inline]
  fn insert_to_log(&mut self, meta: Meta, key: &[u8], val: &[u8]) -> Result<(), Error> {
    {
      let active_lf = self.lfs.back().expect("no active log file");
      match active_lf.value().insert(meta, key, val) {
        Ok(_) => return Ok(()),
        Err(LogFileError::Log(skl::map::Error::Full(_))) => {}
        Err(e) => return Err(e.into()),
      }
    }

    let last_fid = self.manifest.last_fid();
    let new_fid = last_fid + 1;
    let new_lf = LogFile::create(self.cmp.clone(), self.opts.create_options(new_fid))?;
    self.manifest.append(ManifestEvent::add_log(new_fid))?;
    new_lf.insert(meta, key, val)?;
    self.lfs.insert(new_fid, new_lf);
    Ok(())
  }

  fn insert_entry_to_shared_vlog(
    &mut self,
    mut meta: Meta,
    key: &[u8],
    val: &[u8],
  ) -> Result<(), Error> {
    meta.set_value_pointer();

    todo!()
  }

  fn insert_entry_to_standalone_vlog(
    &mut self,
    mut meta: Meta,
    key: &[u8],
    val: &[u8],
  ) -> Result<(), Error> {
    meta.set_big_value_pointer();

    let last_fid = self.manifest.last_fid();
    let new_fid = last_fid + 1;

    let mut vlog = ValueLog::create(CreateOptions::new(new_fid))?;
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

    self.insert_to_log(meta, key, vp_buf).and_then(|_| {
      // write new fid to manifest file
      self
        .manifest
        .append(ManifestEvent::add_vlog(new_fid))
        .map_err(Into::into)
    })
  }
}
