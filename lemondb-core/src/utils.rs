use crate::types::fid::Fid;

const MAX_DIGITS: usize = 20; // u64::MAX has 20 digits

const VLOG_EXTENSION: &str = "vlog";
const ACTIVE_LOG_EXTENSION: &str = "alog";
const FROZEN_LOG_EXTENSION: &str = "flog";

// 20 digits + 1 dot + 4 extension
const MAX_FILENAME_SUFFIX_LEN: usize = 4 + MAX_DIGITS + 1;

/// Returns the filename for the given `fid` and `ext` extension.
///
/// The filename is in the format `{fid}.{ext}`. e.g., `00000000000000000001.vlog`.
pub fn filename<P>(path: P, fid: Fid, ext: &str) -> std::path::PathBuf
where
  P: AsRef<std::path::Path>,
{
  use std::fmt::Write;

  let mut path = path.as_ref().to_path_buf();
  path.reserve_exact(MAX_FILENAME_SUFFIX_LEN + 1);
  write!(
    path.as_mut_os_string(),
    "{}{:020}.{}",
    std::path::MAIN_SEPARATOR_STR,
    fid,
    ext
  )
  .unwrap();

  path
}

/// Returns the current timestamp in milliseconds.
#[cfg(feature = "ttl")]
#[inline]
pub fn now_timestamp() -> u64 {
  time::OffsetDateTime::now_utc().unix_timestamp() as u64
}
