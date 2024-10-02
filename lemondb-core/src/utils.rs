/// Returns the current timestamp in milliseconds.
#[cfg(feature = "ttl")]
#[inline]
pub fn now_timestamp() -> u64 {
  time::OffsetDateTime::now_utc().unix_timestamp() as u64
}
