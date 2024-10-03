use core::cmp;

use dbutils::traits::Type;

use super::{generic_key_ref::GenericKeyRef, meta::Meta};

/// An internal generic key.
pub struct GenericKey<K: ?Sized> {
  pub(super) meta: Meta,
  pub(super) data: K,
}

impl<K> PartialEq for GenericKey<K>
where
  K: ?Sized + PartialEq,
{
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.meta.raw() == other.meta.raw() && self.data.eq(&other.data)
  }
}

impl<K> Eq for GenericKey<K> where K: ?Sized + Eq {}

impl<K> PartialOrd for GenericKey<K>
where
  K: ?Sized + Ord,
{
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<K> Ord for GenericKey<K>
where
  K: ?Sized + Ord,
{
  #[inline]
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    self
      .data
      .cmp(&other.data)
      .then_with(|| other.meta.version().cmp(&self.meta.version())) // make sure latest version at the front
  }
}

impl<K> core::fmt::Debug for GenericKey<K>
where
  K: ?Sized + core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("GenericKey")
      .field("meta", &self.meta)
      .field("data", &&self.data)
      .finish()
  }
}

impl<K> Type for GenericKey<K>
where
  K: ?Sized + Type,
{
  type Ref<'a> = GenericKeyRef<K::Ref<'a>>;

  type Error = K::Error;

  #[inline]
  fn encoded_len(&self) -> usize {
    K::encoded_len(&self.data) + Meta::SIZE
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    let len = K::encode(&self.data, buf)?;
    buf[len..len + Meta::VERSION_SIZE].copy_from_slice(&self.meta.raw().to_le_bytes());
    buf[len + Meta::VERSION_SIZE..len + Meta::SIZE]
      .copy_from_slice(&self.meta.expire_at().to_le_bytes());
    Ok(len + Meta::SIZE)
  }
}
