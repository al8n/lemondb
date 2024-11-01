use core::cmp;

use dbutils::{buffer::VacantBuffer, error::InsufficientBuffer, types::Type};

use super::{active_meta::ActiveMeta, key_ref::KeyRef};

/// Encodes an error that occurred during encoding [`Key`].
pub enum EncodeError<K: ?Sized + Type> {
  /// The actual encoding error of `K`.
  Key(K::Error),
  /// The buffer is insufficient to encode the [`Key`].
  InsufficientBuffer(InsufficientBuffer),
}

/// An internal generic key.
pub struct Key<K: ?Sized> {
  pub(super) meta: ActiveMeta,
  pub(super) data: K,
}

impl<K> PartialEq for Key<K>
where
  K: ?Sized + PartialEq,
{
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.data.eq(&other.data)
  }
}

impl<K> Eq for Key<K> where K: ?Sized + Eq {}

impl<K> PartialOrd for Key<K>
where
  K: ?Sized + Ord,
{
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<K> Ord for Key<K>
where
  K: ?Sized + Ord,
{
  #[inline]
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    self.data.cmp(&other.data)
  }
}

impl<K> core::fmt::Debug for Key<K>
where
  K: ?Sized + core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Key")
      .field("meta", &self.meta)
      .field("data", &&self.data)
      .finish()
  }
}

impl<K> Type for Key<K>
where
  K: ?Sized + Type,
{
  type Ref<'a> = KeyRef<'a, K>;

  type Error = EncodeError<K>;

  #[inline]
  fn encoded_len(&self) -> usize {
    K::encoded_len(&self.data) + ActiveMeta::SIZE
  }

  #[inline]
  fn encode_to_buffer(&self, buf: &mut VacantBuffer<'_>) -> Result<usize, Self::Error> {
    buf
      .put_slice(&self.meta.encode())
      .map_err(EncodeError::InsufficientBuffer)
      .and_then(|_| {
        self
          .data
          .encode_to_buffer(buf)
          .map(|klen| klen + ActiveMeta::SIZE)
          .map_err(EncodeError::Key)
      })
  }
}
