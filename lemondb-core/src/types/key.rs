use core::{cmp, marker::PhantomData};

use dbutils::{traits::Type, StaticComparator};

use super::{key_ref::KeyRef, meta::Meta};

/// An internal key.
pub struct Key<C> {
  pub(super) meta: Meta,
  _phantom: PhantomData<C>,
  pub(super) data: [u8],
}

impl<C: StaticComparator> PartialEq for Key<C> {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.meta.raw() == other.meta.raw() && self.data.eq(&other.data)
  }
}

impl<C: StaticComparator> Eq for Key<C> {}

impl<C: StaticComparator> PartialOrd for Key<C> {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<C: StaticComparator> Ord for Key<C> {
  #[inline]
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    C::compare(&self.data, &other.data).then_with(|| other.meta.version().cmp(&self.meta.version()))
    // make sure latest version at the front
  }
}

impl<C> core::fmt::Debug for Key<C> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Key")
      .field("meta", &self.meta)
      .field("data", &&self.data)
      .finish()
  }
}

impl<C> Type for Key<C> {
  type Ref<'a> = KeyRef<'a, C>;

  type Error = ();

  #[inline]
  fn encoded_len(&self) -> usize {
    self.data.len() + Meta::SIZE
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    let len = self.data.len();
    buf[..len].copy_from_slice(&self.data);
    buf[len..len + Meta::VERSION_SIZE].copy_from_slice(&self.meta.raw().to_le_bytes());
    buf[len + Meta::VERSION_SIZE..len + Meta::SIZE]
      .copy_from_slice(&self.meta.expire_at().to_le_bytes());
    Ok(len + Meta::SIZE)
  }
}
