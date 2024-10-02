use core::{cmp, marker::PhantomData};

use super::meta::Meta;

/// An internal generic key for querying.
pub struct Query<'a, Q: ?Sized, K: ?Sized> {
  pub(super) meta: Meta,
  pub(super) key: &'a Q,
  _phantom: PhantomData<K>,
}

impl<'a, Q: ?Sized, K: ?Sized> Query<'a, Q, K> {
  /// Creates a new `Query`.
  #[inline]
  pub fn new(meta: Meta, key: &'a Q) -> Self {
    Self {
      meta,
      key,
      _phantom: PhantomData,
    }
  }
}

impl<'a, Q, K> PartialEq for Query<'a, Q, K>
where
  Q: ?Sized + PartialEq,
  K: ?Sized,
{
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.meta.raw() == other.meta.raw() && self.key.eq(other.key)
  }
}

impl<'a, Q, K> Eq for Query<'a, Q, K>
where
  Q: ?Sized + Eq,
  K: ?Sized,
{
}

impl<'a, Q, K> PartialOrd for Query<'a, Q, K>
where
  K: ?Sized,
  Q: ?Sized + Ord,
{
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<'a, Q, K> Ord for Query<'a, Q, K>
where
  K: ?Sized,
  Q: ?Sized + Ord,
{
  #[inline]
  fn cmp(&self, other: &Self) -> cmp::Ordering {
    self
      .key
      .cmp(other.key)
      .then_with(|| self.meta.version().cmp(&other.meta.version()))
  }
}
