use core::{cmp, marker::PhantomData};
use dbutils::{
  equivalent::{Comparable, Equivalent},
  types::Type,
};

use super::key_ref::KeyRef;

/// A type for flexible lookup.
#[derive(ref_cast::RefCast)]
#[repr(transparent)]
pub struct Query<'a, K, Q>
where
  K: ?Sized,
  Q: ?Sized,
{
  _k: PhantomData<&'a K>,
  key: Q,
}

impl<'a, K, Q> Equivalent<KeyRef<'a, K>> for Query<'a, K, Q>
where
  K: Type + ?Sized,
  Q: ?Sized + Equivalent<K::Ref<'a>>,
{
  #[inline]
  fn equivalent(&self, p: &KeyRef<'a, K>) -> bool {
    self.key.equivalent(p.key())
  }
}

impl<'a, K, Q> Comparable<KeyRef<'a, K>> for Query<'a, K, Q>
where
  K: Type + ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
{
  #[inline]
  fn compare(&self, p: &KeyRef<'a, K>) -> cmp::Ordering {
    self.key.compare(p.key())
  }
}
