use core::{marker::PhantomData, ops::{Bound, RangeBounds}};

use dbutils::traits::{Type, TypeRef, KeyRef};
use skl::{full::{sync::SkipMap, FullMap}, Comparator};

use crate::types::meta::Meta;

struct GenericComparator<K: ?Sized> {
  _k: PhantomData<K>,
}

impl<K: ?Sized> core::fmt::Debug for GenericComparator<K> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("GenericComparator")
      .finish()
  }
}

impl<K> Comparator for GenericComparator<K>
where
  K: ?Sized + Type,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
{
  fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
    unsafe { <K::Ref<'_> as KeyRef<'_, K>>::compare_binary(a, b) }
  }

  fn contains(&self, start_bound: Bound<&[u8]>, end_bound: Bound<&[u8]>, key: &[u8]) -> bool {
    unsafe {
      let start = start_bound.map(|b| <K::Ref<'_> as TypeRef<'_>>::from_slice(b));
      let end = end_bound.map(|b| <K::Ref<'_> as TypeRef<'_>>::from_slice(b));
      let key = <K::Ref<'_> as TypeRef<'_>>::from_slice(key);
      
      (start, end).contains(&key)
    } 
  }
}

/// A writer for writing a frozen log file.
pub struct ImmutableLogFileWriter<K: ?Sized> {
  map: SkipMap<Meta, GenericComparator<K>>,
}


/// A frozen log file.
pub struct ImmutableLogFile<K: ?Sized> {
  map: SkipMap<Meta, GenericComparator<K>>,
}

impl<K> ImmutableLogFile<K>
where
  K: ?Sized,
{
  /// Returns `true` if the frozne log contains the version.
  pub fn contains_version(&self, version: u64) -> bool {
    todo!()
  }
}

impl<K> ImmutableLogFile<K>
where
  K: ?Sized + Type,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
{
  
}