use super::*;

#[derive(Clone, Copy)]
pub struct LogFileIterator<'a, C, Q: ?Sized = &'static [u8], R = core::ops::RangeFull> {
  pub(super) iter: skl::map::MapIterator<'a, Meta, C, Q, R>,
  pub(super) all_versions: bool,
  // Indicates if it is possible to yield items.
  pub(super) yield_: bool,
}

impl<'a, C: Comparator, Q, R> Iterator for LogFileIterator<'a, C, Q, R>
where
  &'a [u8]: PartialOrd<Q>,
  Q: ?Sized + PartialOrd<&'a [u8]>,
  R: RangeBounds<Q>,
{
  type Item = EntryRef<'a, C>;

  fn next(&mut self) -> Option<Self::Item> {
    if !self.yield_ {
      return None;
    }

    if self.all_versions {
      return self.iter.next().map(EntryRef::new);
    }

    loop {
      match self.iter.next() {
        Some(ent) if !ent.trailer().is_removed() => return Some(EntryRef::new(ent)),
        None => return None,
        _ => {}
      }
    }
  }
}

impl<'a, C: Comparator, Q, R> DoubleEndedIterator for LogFileIterator<'a, C, Q, R>
where
  &'a [u8]: PartialOrd<Q>,
  Q: ?Sized + PartialOrd<&'a [u8]>,
  R: RangeBounds<Q>,
{
  fn next_back(&mut self) -> Option<EntryRef<'a, C>> {
    if !self.yield_ {
      return None;
    }

    if self.all_versions {
      return self.iter.next_back().map(EntryRef::new);
    }

    loop {
      match self.iter.next_back() {
        Some(ent) if !ent.trailer().is_removed() => return Some(EntryRef::new(ent)),
        None => return None,
        _ => {}
      }
    }
  }
}

impl<'a, C, Q, R> LogFileIterator<'a, C, Q, R> {
  /// Returns the entry at the current position of the iterator.
  #[inline]
  pub fn entry(&self) -> Option<EntryRef<'a, C>> {
    if !self.yield_ {
      return None;
    }

    self.iter.entry().map(|e| EntryRef::new(*e))
  }

  /// Returns the bounds of the iterator.
  #[inline]
  pub fn bounds(&self) -> &R {
    self.iter.bounds()
  }
}

impl<'a, C: Comparator, Q, R> LogFileIterator<'a, C, Q, R>
where
  &'a [u8]: PartialOrd<Q>,
  Q: ?Sized + PartialOrd<&'a [u8]>,
  R: RangeBounds<Q>,
{
  /// Moves the iterator to the highest element whose key is below the given bound.
  /// If no such element is found then `None` is returned.
  pub fn seek_upper_bound(&mut self, upper: Bound<&[u8]>) -> Option<EntryRef<'a, C>> {
    if !self.yield_ {
      return None;
    }

    if self.all_versions {
      return self.iter.seek_upper_bound(upper).map(EntryRef::new);
    }

    match self.iter.seek_upper_bound(upper) {
      Some(ent) if !ent.trailer().is_removed() => {
        return Some(EntryRef::new(ent));
      }
      None => None,
      _ => loop {
        match self.iter.next_back() {
          Some(ent) if !ent.trailer().is_removed() => return Some(EntryRef::new(ent)),
          None => return None,
          _ => {}
        }
      },
    }
  }

  /// Moves the iterator to the highest element whose key is below the given bound.
  /// If no such element is found then `None` is returned.
  pub fn seek_lower_bound(&mut self, lower: Bound<&[u8]>) -> Option<EntryRef<'a, C>> {
    if !self.yield_ {
      return None;
    }

    if self.all_versions {
      return self.iter.seek_lower_bound(lower).map(EntryRef::new);
    }

    match self.iter.seek_lower_bound(lower) {
      Some(ent) if !ent.trailer().is_removed() => {
        return Some(EntryRef::new(ent));
      }
      None => None,
      _ => loop {
        match self.iter.next() {
          Some(ent) if !ent.trailer().is_removed() => return Some(EntryRef::new(ent)),
          None => return None,
          _ => {}
        }
      },
    }
  }
}
