use super::*;

#[derive(Clone, Copy)]
pub(crate) struct LogFileAllVersionsIter<'a, C, Q: ?Sized = &'static [u8], R = core::ops::RangeFull>
{
  pub(super) iter: skl::map::AllVersionsIter<'a, Meta, Arc<C>, Q, R>,
  // Indicates if it is possible to yield items.
  pub(super) yield_: bool,
}

impl<'a, C: Comparator, Q, R> Iterator for LogFileAllVersionsIter<'a, C, Q, R>
where
  &'a [u8]: PartialOrd<Q>,
  Q: ?Sized + PartialOrd<&'a [u8]>,
  R: RangeBounds<Q>,
{
  type Item = Result<VersionedEntryRef<'a, Meta>, LogFileError>;

  fn next(&mut self) -> Option<Self::Item> {
    if !self.yield_ {
      return None;
    }

    self.iter.next().map(|ent| {
      let trailer = ent.trailer();
      validate_checksum(
        trailer.version(),
        ent.key(),
        ent.value(),
        trailer.checksum(),
      )
      .map(|_| ent)
      .map_err(Into::into)
    })
  }
}

impl<'a, C: Comparator, Q, R> DoubleEndedIterator for LogFileAllVersionsIter<'a, C, Q, R>
where
  &'a [u8]: PartialOrd<Q>,
  Q: ?Sized + PartialOrd<&'a [u8]>,
  R: RangeBounds<Q>,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    if !self.yield_ {
      return None;
    }

    self.iter.next_back().map(|ent| {
      let trailer = ent.trailer();
      validate_checksum(
        trailer.version(),
        ent.key(),
        ent.value(),
        trailer.checksum(),
      )
      .map(|_| ent)
      .map_err(Into::into)
    })
  }
}

impl<'a, C, Q, R> LogFileAllVersionsIter<'a, C, Q, R> {
  /// Returns the entry at the current position of the iterator.
  #[inline]
  pub fn entry(&self) -> Option<VersionedEntryRef<'a, Meta>> {
    if !self.yield_ {
      return None;
    }

    self.iter.entry().cloned()
  }

  /// Returns the bounds of the iterator.
  #[inline]
  pub fn bounds(&self) -> &R {
    self.iter.bounds()
  }
}

impl<'a, C: Comparator, Q, R> LogFileAllVersionsIter<'a, C, Q, R>
where
  &'a [u8]: PartialOrd<Q>,
  Q: ?Sized + PartialOrd<&'a [u8]>,
  R: RangeBounds<Q>,
{
  /// Moves the iterator to the highest element whose key is below the given bound.
  /// If no such element is found then `None` is returned.
  pub fn seek_upper_bound(
    &mut self,
    upper: Bound<&[u8]>,
  ) -> Result<Option<VersionedEntryRef<'a, Meta>>, LogFileError> {
    if !self.yield_ {
      return Ok(None);
    }

    match self.iter.seek_upper_bound(upper) {
      Some(ent) => {
        let trailer = ent.trailer();
        return validate_checksum(
          trailer.version(),
          ent.key(),
          ent.value(),
          trailer.checksum(),
        )
        .map(|_| Some(ent))
        .map_err(Into::into);
      }
      None => Ok(None),
    }
  }

  /// Moves the iterator to the highest element whose key is below the given bound.
  /// If no such element is found then `None` is returned.
  pub fn seek_lower_bound(
    &mut self,
    lower: Bound<&[u8]>,
  ) -> Result<Option<VersionedEntryRef<'a, Meta>>, LogFileError> {
    if !self.yield_ {
      return Ok(None);
    }

    match self.iter.seek_lower_bound(lower) {
      Some(ent) => {
        let trailer = ent.trailer();
        return validate_checksum(
          trailer.version(),
          ent.key(),
          ent.value(),
          trailer.checksum(),
        )
        .map(|_| Some(ent))
        .map_err(Into::into);
      }
      None => Ok(None),
    }
  }
}
