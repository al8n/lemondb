//! The code in this mod is copied and modified base on [prost]
//!
//! [prost]: https://github.com/tokio-rs/prost/blob/master/prost/src/encoding.rs.

// use crate::error::ChecksumMismatch;

use core::iter::Iterator;
use core::result::Result;

pub(crate) trait TryMap<I: Iterator> {
  fn try_map<F, T, E>(self, f: F) -> TryMapIterator<Self, F>
  where
    F: FnMut(I::Item) -> Result<T, E>,
    Self: Sized;
}

impl<I: Iterator> TryMap<I> for I {
  fn try_map<F, T, E>(self, f: F) -> TryMapIterator<Self, F>
  where
    F: FnMut(I::Item) -> Result<T, E>,
    Self: Sized,
  {
    TryMapIterator { iter: self, f }
  }
}

pub(crate) struct TryMapIterator<I, F> {
  iter: I,
  f: F,
}

impl<I, F, T, E> Iterator for TryMapIterator<I, F>
where
  I: Iterator,
  F: FnMut(I::Item) -> Result<T, E>,
{
  type Item = Result<T, E>;

  fn next(&mut self) -> Option<Self::Item> {
    match self.iter.next() {
      Some(item) => Some((self.f)(item)),
      None => None,
    }
  }
}
