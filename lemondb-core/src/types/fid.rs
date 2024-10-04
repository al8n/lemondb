pub use arbitrary_int::u63;

use arbitrary_int::Number;
use dbutils::CheapClone;
use derive_more::{Add, AddAssign};

/// The file ID in the database.
///
/// The maximum number of files in the database is `2^63`.
#[derive(Debug, Default, Add, AddAssign, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fid(u63);

impl core::fmt::Display for Fid {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl CheapClone for Fid {
  #[inline]
  fn cheap_clone(&self) -> Self {
    *self
  }
}

impl Fid {
  /// Creates a new file ID.
  #[inline]
  pub const fn new(id: u63) -> Self {
    Self(id)
  }
}

impl From<u63> for Fid {
  #[inline]
  fn from(id: u63) -> Self {
    Self(id)
  }
}

impl From<Fid> for u63 {
  #[inline]
  fn from(fid: Fid) -> Self {
    fid.0
  }
}

macro_rules! impl_from {
  ($($ty:ident), +$(,)?) => {
    $(
      impl From<$ty> for Fid {
        #[inline]
        fn from(id: $ty) -> Self {
          Self(id.into())
        }
      }
    )*
  };
}

impl_from!(u8, u16, u32);

impl TryFrom<u64> for Fid {
  type Error = InvalidFid;

  #[inline]
  fn try_from(id: u64) -> Result<Self, Self::Error> {
    if id > u63::MAX.into() {
      Err(InvalidFid(id))
    } else {
      Ok(Self(id.into()))
    }
  }
}

impl From<Fid> for u64 {
  #[inline]
  fn from(fid: Fid) -> Self {
    fid.0.into()
  }
}

impl From<&Fid> for u64 {
  #[inline]
  fn from(fid: &Fid) -> Self {
    fid.0.into()
  }
}

impl TryFrom<usize> for Fid {
  type Error = InvalidFid;

  #[inline]
  fn try_from(id: usize) -> Result<Self, Self::Error> {
    (id as u64).try_into()
  }
}

/// An error indicating that the file ID is invalid.
#[derive(Debug)]
pub struct InvalidFid(u64);

impl From<u64> for InvalidFid {
  #[inline]
  fn from(id: u64) -> Self {
    Self(id)
  }
}

impl core::fmt::Display for InvalidFid {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(
      f,
      "invalid file id: {}, the maximum file id is {}",
      self.0,
      u63::MAX
    )
  }
}

impl core::error::Error for InvalidFid {}
