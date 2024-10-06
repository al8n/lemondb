use core::borrow::Borrow;
use std::{borrow::Cow, sync::Arc};

use derive_more::{Deref, Display, From, Into};
use smol_str::{SmolStr, SmolStrBuilder};

/// The default table name.
pub const DEFAULT_TABLE_NAME: &str = "default";

macro_rules! impl_from {
  ($($ty:ty), +$(,)?) => {
    $(
      impl From<$ty> for TableName {
        #[inline]
        fn from(s: $ty) -> Self {
          Self(SmolStr::from(s))
        }
      }
    )*
  };
}

impl_from!(
  String,
  &String,
  &str,
  Cow<'_, str>,
  Box<str>,
  Arc<str>,
  SmolStrBuilder,
);

/// A table id.
#[derive(Debug, Display, Deref, From, Into, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableName(SmolStr);

impl Default for TableName {
  #[inline]
  fn default() -> Self {
    Self::from(DEFAULT_TABLE_NAME)
  }
}

impl AsRef<str> for TableName {
  #[inline]
  fn as_ref(&self) -> &str {
    self.0.as_str()
  }
}

impl Borrow<str> for TableName {
  #[inline]
  fn borrow(&self) -> &str {
    self.0.as_str()
  }
}

impl PartialEq<str> for TableName {
  #[inline]
  fn eq(&self, other: &str) -> bool {
    self.0.as_str() == other
  }
}

#[test]
fn test_table_id_display() {
  let table_id = TableName::from("table_id");
  assert_eq!(table_id.to_string(), "table_id");
}
