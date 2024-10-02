use super::key_ref::KeyRef;

/// A reference to the entry in the database.
pub struct EntryRef<'a, C> {
  key: KeyRef<'a, C>,
  value: &'a [u8],
}

impl<'a, C> EntryRef<'a, C> {
  /// Creates a new entry reference.
  #[inline]
  pub const fn new(key: KeyRef<'a, C>, value: &'a [u8]) -> Self {
    Self { key, value }
  }
}
