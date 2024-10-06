use core::mem;

use crate::types::{fid::Fid, table_id::TableId, table_name::TableName};

use super::ManifestRecordError;

use aol::{CustomFlags, Entry, EntryFlags};
use dbutils::{
  buffer::VacantBuffer,
  error::{IncompleteBuffer, InsufficientBuffer},
};
use derive_more::{AsRef, From, Into};
use smol_str::SmolStr;

/// Unknown manifest event.
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("unknown manifest record type: {0}")]
pub struct UnknownManifestRecordType(pub(crate) u8);

/// The manifest record.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ManifestRecord {
  /// Log record.
  Log {
    /// File ID.
    fid: Fid,
    /// Table ID.
    tid: TableId,
  },
  /// Table record.
  Table {
    /// Table ID.
    id: TableId,
    /// Table name.
    name: TableName,
  },
}

impl ManifestRecord {
  /// Creates a new log record.
  #[inline]
  pub const fn log(fid: Fid, tid: TableId) -> Self {
    Self::Log { fid, tid }
  }

  /// Creates a new table record.
  #[inline]
  pub const fn table(table_id: TableId, name: TableName) -> Self {
    Self::Table { id: table_id, name }
  }
}

impl aol::Record for ManifestRecord {
  type Error = ManifestRecordError;

  fn encoded_size(&self) -> usize {
    match self {
      Self::Log { fid, tid, .. } => 1 + fid.encoded_len() + tid.encoded_len(),
      Self::Table { id, name } => 1 + id.encoded_len() + mem::size_of::<u8>() + name.len(),
    }
  }

  fn encode(&self, buf: &mut VacantBuffer<'_>) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_size();
    let cap = buf.capacity();
    if cap < encoded_len {
      return Err(InsufficientBuffer::with_information(encoded_len, cap).into());
    }

    match self {
      Self::Log { fid, tid } => {
        let mut cur = 0;
        buf.put_u8(0)?;
        cur += 1;
        cur += fid.encode_to_buffer(buf)?;
        cur += tid.encode_to_buffer(buf)?;
        Ok(cur)
      }
      Self::Table { id, name } => {
        let mut cur = 0;
        buf.put_u8(1)?;
        cur += 1;
        cur += id.encode_to_buffer(buf)?;

        let remaining = buf.remaining();
        let want = 1 + name.len();
        if want > remaining {
          return Err(InsufficientBuffer::with_information(cur + want, cur + remaining).into());
        }

        buf.put_u8(name.len() as u8)?;
        cur += 1;
        buf.put_slice(name.as_bytes())?;
        cur += name.len();
        Ok(cur)
      }
    }
  }

  fn decode(buf: &[u8]) -> Result<(usize, Self), Self::Error> {
    if buf.is_empty() {
      return Err(IncompleteBuffer::new().into());
    }

    let kind = buf[0];
    let mut cur = 1;
    Ok(match kind {
      0 => {
        let (n, fid) = Fid::decode(&buf[cur..])?;
        cur += n;
        let (n, tid) = TableId::decode(&buf[cur..])?;
        cur += n;

        (cur, Self::Log { fid, tid })
      }
      1 => {
        let (n, id) = TableId::decode(&buf[cur..])?;

        cur += n;
        let len = buf[cur] as usize;
        cur += 1;
        if buf.len() < cur + len {
          return Err(IncompleteBuffer::with_information(cur + len, buf.len()).into());
        }

        let name = SmolStr::from(core::str::from_utf8(&buf[cur..cur + len])?);
        cur += len;
        (
          cur,
          Self::Table {
            id,
            name: name.into(),
          },
        )
      }
      _ => {
        return Err(Self::Error::UnknownRecordType(UnknownManifestRecordType(
          kind,
        )))
      }
    })
  }
}

/// - The first bit of the manifest entry indicating it is a creation event or a deletion event.
///   - `0`: Creation event.
///   - `1`: Deletion event.
/// - The second bit of the manifest entry indicating it is a table event or not.
///   - `1`: Table event.
/// - The third bit of the manifest entry indicating it is a active log event or not.
///   - `1`: Active log event.
/// - The fourth bit of the manifest entry indicating it is a frozen log event or not.
///   - `1`: Frozen log event.
/// - The fifth bit of the manifest entry indicating it is a bloomfilter or not.
///   - `1`: Bloomfilter.
/// - The sixth bit of the manifest entry indicating it is a value log event or not.
///   - `1`: Value log event.
/// - The seventh and eighth bits are reserved for future use.
#[derive(Debug, Clone, Copy, From, Into, PartialEq, Eq, Hash)]
pub struct ManifestEntryFlags(EntryFlags);

impl core::fmt::Display for ManifestEntryFlags {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match () {
      () if self.is_creation() && self.is_active_log() => write!(f, "create_active_log"),
      () if self.is_creation() && self.is_frozen_log() => write!(f, "create_frozen_log"),
      () if self.is_creation() && self.is_bloomfilter() => write!(f, "create_bloomfilter"),
      () if self.is_creation() && self.is_value_log() => write!(f, "create_value_log"),
      () if self.is_creation() && self.is_table() => write!(f, "create_table"),
      () if self.is_deletion() && self.is_active_log() => write!(f, "delete_active_log"),
      () if self.is_deletion() && self.is_frozen_log() => write!(f, "delete_frozen_log"),
      () if self.is_deletion() && self.is_bloomfilter() => write!(f, "delete_bloomfilter"),
      () if self.is_deletion() && self.is_value_log() => write!(f, "delete_value_log"),
      () if self.is_deletion() && self.is_table() => write!(f, "delete_table"),
      _ => unreachable!(),
    }
  }
}

macro_rules! manifest_entry_flags_constructors {
  ($($idx:literal: $name:ident $($log:ident)?), +$(,)?) => {
    $(
      paste::paste! {
        #[doc = "Returns a flag indicating it is a creation event for " $name $(" " $log)? "."]
        #[inline]
        pub const fn [< create_ $name $(_$log)? >]() -> Self {
          Self(EntryFlags::creation_with_custom_flag(CustomFlags::empty().[< with_bit $idx>]()))
        }

        #[doc = "Returns a flag indicating it is a deletion event for " $name $(" " $log)? "."]
        #[inline]
        pub const fn [< delete_ $name $(_$log)? >]() -> Self {
          Self(EntryFlags::deletion_with_custom_flag(CustomFlags::empty().[< with_bit $idx>]()))
        }

        /// Returns `true` if the flag is a table event.
        #[doc = "Returns `true` if the flag is a " $name $(" " $log)? " event."]
        #[inline]
        pub const fn [< is_ $name $(_$log)? >](&self) -> bool {
          self.0.custom_flag().[< bit $idx >]()
        }
      }
    )*
  };
}

macro_rules! possible_manifest_entry_flags {
  ($($name:ident $($log:ident)?), +$(,)?) => {
    paste::paste! {
      {
        &[
          $(
            Self::[< create_ $name $(_ $log)?>]().bits(),
            Self::[< delete_ $name $(_ $log)?>]().bits(),
          )*
        ]
      }
    }
  };
}

impl ManifestEntryFlags {
  manifest_entry_flags_constructors!(
    1: table,
    2: active log,
    3: frozen log,
    4: bloomfilter,
    5: value log
  );

  /// Returns `true` if the flag is a creation event.
  #[inline]
  pub const fn is_creation(&self) -> bool {
    self.0.is_creation()
  }

  /// Returns `true` if the flag is a deletion event.
  #[inline]
  pub const fn is_deletion(&self) -> bool {
    self.0.is_deletion()
  }

  /// Returns the flag in the form of a `u8`.
  #[inline]
  pub const fn bits(&self) -> u8 {
    self.0.bits()
  }

  const POSSIBLE_FLAGS: &[u8] = possible_manifest_entry_flags!(
    table,
    active log,
    frozen log,
    bloomfilter,
    value log
  );

  #[inline]
  pub(super) fn is_possible_flag(bits: u8) -> bool {
    Self::POSSIBLE_FLAGS.contains(&bits)
  }
}

/// An entry in the manifest log.
#[derive(Debug, From, Into, AsRef)]
pub struct ManifestEntry(Entry<ManifestRecord>);

macro_rules! manifest_entry_constructors {
  ($($name: ident $($log:ident)?), +$(,)?) => {
    $(
      paste::paste! {
        #[doc = "Returns an instance which indicates a creation event for " $name $(" " $log)? "."]
        ///
        /// ## Example
        ///
        /// ```rust
        /// use lemondb_core::manifest::ManifestEntry;
        ///
        #[doc = "let entry = ManifestEntry::create_" $name $("_" $log)? "(Default::default(), Default::default());"]
        /// assert!(entry.flag().is_creation());
        /// ```
        #[inline]
        pub const fn [< create_ $name _log>](fid: Fid, tid: TableId) -> Self {
          Self(Entry::with_flags(ManifestEntryFlags::[< create_ $name $(_ $log)?>]().0, ManifestRecord::log(fid, tid)))
        }

        #[doc = "Returns an instance which indicates a deletion event for " $name $(" " $log)? "."]
        ///
        /// ## Example
        ///
        /// ```rust
        /// use lemondb_core::manifest::ManifestEntry;
        ///
        #[doc = "let entry = ManifestEntry::delete_" $name $("_" $log)? "(Default::default(), Default::default());"]
        /// assert!(entry.flag().is_deletion());
        /// ```
        #[inline]
        pub const fn [< delete_ $name _log>](fid: Fid, tid: TableId) -> Self {
          Self(Entry::with_flags(ManifestEntryFlags::[< delete_ $name $(_ $log)?>]().0, ManifestRecord::log(fid, tid)))
        }
      }
    )*
  };
}

impl ManifestEntry {
  /// Returns the flags of the entry.
  #[inline]
  pub const fn flag(&self) -> ManifestEntryFlags {
    ManifestEntryFlags(self.0.flag())
  }

  manifest_entry_constructors!(
    table,
    active log,
    frozen log,
    bloomfilter,
    value log,
  );
}
