/// The metadata of the entry in immutable log file in the database.
pub mod immutable_meta;

/// The metadata of the entry in the active log file in the database.
pub mod active_meta;

/// The file ID in the database.
pub mod fid;

/// The table name in the database.
pub mod table_name;

/// The table ID in the database.
pub mod table_id;

/// The log extension in the database.
pub mod log_extension;

/// The internal key in the database.
pub mod key;

/// The reference to an internal key in the database.
pub mod key_ref;

/// A value type, which can either be a value or a pointer.
pub mod value;

/// A pointer pointing to an entry with a large value in the value log.
pub mod pointer;

// /// The reference to an entry in the database.
// pub mod entry_ref;

// /// The entry in the database.
// pub mod entry;

// /// The generic entry in the database.
// pub mod generic_entry;

// /// The reference to a generic entry in the database.
// pub mod generic_entry_ref;

/// A type used for lookups in the database.
pub mod query;
