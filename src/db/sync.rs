use super::*;

use crate::{
  error::Error,
  manifest::{ManifestFile, ManifestRecord},
  options::WalOptions,
  wal::Wal,
  AtomicFid, Mu, TableId,
};

#[cfg(not(feature = "parking_lot"))]
use std::sync::Mutex;

use aol::CustomFlags;
#[cfg(feature = "parking_lot")]
use parking_lot::Mutex;
use smol_str::SmolStr;

/// Table
pub struct Table<C = Ascend> {
  name: SmolStr,
  id: TableId,
  db: Arc<Db<C>>,
}

/// Database
pub struct Db<C = Ascend> {
  fid_generator: Arc<AtomicFid>,
  manifest: Arc<Mutex<ManifestFile>>,
  cmp: Arc<C>,
}

impl Db {
  /// Open a table with the given name and options.
  pub fn open_table<N: Into<SmolStr>>(&self, name: N, opts: TableOptions) -> Result<Table, Error> {
    let mut file = self.manifest.lock_me();
    let manifest = file.manifest();

    let name: SmolStr = name.into();
    match manifest.get_table(name.as_str()) {
      Some(table) => {
        if opts.create_new {
          return Err(Error::TableAlreadyExists(name));
        }

        // TODO: open table logic

        Ok(Table {})
      }
      None => {
        // if we do not have create or create_new, return error
        if !(opts.create || opts.create_new) {
          return Err(Error::TableNotFound(name));
        }

        let next_table_id = file.next_table_id();
        let next_fid = file.next_fid();

        let wal = Wal::create(
          next_fid,
          self.fid_generator.clone(),
          self.manifest.clone(),
          self.cmp.clone(),
          // TODO: cleanup options
          WalOptions::new(),
        )?;

        // add table to manifest
        file.append_batch(std::vec![
          aol::Entry::creation_with_custom_flags(
            CustomFlags::empty().with_bit2(),
            ManifestRecord::table(next_table_id, name)
          ),
          aol::Entry::creation(ManifestRecord::log(next_fid, next_table_id)),
        ])?; // TODO: cleanup error construct

        // TODO: bootstrap table tasks

        Ok(Table {})
      }
    }
  }
}
