use super::*;

use crate::{
  error::Error,
  manifest::{ManifestFile, ManifestRecord},
  options::{Options, TableOptions, WalOptions},
  wal::Wal,
  AtomicFid, Mu, TableId,
};

#[cfg(not(feature = "parking_lot"))]
use std::sync::Mutex;

#[cfg(feature = "std")]
use std::collections::HashMap;

#[cfg(not(feature = "std"))]
use hashbrown::HashMap;

use aol::CustomFlags;
#[cfg(feature = "parking_lot")]
use parking_lot::Mutex;
use smol_str::SmolStr;

/// Table
pub struct Table<C = Ascend> {
  name: SmolStr,
  id: TableId,
  wal: Wal<C>,
}

/// Database
pub struct Db<C = Ascend> {
  fid_generator: Arc<AtomicFid>,
  manifest: Arc<Mutex<ManifestFile>>,
  tables: Mutex<HashMap<TableId, Table<C>>>,
  cmp: Arc<C>,
  opts: Options,
}

impl Db {
  /// Open a table with the given name and options.
  ///
  /// If the table already be opened, then the `opts` will be ignored, and returned the opened table.
  pub fn open_table<N: Into<SmolStr>>(&self, name: N, opts: TableOptions) -> Result<Table, Error> {
    let mut file = self.manifest.lock_me();
    let manifest = file.manifest();

    let name: SmolStr = name.into();
    match manifest.get_table(name.as_str()) {
      Some(table_manifest) => {
        if opts.create_new {
          return Err(Error::TableAlreadyExists(name));
        }

        let mut tables = self.tables.lock_me();
        if let Some(t) = tables.get(&table_manifest.id) {
          return Err(Error::TableAlreadyOpened(name));
        }

        let wal = Wal::open(
          table_manifest,
          self.fid_generator.clone(),
          self.manifest.clone(),
          self.cmp.clone(),
          opts.to_wal_options(self.opts.in_memory),
        )?;
        let t = Table {
          name,
          id: table_manifest.id,
          wal,
        };
        tables.insert(table_manifest.id, t.clone());
        // TODO: bootstrap table logic

        Ok(t)
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
          opts.to_wal_options(self.opts.in_memory),
        )?;

        // add table to manifest
        file.append_batch(std::vec![
          aol::Entry::creation_with_custom_flags(
            CustomFlags::empty().with_bit2(),
            ManifestRecord::table(next_table_id, name.clone())
          ),
          aol::Entry::creation(ManifestRecord::log(next_fid, next_table_id)),
        ])?; // TODO: cleanup error construct

        let t = Table {
          name,
          id: next_table_id,
          wal,
        };
        let mut tables = self.tables.lock_me();
        // TODO: bootstrap table tasks
        tables.insert(next_table_id, t.clone());

        Ok(t)
      }
    }
  }
}
