use super::*;

use crate::{
  error::Error,
  manifest::{ManifestFile, ManifestRecord},
  options::{Options, TableOptions, WalOptions},
  wal::Wal,
  AtomicFid, Mu, TableId,
};

use core::cell::UnsafeCell;
#[cfg(not(feature = "parking_lot"))]
use std::sync::Mutex;

#[cfg(feature = "std")]
use std::collections::HashMap;

use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender};
#[cfg(not(feature = "std"))]
use hashbrown::HashMap;

use aol::CustomFlags;
#[cfg(feature = "parking_lot")]
use parking_lot::Mutex;
use smol_str::SmolStr;

struct TableInner<C = Ascend> {
  name: SmolStr,
  id: TableId,
  wal: Arc<UnsafeCell<Wal<C>>>,
  write_tx: Sender<Event>,
}

// Safety: TableInner is Sync and Send because it can only be written by one thread.
unsafe impl<C> Send for TableInner<C> {}
unsafe impl<C> Sync for TableInner<C> {}

struct TableWriter<C = Ascend> {
  name: SmolStr,
  id: TableId,
  rx: Receiver<Event>,
  wal: Arc<UnsafeCell<Wal<C>>>,
}

impl<C: Comparator + Send + Sync + 'static> TableWriter<C> {
  #[inline]
  const fn new(
    name: SmolStr,
    id: TableId,
    rx: Receiver<Event>,
    wal: Arc<UnsafeCell<Wal<C>>>,
  ) -> Self {
    Self { name, id, rx, wal }
  }

  fn run(self) {
    let Self { name, id, rx, wal } = self;

    loop {
      crossbeam_channel::select! {
        recv(rx) -> msg => {
          match msg {
            Ok(Event::Write { key, value, tx, table_id }) => {
              assert_eq!(id, table_id, "table({id})'s writer receive a write event of table({table_id}), please report this bug to https://github.com/al8n/lemondb/issues");

              // Safety: we are the only thread that writes the wal.
              let wal = unsafe { &mut *wal.get() };

              if let Err(_e) = tx.send(wal.insert(id, 0, &key, &value)) {
                #[cfg(feature = "tracing")]
                tracing::error!(table_id=%id, table_name=%name, err=%_e, "failed to send write result");
              }
            }
            Ok(Event::WriteBatch { table_id, batch, tx }) => {
              assert_eq!(id, table_id, "table({id})'s writer receive a write event of table({table_id}), please report this bug to https://github.com/al8n/lemondb/issues");
            }
            Ok(Event::Remove { table_id, key, tx }) => {
              assert_eq!(id, table_id, "table({id})'s writer receive a write event of table({table_id}), please report this bug to https://github.com/al8n/lemondb/issues");

              // Safety: we are the only thread that writes the wal.
              let wal = unsafe { &mut *wal.get() };

              if let Err(_e) = tx.send(wal.remove(id, 0, &key)) {
                #[cfg(feature = "tracing")]
                tracing::error!(table_id=%id, table_name=%name, err=%_e, "failed to send remove result");
              }
            }
            Err(_) => break,
          }
        }
      }
    }
  }
}

// Safety: TableInner is Sync and Send because it can only be written by one thread.
unsafe impl<C> Send for TableWriter<C> {}
unsafe impl<C> Sync for TableWriter<C> {}

/// Table
pub struct Table<C = Ascend> {
  inner: Arc<TableInner<C>>,
}

impl<C> Clone for Table<C> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<C: Comparator + Send + Sync + 'static> Table<C> {
  /// Insert a key-value pair into the table.
  pub fn insert(&self, key: Bytes, value: Bytes) -> Result<(), Error> {
    self.insert_in(key, value)
  }

  fn insert_in(&self, key: Bytes, value: Bytes) -> Result<(), Error> {
    let (tx, rx) = oneshot::channel();
    if let Err(_e) = self.inner.write_tx.send(Event::Write {
      table_id: self.inner.id,
      key,
      value,
      tx,
    }) {
      #[cfg(feature = "tracing")]
      tracing::error!(table_id=%self.inner.id, table=%self.inner.name, err=%_e);
    }

    match rx.recv() {
      Ok(res) => res,
      Err(_) => Err(Error::TableClosed(self.inner.name.clone())),
    }
  }

  fn bootstrap(
    name: SmolStr,
    id: TableId,
    wal: Wal<C>,
    write_ch: Option<Sender<Event>>,
  ) -> Result<Self, Error> {
    let wal = Arc::new(UnsafeCell::new(wal));
    match write_ch {
      Some(tx) => Ok(Self {
        inner: Arc::new(TableInner {
          name: name.clone(),
          id,
          wal: wal.clone(),
          write_tx: tx,
        }),
      }),
      // run table in standalone mode
      None => {
        let (tx, rx) = crossbeam_channel::bounded(100);
        let table = Self {
          inner: Arc::new(TableInner {
            name: name.clone(),
            id,
            wal: wal.clone(),
            write_tx: tx,
          }),
        };

        let writer = TableWriter::new(name, id, rx, wal);
        std::thread::spawn(move || writer.run());
        Ok(table)
      }
    }
  }
}

enum Event {
  Write {
    table_id: TableId,
    key: Bytes,
    value: Bytes,
    tx: oneshot::Sender<Result<(), Error>>,
  },
  WriteBatch {
    table_id: TableId,
    batch: Vec<(Bytes, Bytes)>,
    tx: oneshot::Sender<Result<(), Error>>,
  },
  Remove {
    table_id: TableId,
    key: Bytes,
    tx: oneshot::Sender<Result<(), Error>>,
  },
}

/// Database
pub struct Db<C = Ascend> {
  fid_generator: Arc<AtomicFid>,
  manifest: Arc<Mutex<ManifestFile>>,
  tables: Mutex<HashMap<TableId, Table<C>>>,
  default_wal: Wal<C>,
  main_write_tx: Sender<Event>,
  main_write_rx: Receiver<Event>,
  cmp: Arc<C>,
  opts: Options,
}

impl Db {
  /// Get a table with the given name. If this method returns `None`, then it means that the table either does not exist or has not been opened.
  ///
  /// See also [`open_table`](#method.open_table) and [`get_or_open_table`](#method.get_or_open_table).
  #[inline]
  pub fn get_table(&self, name: &str) -> Option<Table> {
    let tables = self.tables.lock_me();
    tables.values().find(|t| t.inner.name == name).cloned()
  }

  /// Get a table with the given name and options, if the table does not exist, then it will open the table according to the [`TableOptions`].
  ///
  /// See also [`open_table`](#method.open_table) and [`get_table`](#method.get_table).
  #[inline]
  pub fn get_or_open_table<N: Into<SmolStr>>(
    &self,
    name: N,
    opts: TableOptions,
  ) -> Result<Table, Error> {
    let name: SmolStr = name.into();
    match self.get_table(name.as_str()) {
      Some(t) => Ok(t),
      None => self.open_table(name, opts),
    }
  }

  /// Open a table with the given name and options.
  ///
  /// If the table already be opened, then an error will be returned.
  ///
  /// See also [`get_table`](#method.get_table) and [`get_or_open_table`](#method.get_or_open_table).
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
        if tables.get(&table_manifest.id).is_some() {
          return Err(Error::TableAlreadyOpened(name));
        }

        let wal = Wal::open(
          table_manifest,
          self.fid_generator.clone(),
          self.manifest.clone(),
          self.cmp.clone(),
          opts.to_wal_options(self.opts.in_memory),
        )?;

        let t = if opts.standalone {
          Table::bootstrap(name, table_manifest.id, wal, None)?
        } else {
          Table::bootstrap(
            name,
            table_manifest.id,
            wal,
            Some(self.main_write_tx.clone()),
          )?
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

        let table_id = file.next_table_id();
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
            ManifestRecord::table(table_id, name.clone())
          ),
          aol::Entry::creation(ManifestRecord::log(next_fid, table_id)),
        ])?; // TODO: cleanup error construct

        let t = if opts.standalone {
          Table::bootstrap(name, table_id, wal, None)?
        } else {
          Table::bootstrap(name, table_id, wal, Some(self.main_write_tx.clone()))?
        };
        let mut tables = self.tables.lock_me();
        tables.insert(table_id, t.clone());

        Ok(t)
      }
    }
  }
}
