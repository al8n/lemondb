use super::*;

use crate::{
  error::Error,
  manifest::{ManifestFile, ManifestRecord},
  options::{MemoryMode, Options, TableOptions, WalOptions},
  wal::Wal,
  AtomicFid, Meta, Mu, TableId,
};

use core::{
  cell::UnsafeCell,
  sync::atomic::{AtomicBool, Ordering},
};
#[cfg(not(feature = "parking_lot"))]
use std::sync::Mutex;

#[cfg(feature = "std")]
use std::collections::HashMap;

use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender};
use either::Either;
#[cfg(not(feature = "std"))]
use hashbrown::HashMap;

use aol::CustomFlags;
#[cfg(feature = "parking_lot")]
use parking_lot::Mutex;
use smol_str::SmolStr;

struct StandaloneTableWriter<C = Ascend> {
  name: SmolStr,
  id: TableId,
  rx: Receiver<Event>,
  wal: Arc<UnsafeCell<Wal<C>>>,
  ignore_writes_after_close: bool,
  remove_table_rx: Receiver<()>,
  close_table_rx: Receiver<()>,
  shutdown_db_rx: Receiver<()>,
}

impl<C: Comparator + Send + Sync + 'static> StandaloneTableWriter<C> {
  #[inline]
  const fn new(
    name: SmolStr,
    id: TableId,
    rx: Receiver<Event>,
    wal: Arc<UnsafeCell<Wal<C>>>,
    ignore_writes_after_close: bool,
    remove_table_rx: Receiver<()>,
    close_table_rx: Receiver<()>,
    shutdown_db_rx: Receiver<()>,
  ) -> Self {
    Self {
      name,
      id,
      rx,
      wal,
      ignore_writes_after_close,
      shutdown_db_rx,
      remove_table_rx,
      close_table_rx,
    }
  }

  fn run(self) {
    macro_rules! drain_ignore_onflight {
      ($this:ident -> $err:expr) => {{
        for msg in $this.rx {
          match msg {
            Event::Write { tx, .. } => {
              if let Err(_e) = tx.send(Err($err)) {
                #[cfg(feature = "tracing")]
                tracing::error!(table_id=%$this.id, table_name=%$this.name, err=%_e, "failed to send write result");
              }
            }
            Event::WriteBatch { tx, .. } => {
              if let Err(_e) = tx.send(Err($err)) {
                #[cfg(feature = "tracing")]
                tracing::error!(table_id=%$this.id, table_name=%$this.name, err=%_e, "failed to send write result");
              }
            }
            Event::Remove { tx, .. } => {
              if let Err(_e) = tx.send(Err($err)) {
                #[cfg(feature = "tracing")]
                tracing::error!(table_id=%$this.id, table_name=%$this.name, err=%_e, "failed to send write result");
              }
            }
          }
        }

        return;
      }};
    }

    macro_rules! drain_onflight {
      ($this:ident -> $err:expr) => {{
        for msg in $this.rx {
          match msg {
            Event::Write { tx, table_id, key, value } => {
              assert_eq!($this.id, table_id, "table({})'s writer receive a write event of table({table_id}), please report this bug to https://github.com/al8n/lemondb/issues", $this.id);

              // Safety: we are the only thread that writes the wal.
              let wal = unsafe { &mut *$this.wal.get() };

              if let Err(_e) = tx.send(wal.insert($this.id, 0, &key, &value)) {
                #[cfg(feature = "tracing")]
                tracing::error!(table_id=%$this.id, table_name=%$this.name, err=%_e, "failed to send write result");
              }
            }
            Event::WriteBatch { tx, table_id, batch } => {
              assert_eq!($this.id, table_id, "table({})'s writer receive a write event of table({table_id}), please report this bug to https://github.com/al8n/lemondb/issues", $this.id);
            }
            Event::Remove { tx, table_id, key } => {
              assert_eq!($this.id, table_id, "table({})'s writer receive a write event of table({table_id}), please report this bug to https://github.com/al8n/lemondb/issues", $this.id);

              // Safety: we are the only thread that writes the wal.
              let wal = unsafe { &mut *$this.wal.get() };

              if let Err(_e) = tx.send(wal.remove($this.id, 0, &key)) {
                #[cfg(feature = "tracing")]
                tracing::error!(table_id=%$this.id, table_name=%$this.name, err=%_e, "failed to send remove result");
              }
            }
          }
        }

        return;
      }};
    }

    let id = self.id;
    loop {
      crossbeam_channel::select_biased! {
        recv(self.remove_table_rx) -> _ => drain_ignore_onflight!(self -> Error::TableRemoved(self.name.clone())),
        recv(self.close_table_rx) -> _ => if self.ignore_writes_after_close {
          drain_ignore_onflight!(self -> Error::TableClosed(self.name.clone()))
        } else {
          drain_onflight!(self -> Error::TableClosed(self.name.clone()))
        },
        recv(self.shutdown_db_rx) -> _ => if self.ignore_writes_after_close {
          drain_ignore_onflight!(self -> Error::DatabaseClosed)
        } else {
          drain_onflight!(self -> Error::DatabaseClosed)
        },
        recv(self.rx) -> msg => {
          match msg {
            Ok(Event::Write { key, value, tx, table_id }) => {
              assert_eq!(id, table_id, "table({id})'s writer receive a write event of table({table_id}), please report this bug to https://github.com/al8n/lemondb/issues");

              // Safety: we are the only thread that writes the wal.
              let wal = unsafe { &mut *self.wal.get() };

              if let Err(_e) = tx.send(wal.insert(id, 0, &key, &value)) {
                #[cfg(feature = "tracing")]
                tracing::error!(table_id=%id, table_name=%self.name, err=%_e, "failed to send write result");
              }
            }
            Ok(Event::WriteBatch { table_id, batch, tx }) => {
              assert_eq!(id, table_id, "table({id})'s writer receive a write event of table({table_id}), please report this bug to https://github.com/al8n/lemondb/issues");
            }
            Ok(Event::Remove { table_id, key, tx }) => {
              assert_eq!(id, table_id, "table({id})'s writer receive a write event of table({table_id}), please report this bug to https://github.com/al8n/lemondb/issues");

              // Safety: we are the only thread that writes the wal.
              let wal = unsafe { &mut *self.wal.get() };

              if let Err(_e) = tx.send(wal.remove(id, 0, &key)) {
                #[cfg(feature = "tracing")]
                tracing::error!(table_id=%id, table_name=%self.name, err=%_e, "failed to send remove result");
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
unsafe impl<C> Send for StandaloneTableWriter<C> {}
unsafe impl<C> Sync for StandaloneTableWriter<C> {}

struct TableInner<C = Ascend> {
  name: SmolStr,
  id: TableId,
  wal: Arc<UnsafeCell<Wal<C>>>,
  write_tx: Sender<Event>,
  manifest: Arc<Mutex<ManifestFile>>,
  closed: AtomicBool,
  removed: AtomicBool,
  close_table_tx: Sender<()>,
  remove_table_tx: Sender<()>,
}

// Safety: TableInner is Sync and Send because it can only be written by one thread.
unsafe impl<C> Send for TableInner<C> {}
unsafe impl<C> Sync for TableInner<C> {}

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
  /// Returns `true` if the table contains the specified key.
  pub fn contains(&self, key: &[u8]) -> Result<bool, Error> {
    self.check_status()?;
    let wal = unsafe { &*self.inner.wal.get() };
    wal.contains(0, key)
  }

  /// Get the value of the key.
  pub fn get(&self, key: &[u8]) -> Result<Option<crate::types::Entry>, Error> {
    self.check_status()?;

    let wal = unsafe { &*self.inner.wal.get() };

    wal.get(0, key)
  }

  /// Insert a key-value pair into the table.
  #[inline]
  pub fn insert(&self, key: Bytes, value: Bytes) -> Result<(), Error> {
    self.insert_in(key, value)
  }

  /// Remove a key from the table.
  pub fn remove(&self, key: Bytes) -> Result<(), Error> {
    self.check_status()?;

    let (tx, rx) = oneshot::channel();
    if let Err(_e) = self.inner.write_tx.send(Event::Remove {
      table_id: self.inner.id,
      key,
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

  #[inline]
  fn check_status(&self) -> Result<(), Error> {
    if self.inner.closed.load(Ordering::Acquire) {
      return Err(Error::TableClosed(self.inner.name.clone()));
    }

    if self.inner.removed.load(Ordering::Acquire) {
      return Err(Error::TableRemoved(self.inner.name.clone()));
    }

    Ok(())
  }

  fn insert_in(&self, key: Bytes, value: Bytes) -> Result<(), Error> {
    self.check_status()?;

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
    manifest: Arc<Mutex<ManifestFile>>,
    wal: Wal<C>,
    write_ch: Either<Sender<Event>, usize>,
    ignore_writes_after_close: bool,
    shutdown_db_rx: Receiver<()>,
  ) -> Result<Self, Error> {
    let wal = Arc::new(UnsafeCell::new(wal));
    let (close_table_tx, close_table_rx) = crossbeam_channel::bounded(1);
    let (remove_table_tx, remove_table_rx) = crossbeam_channel::bounded(1);
    match write_ch {
      Either::Left(tx) => Ok(Self {
        inner: Arc::new(TableInner {
          name: name.clone(),
          id,
          wal: wal.clone(),
          write_tx: tx,
          closed: AtomicBool::new(false),
          removed: AtomicBool::new(false),
          manifest,
          close_table_tx,
          remove_table_tx,
        }),
      }),
      // run table in standalone mode
      Either::Right(size) => {
        let (tx, rx) = crossbeam_channel::bounded(size);
        let table = Self {
          inner: Arc::new(TableInner {
            name: name.clone(),
            id,
            wal: wal.clone(),
            write_tx: tx,
            closed: AtomicBool::new(false),
            removed: AtomicBool::new(false),
            manifest,
            close_table_tx,
            remove_table_tx,
          }),
        };

        let writer = StandaloneTableWriter::new(
          name,
          id,
          rx,
          wal,
          ignore_writes_after_close,
          remove_table_rx,
          close_table_rx,
          shutdown_db_rx,
        );
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
  in_memory: Option<MemoryMode>,
  shutdown_tx: Sender<()>,
  shutdown_rx: Receiver<()>,
}

impl Db {
  /// Open a database with the given directory and options.
  pub fn open<P>(dir: P, opts: Options) -> Result<Self, Error> {
    if let Some(mode) = opts.in_memory {}
    todo!()
  }

  /// Open a database in memory with the given options.
  pub fn open_inmemory(memory_mode: MemoryMode, opts: Options) -> Result<Self, Error> {
    todo!()
  }

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
          opts.to_wal_options(self.in_memory),
        )?;

        let t = if opts.standalone {
          Table::bootstrap(
            name,
            table_manifest.id,
            self.manifest.clone(),
            wal,
            Either::Right(opts.write_buffer_size()),
            opts.ignore_writes_after_close,
            self.shutdown_rx.clone(),
          )?
        } else {
          Table::bootstrap(
            name,
            table_manifest.id,
            self.manifest.clone(),
            wal,
            Either::Left(self.main_write_tx.clone()),
            opts.ignore_writes_after_close,
            self.shutdown_rx.clone(),
          )?
        };

        tables.insert(table_manifest.id, t.clone());

        Ok(t)
      }
      None => {
        if self.opts.read_only {
          return Err(Error::ReadOnly);
        }

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
          opts.to_wal_options(self.in_memory),
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
          Table::bootstrap(
            name,
            table_id,
            self.manifest.clone(),
            wal,
            Either::Right(opts.write_buffer_size()),
            opts.ignore_writes_after_close,
            self.shutdown_rx.clone(),
          )?
        } else {
          Table::bootstrap(
            name,
            table_id,
            self.manifest.clone(),
            wal,
            Either::Left(self.main_write_tx.clone()),
            opts.ignore_writes_after_close,
            self.shutdown_rx.clone(),
          )?
        };
        let mut tables = self.tables.lock_me();
        tables.insert(table_id, t.clone());

        Ok(t)
      }
    }
  }

  /// Remove the table from the database. Returns `Ok(true)` if this call triggers the removal of the table and successfully remove ths table.
  /// Otherwise, if this method returns `Ok(false)`, then it means that the table is already removed or is in the process of being removed by another thread.
  ///
  /// When trying to remove a table, the files of this table will not be deleted immediately, but the table will be marked as removed in the manifest file.
  /// After the table is marked as removed, the table will be closed and no more write operations can be performed on the table.
  ///
  /// It is safe to call this method multiple times.
  pub fn remove_table(&self, name: &str) -> Result<bool, Error> {
    let mut tables = self.tables.lock_me();
    let table = tables.values().find(|t| t.inner.name == name);

    let id = match table {
      None => {
        let mut manifest_file = self.manifest.lock_me();
        let manifest = manifest_file.manifest();
        match manifest.get_table(name) {
          None => return Err(Error::TableNotFound(name.into())),
          Some(table_manifest) => {
            let id = table_manifest.id;
            let name = table_manifest.name.clone();

            let mut batch =
              Vec::with_capacity(table_manifest.logs.len() + table_manifest.vlogs.len() + 1);

            table_manifest.logs.iter().for_each(|fid| {
              batch.push(aol::Entry::deletion(ManifestRecord::log(*fid, id)));
            });

            table_manifest.vlogs.iter().for_each(|fid| {
              batch.push(aol::Entry::deletion_with_custom_flags(
                CustomFlags::empty().with_bit1(),
                ManifestRecord::log(*fid, id),
              ));
            });

            batch.push(aol::Entry::deletion_with_custom_flags(
              CustomFlags::empty().with_bit2(),
              ManifestRecord::table(id, name.clone()),
            ));

            manifest_file.append_batch(batch)?;

            return Ok(true);
          }
        }
      }
      Some(t) => {
        if t.inner.removed.fetch_or(true, Ordering::AcqRel) {
          return Ok(false);
        }

        t.inner.closed.store(true, Ordering::Release);

        if let Err(_e) = t.inner.remove_table_tx.send(()) {
          #[cfg(feature = "tracing")]
          tracing::error!(table_id=%t.inner.id, table=%t.inner.name, err=%_e);
        }

        let mut manifest_file = t.inner.manifest.lock_me();
        let table_manifest = manifest_file
          .manifest()
          .get_table(t.inner.name.as_str())
          .ok_or_else(|| Error::TableNotFound(t.inner.name.clone()))?;
        let id = table_manifest.id;
        let name = table_manifest.name.clone();

        let mut batch =
          Vec::with_capacity(table_manifest.logs.len() + table_manifest.vlogs.len() + 1);

        table_manifest.logs.iter().for_each(|fid| {
          batch.push(aol::Entry::deletion(ManifestRecord::log(*fid, id)));
        });

        table_manifest.vlogs.iter().for_each(|fid| {
          batch.push(aol::Entry::deletion_with_custom_flags(
            CustomFlags::empty().with_bit1(),
            ManifestRecord::log(*fid, id),
          ));
        });

        batch.push(aol::Entry::deletion_with_custom_flags(
          CustomFlags::empty().with_bit2(),
          ManifestRecord::table(id, name.clone()),
        ));

        manifest_file.append_batch(batch)?;

        id
      }
    };

    tables.remove(&id);
    Ok(true)
  }

  /// Close the table. After the table is closed, no more write operations can be performed on the table.
  pub fn close_table(&self, id: TableId) -> Result<(), Error> {
    let mut tables = self.tables.lock_me();
    match tables.remove(&id) {
      None => Ok(()),
      Some(t) => {
        if t.inner.closed.fetch_or(true, Ordering::AcqRel) {
          return Ok(());
        }

        if let Err(_e) = t.inner.close_table_tx.send(()) {
          #[cfg(feature = "tracing")]
          tracing::error!(table_id=%t.inner.id, table=%t.inner.name, err=%_e);
        }

        Ok(())
      }
    }
  }
}
