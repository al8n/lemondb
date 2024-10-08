use aol::{CustomFlags, EntryFlags};

use super::*;

#[test]
fn test_manifest_file() {
  let dir = tempfile::tempdir().unwrap();
  let mut file = ManifestFile::open(
    Some(dir.path()),
    ManifestOptions::new().with_rewrite_threshold(1000),
  )
  .unwrap();

  let fid = Fid::from(0u32);
  let tid = TableId::new(0);

  // Test empty manifest.
  assert!(file.manifest().tables().is_empty());

  // Mock the database behavior, first create a table, when a table is created
  // a new active log and value log are also created.
  file
    .append_batch([
      ManifestEntry::create_table(tid, "foo".into()),
      ManifestEntry::create_active_log(fid, tid),
      ManifestEntry::create_value_log(fid, tid),
    ])
    .unwrap();

  {
    let manifest = file.manifest();
    assert_eq!(manifest.tables().len(), 1);

    let table = manifest.get_table("foo").unwrap();
    assert_eq!(table.id(), tid);
    assert_eq!(table.name(), "foo");
    assert!(table.active_logs().contains(&fid));
    assert!(table.value_logs().contains(&fid));
    assert!(table.frozen_logs().is_empty());
    assert!(table.bloomfilters().is_empty());
  }

  // after some time, the active log is frozen and a new active log is created
  file
    .append_batch([
      ManifestEntry::create_frozen_log(fid, tid),
      ManifestEntry::create_bloomfilter(fid, tid),
      ManifestEntry::create_active_log(fid.next(), tid),
    ])
    .unwrap();

  {
    let manifest = file.manifest();
    assert_eq!(manifest.tables().len(), 1);

    let table = manifest.get_table("foo").unwrap();
    assert_eq!(table.id(), tid);
    assert_eq!(table.name(), "foo");

    let active_logs = table.active_logs();
    assert_eq!(active_logs.len(), 2);
    assert!(active_logs.contains(&fid.next()));
    assert!(active_logs.contains(&fid));
    assert!(table.value_logs().contains(&fid));
    assert!(table.frozen_logs().contains(&fid));
    assert!(table.bloomfilters().contains(&fid));
  }

  // after some time, the last reference to the old active log is dropped, then we need to remove
  // the old active log.
  file
    .append(ManifestEntry::delete_active_log(fid, tid))
    .unwrap();

  {
    let manifest = file.manifest();
    assert_eq!(manifest.tables().len(), 1);

    let table = manifest.get_table("foo").unwrap();
    assert_eq!(table.id(), tid);
    assert_eq!(table.name(), "foo");

    let active_logs = table.active_logs();
    assert_eq!(active_logs.len(), 1);
    assert!(!active_logs.contains(&fid));
    assert!(active_logs.contains(&fid.next()));
    assert!(table.value_logs().contains(&fid));
    assert!(table.frozen_logs().contains(&fid));
    assert!(table.bloomfilters().contains(&fid));
  }

  // after some time, the frozen log and bloomfilter for fid(0) is garbage collected.
  file
    .append_batch([
      ManifestEntry::delete_frozen_log(fid, tid),
      ManifestEntry::delete_bloomfilter(fid, tid),
    ])
    .unwrap();

  {
    let manifest = file.manifest();
    assert_eq!(manifest.tables().len(), 1);

    let table = manifest.get_table("foo").unwrap();
    assert_eq!(table.id(), tid);
    assert_eq!(table.name(), "foo");

    let active_logs = table.active_logs();
    assert_eq!(active_logs.len(), 1);
    assert!(!active_logs.contains(&fid));
    assert!(active_logs.contains(&fid.next()));
    assert!(table.value_logs().contains(&fid));
    assert!(table.frozen_logs().is_empty());
    assert!(table.bloomfilters().is_empty());
  }

  // after some time, the current value log is full, we need a new one
  let new_vlog_id = fid.next().next();
  file
    .append(ManifestEntry::create_value_log(new_vlog_id, tid))
    .unwrap();

  {
    let manifest = file.manifest();
    assert_eq!(manifest.tables().len(), 1);

    let table = manifest.get_table("foo").unwrap();
    assert_eq!(table.id(), tid);
    assert_eq!(table.name(), "foo");

    let active_logs = table.active_logs();
    assert_eq!(active_logs.len(), 1);
    assert!(!active_logs.contains(&fid));
    assert!(active_logs.contains(&fid.next()));

    let value_logs = table.value_logs();
    assert!(value_logs.contains(&new_vlog_id));
    assert!(value_logs.contains(&fid));

    assert!(table.frozen_logs().is_empty());
    assert!(table.bloomfilters().is_empty());
  }

  // after some time, the old value log is garbage collected.
  file
    .append(ManifestEntry::delete_value_log(fid, tid))
    .unwrap();

  {
    let manifest = file.manifest();
    assert_eq!(manifest.tables().len(), 1);

    let table = manifest.get_table("foo").unwrap();
    assert_eq!(table.id(), tid);
    assert_eq!(table.name(), "foo");

    let active_logs = table.active_logs();
    assert_eq!(active_logs.len(), 1);
    assert!(!active_logs.contains(&fid));
    assert!(active_logs.contains(&fid.next()));

    let value_logs = table.value_logs();
    assert!(value_logs.contains(&new_vlog_id));
    assert!(!value_logs.contains(&fid));

    assert!(table.frozen_logs().is_empty());
    assert!(table.bloomfilters().is_empty());
  }

  // after some time, we create a table
  file
    .append(ManifestEntry::create_table(tid.next(), "bar".into()))
    .unwrap();

  {
    let manifest = file.manifest();
    assert_eq!(manifest.tables().len(), 2);
  }

  // after some time, we delete the table
  file
    .append(ManifestEntry::delete_table(tid.next(), "bar".into()))
    .unwrap();

  {
    let manifest = file.manifest();
    assert_eq!(manifest.tables().len(), 1);
  }

  // Now let's test reopening the manifest file.
  drop(file);
  let mut file = ManifestFile::open(
    Some(dir.path()),
    ManifestOptions::new().with_rewrite_threshold(1000),
  )
  .unwrap();

  {
    let manifest = file.manifest();
    assert_eq!(manifest.tables().len(), 1);

    let table = manifest.get_table("foo").unwrap();
    assert_eq!(table.id(), tid);
    assert_eq!(table.name(), "foo");

    let active_logs = table.active_logs();
    assert_eq!(active_logs.len(), 1);
    assert!(!active_logs.contains(&fid));
    assert!(active_logs.contains(&fid.next()));

    let value_logs = table.value_logs();
    assert!(value_logs.contains(&new_vlog_id));
    assert!(!value_logs.contains(&fid));

    assert!(table.frozen_logs().is_empty());
    assert!(table.bloomfilters().is_empty());
  }

  // Now let us test some bad behavior.

  // 1. try to append an entry with impossible flags
  let flag = ManifestEntryFlags(EntryFlags::creation_with_custom_flag(CustomFlags::all()));
  {
    let ent = ManifestEntry(Entry::with_flags(flag.0, ManifestRecord::log(fid, tid)));
    let ManifestRecordError::InvalidEntryFlag(err) =
      file.append(ent.clone()).unwrap_err().unwrap_left()
    else {
      panic!("wrong error")
    };
    assert_eq!(flag, err);

    let ManifestRecordError::InvalidEntryFlag(err) =
      file.append_batch([ent]).unwrap_err().unwrap_left()
    else {
      panic!("wrong error")
    };
    assert_eq!(flag, err);
  }

  // 2. try to create a default table
  {
    let ent = ManifestEntry::create_table(tid, Default::default());
    let ManifestError::ReservedTable = file.append(ent.clone()).unwrap_err().unwrap_middle() else {
      panic!("wrong error")
    };

    let ManifestError::ReservedTable = file.append_batch([ent]).unwrap_err().unwrap_middle() else {
      panic!("wrong error")
    };
  }

  // Now let us trigger the rewrite
  for i in 10..1510u32 {
    file
      .append(ManifestEntry::delete_active_log(
        Fid::from(i),
        TableId::new(0),
      ))
      .unwrap();
  }

  {
    let manifest = file.manifest();
    assert_eq!(manifest.tables().len(), 1, "{:?}", manifest.tables());

    let table = manifest.get_table("foo").unwrap();
    assert_eq!(table.id(), tid);
    assert_eq!(table.name(), "foo");

    let active_logs = table.active_logs();
    assert_eq!(active_logs.len(), 1);
    assert!(!active_logs.contains(&fid));
    assert!(active_logs.contains(&fid.next()));

    let value_logs = table.value_logs();
    assert!(value_logs.contains(&new_vlog_id));
    assert!(!value_logs.contains(&fid));

    assert!(table.frozen_logs().is_empty());
    assert!(table.bloomfilters().is_empty());
  }

  drop(file);
  let file = ManifestFile::open(
    Some(dir.path()),
    ManifestOptions::new().with_rewrite_threshold(1000),
  )
  .unwrap();

  {
    let manifest = file.manifest();
    assert_eq!(manifest.tables().len(), 1, "{:?}", manifest.tables());

    let table = manifest.get_table("foo").unwrap();
    assert_eq!(table.id(), tid);
    assert_eq!(table.name(), "foo");

    let active_logs = table.active_logs();
    assert_eq!(active_logs.len(), 1);
    assert!(!active_logs.contains(&fid));
    assert!(active_logs.contains(&fid.next()));

    let value_logs = table.value_logs();
    assert!(value_logs.contains(&new_vlog_id));
    assert!(!value_logs.contains(&fid));

    assert!(table.frozen_logs().is_empty());
    assert!(table.bloomfilters().is_empty());
  }
}
