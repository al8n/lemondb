use aol::{memory::Snapshot, CustomFlags, Entry};

use super::*;

impl Snapshot for Manifest {
  type Record = ManifestRecord;

  type Options = ManifestOptions;

  type Error = ManifestError;

  fn new(opts: Self::Options) -> Result<Self, Self::Error> {
    Ok(Self {
      tables: HashMap::new(),
      last_fid: Fid::new(0),
      last_table_id: TableId::new(0),
      creations: 0,
      deletions: 0,
      opts,
    })
  }

  #[inline]
  fn validate(&self, entry: &Entry<Self::Record>) -> Result<(), Self::Error> {
    self.validate_in(entry)
  }

  fn options(&self) -> &Self::Options {
    &self.opts
  }

  fn should_rewrite(&self) -> bool {
    self.deletions > self.opts.rewrite_threshold
      && self.deletions > MANIFEST_DELETIONS_RATIO * self.creations.saturating_sub(self.deletions)
  }

  #[inline]
  fn insert(&mut self, entry: Entry<Self::Record>) -> Result<(), Self::Error> {
    self.insert_in(entry)
  }

  fn into_iter(self) -> impl Iterator<Item = Entry<Self::Record>> {
    self
      .tables
      .into_iter()
      .filter_map(|(tid, table)| {
        if table.is_removed() {
          return None;
        }

        Some(
          core::iter::once(Entry::creation(ManifestRecord::Table {
            id: tid,
            name: table.name,
          }))
          .chain(
            table
              .vlogs
              .into_iter()
              .map(move |fid| {
                Entry::creation_with_custom_flags(
                  CustomFlags::empty().with_bit1(),
                  ManifestRecord::Log { fid, tid },
                )
              })
              .chain(
                table
                  .logs
                  .into_iter()
                  .map(move |fid| Entry::creation(ManifestRecord::Log { fid, tid })),
              ),
          ),
        )
      })
      .flatten()
  }
}

pub(crate) struct MemoryManifest {
  manifest: Manifest,
}

impl MemoryManifest {
  #[inline]
  pub(super) fn new(opts: ManifestOptions) -> Self {
    Self {
      manifest: Manifest::new(opts).unwrap(),
    }
  }

  #[inline]
  pub(super) fn append(&mut self, entry: aol::Entry<ManifestRecord>) -> Result<(), ManifestError> {
    self.manifest.insert(entry)
  }

  #[inline]
  pub(super) fn append_batch(
    &mut self,
    entries: Vec<aol::Entry<ManifestRecord>>,
  ) -> Result<(), ManifestError> {
    self.manifest.insert_batch(entries.into_iter())
  }

  #[inline]
  pub(super) fn last_fid(&self) -> Fid {
    self.manifest.last_fid
  }
}
