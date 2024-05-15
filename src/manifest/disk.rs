use std::{
  fs::{File, OpenOptions},
  io::{self, BufWriter, Read, Write},
  path::{Path, PathBuf},
};

use super::*;

const MANIFEST_FILENAME: &str = "MANIFEST";
const MANIFEST_DELETIONS_REWRITE_THRESHOLD: usize = 10000;
const MANIFEST_DELETIONS_RATIO: usize = 10;
const KB: usize = 1000;

/// Magic text for the manifest file, this will never be changed.
const MAGIC_TEXT: [u8; 4] = *b"al8n";
const MANIFEST_HEADER_SIZE: usize = MAGIC_TEXT.len() + 2 + 2; // magic text + external magic + lemon magic

/// File structure:
///
/// ```text
/// +----------------------+--------------------------+-----------------------+
/// | magic text (4 bytes) | external magic (2 bytes) | lemon magic (2 bytes) |
/// +----------------------+--------------------------+-----------------------+
/// | kind (1 byte)        | fid (4 bytes)            | checksum (4 bytes)    |
/// +----------------------+--------------------------+-----------------------+
/// | kind (1 byte)        | fid (4 bytes)            | checksum (4 bytes)    |
/// +----------------------+--------------------------+-----------------------+
/// | ...                  | ...                      | ...                   |
/// +----------------------+--------------------------+-----------------------+
/// ```
pub struct DiskManifest {
  path: PathBuf,
  file: BufWriter<File>,
  manifest: Manifest,
  rewrite_threshold: usize,
}

impl DiskManifest {
  /// Open and replay the manifest file.
  pub fn open<P: AsRef<Path>>(
    path: P,
    rewrite_threshold: usize,
    external_version: u16,
    lemon_version: u16,
  ) -> Result<Self, ManifestError> {
    let path = path.as_ref().join(MANIFEST_FILENAME);
    let existing = path.exists();
    let mut file = OpenOptions::new()
      .read(true)
      .create(true)
      .truncate(false)
      .append(true)
      .open(&path)?;

    if !existing {
      let mut buf = [0; MANIFEST_HEADER_SIZE];
      buf[..4].copy_from_slice(&MAGIC_TEXT);
      buf[4..6].copy_from_slice(&external_version.to_le_bytes());
      buf[6..8].copy_from_slice(&lemon_version.to_le_bytes());
      file.write_all(&buf)?;
      file.flush()?;

      return Ok(Self {
        rewrite_threshold,
        file: BufWriter::new(file),
        manifest: Manifest::default(),
        path,
      });
    }

    let mut header = [0; MANIFEST_HEADER_SIZE];
    file.read_exact(&mut header)?;

    if header[..4] != MAGIC_TEXT {
      return Err(ManifestError::BadMagic);
    }

    let external = u16::from_le_bytes(header[4..6].try_into().unwrap());
    if external != external_version {
      return Err(ManifestError::BadExternalVersion {
        expected: external_version,
        found: external,
      });
    }

    let version = u16::from_le_bytes(header[6..MANIFEST_HEADER_SIZE].try_into().unwrap());
    if version != lemon_version {
      return Err(ManifestError::BadVersion {
        expected: lemon_version,
        found: version,
      });
    }

    let mut buffers_: Vec<_> = (0..16).map(|_| vec![0; KB]).collect();
    let mut buffers: Vec<_> = buffers_
      .iter_mut()
      .map(|buf| io::IoSliceMut::new(buf))
      .collect();

    let mut creations = 0;
    let mut deletions = 0;
    let mut vlogs = HashSet::new();
    let mut logs = HashSet::new();
    let mut last_fid = 0;
    loop {
      let bytes_read = file.read_vectored(&mut buffers)?;
      if bytes_read == 0 {
        break;
      }

      for buf in &buffers {
        for encoded in buf.split(|&b| b == b'\n') {
          if encoded.len() < ManifestEvent::MAX_ENCODED_SIZE - 1 {
            return Err(ManifestError::Corrupted);
          }

          let kind = ManifestEventKind::try_from(encoded[0])?;
          let fid = u32::from_le_bytes(encoded[1..5].try_into().unwrap());
          let cks = u32::from_le_bytes(encoded[5..9].try_into().unwrap());
          let cks2 = crc32fast::hash(&encoded[..5]);
          if cks != cks2 {
            return Err(ManifestError::ChecksumMismatch);
          }

          last_fid = last_fid.max(fid);

          match kind {
            ManifestEventKind::AddVlog => {
              creations += 1;
              vlogs.insert(fid);
            }
            ManifestEventKind::AddLog => {
              creations += 1;
              logs.insert(fid);
            }
            ManifestEventKind::RemoveVlog => {
              deletions += 1;
              vlogs.remove(&fid);
            }
            ManifestEventKind::RemoveLog => {
              deletions += 1;
              logs.remove(&fid);
            }
          }
        }
      }
    }

    let manifest = Manifest {
      vlogs,
      logs,
      last_fid,
      creations,
      deletions,
    };

    let mut this = Self {
      rewrite_threshold,
      file: BufWriter::new(file),
      manifest,
      path,
    };

    if this.should_rewrite() {
      return this.rewrite().map(|_| this);
    }

    Ok(this)
  }

  #[inline]
  pub fn append(&mut self, event: ManifestEvent) -> Result<(), ManifestError> {
    if self.should_rewrite() {
      self.rewrite()?;
    }

    append(&mut self.file, event).map(|_| match event.kind {
      ManifestEventKind::AddVlog => {
        self.manifest.vlogs.insert(event.fid);
        self.manifest.last_fid = self.manifest.last_fid.max(event.fid);
        self.manifest.creations += 1;
      }
      ManifestEventKind::AddLog => {
        self.manifest.logs.insert(event.fid);
        self.manifest.last_fid = self.manifest.last_fid.max(event.fid);
        self.manifest.creations += 1;
      }
      ManifestEventKind::RemoveVlog => {
        self.manifest.vlogs.remove(&event.fid);
        self.manifest.deletions += 1;
      }
      ManifestEventKind::RemoveLog => {
        self.manifest.logs.remove(&event.fid);
        self.manifest.deletions += 1;
      }
    })
  }

  #[inline]
  pub fn flush(&mut self) -> Result<(), ManifestError> {
    flush(&mut self.file)
  }

  #[inline]
  pub fn sync_all(&mut self) -> Result<(), ManifestError> {
    self.flush()?;
    self.file.get_mut().sync_all().map_err(Into::into)
  }

  #[inline]
  pub const fn last_fid(&self) -> u32 {
    self.manifest.last_fid
  }

  #[inline]
  fn should_rewrite(&self) -> bool {
    self.manifest.deletions > self.rewrite_threshold
      && self.manifest.deletions
        > MANIFEST_DELETIONS_RATIO
          * self
            .manifest
            .creations
            .saturating_sub(self.manifest.deletions)
  }

  fn rewrite(&mut self) -> Result<(), ManifestError> {
    self.manifest.deletions = 0;

    // truncate the file
    self.file.get_mut().set_len(MANIFEST_HEADER_SIZE as u64)?;

    let mut last_fid = 0;
    for fid in self.manifest.logs.iter() {
      append(
        &mut self.file,
        ManifestEvent {
          kind: ManifestEventKind::AddLog,
          fid: *fid,
        },
      )?;
      last_fid = last_fid.max(*fid);
    }

    for fid in self.manifest.vlogs.iter() {
      append(
        &mut self.file,
        ManifestEvent {
          kind: ManifestEventKind::AddVlog,
          fid: *fid,
        },
      )?;
      last_fid = last_fid.max(*fid);
    }

    self.manifest.creations = self.manifest.logs.len() + self.manifest.vlogs.len();
    self.manifest.last_fid = last_fid;
    self.file.flush()?;
    self.file.get_mut().sync_all()?;
    Ok(())
  }
}

pub fn append(file: &mut BufWriter<File>, event: ManifestEvent) -> Result<(), ManifestError> {
  let mut buf = [0; ManifestEvent::MAX_ENCODED_SIZE];
  buf[0] = event.kind as u8;
  buf[1..5].copy_from_slice(&event.fid.to_le_bytes());
  let cks = crc32fast::hash(&buf[..5]).to_le_bytes();
  buf[5..9].copy_from_slice(&cks);
  buf[9] = b'\n';
  file.write_all(&buf).map_err(Into::into)
}

pub fn flush(file: &mut BufWriter<File>) -> Result<(), ManifestError> {
  file.flush().map_err(Into::into)
}
