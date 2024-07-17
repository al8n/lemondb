use super::{
  error::{DecodeHeaderError, ValueLogError},
  options::{CreateOptions, OpenOptions},
  util::{decode_varint, encode_varint, encoded_len_varint},
  *,
};

use core::cell::UnsafeCell;

use error::EncodeHeaderError;

#[cfg(feature = "std")]
use mmap::*;
#[cfg(feature = "std")]
use mmap_anon::*;

#[cfg(feature = "std")]
mod mmap;
#[cfg(feature = "std")]
mod mmap_anon;

#[derive(derive_more::From)]
enum ValueLogKind {
  Placeholder(Fid),
  // Memory(MemoryValueLog),
  #[cfg(feature = "std")]
  Mmap(MmapValueLog),
  #[cfg(feature = "std")]
  MmapAnon(MmapAnonValueLog),
}

struct EncodedHeader {
  buf: [u8; Header::MAX_ENCODED_SIZE],
  len: usize,
}

impl core::ops::Deref for EncodedHeader {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    &self.buf[..self.len]
  }
}

struct Header {
  kl: u32,
  vl: u32,
  cks: u32,
  version: u64,
}

impl Header {
  const MAX_ENCODED_SIZE: usize = 5 + 10 + 10;
  const MIN_ENCODED_SIZE: usize = 1 + 1 + 1;

  #[inline]
  const fn new(version: u64, kl: usize, vl: usize, cks: u32) -> Self {
    Self {
      kl: kl as u32,
      vl: vl as u32,
      cks,
      version,
    }
  }

  fn encode(&self) -> Result<EncodedHeader, ValueLogError> {
    let mut buf = [0; Self::MAX_ENCODED_SIZE];

    let mut cur = 0;
    // encode key length
    cur += encode_varint(self.kl as u64, &mut buf).map_err(EncodeHeaderError::VarintError)?;
    let vlcks = self.encode_vlcks();

    // encode value length and checksum
    cur += encode_varint(vlcks, &mut buf[cur..]).map_err(EncodeHeaderError::VarintError)?;

    // encode version
    cur += encode_varint(self.version, &mut buf[cur..]).map_err(EncodeHeaderError::VarintError)?;

    Ok(EncodedHeader { buf, len: cur })
  }

  fn decode(buf: &[u8]) -> Result<(usize, Self), ValueLogError> {
    if buf.len() < Self::MIN_ENCODED_SIZE {
      return Err(DecodeHeaderError::NotEnoughBytes.into());
    }

    let mut readed = 0;
    let (kl_size, kl) = decode_varint(buf).map_err(DecodeHeaderError::VarintError)?;
    readed += kl_size;
    let kl = kl as u32;

    let (vlcks_size, vlcks) =
      decode_varint(&buf[readed..]).map_err(DecodeHeaderError::VarintError)?;
    readed += vlcks_size;

    let (version_size, version) =
      decode_varint(&buf[readed..]).map_err(DecodeHeaderError::VarintError)?;
    readed += version_size;

    let (vl, cks) = Self::decode_vlcks(vlcks);

    Ok((
      readed,
      Self {
        kl,
        vl,
        cks,
        version,
      },
    ))
  }

  #[inline]
  const fn encoded_len(&self) -> usize {
    encoded_len_varint(self.kl as u64) + encoded_len_varint(self.encode_vlcks())
  }

  #[inline]
  const fn encode_vlcks(&self) -> u64 {
    // high 32 bits of value length, low 32 bits of checksum
    ((self.vl as u64) << 32) | self.cks as u64
  }

  #[inline]
  const fn decode_vlcks(src: u64) -> (u32, u32) {
    // high 32 bits of value length, low 32 bits of checksum
    ((src >> 32) as u32, src as u32)
  }
}

/// ValueLog is not thread safe and cannot be used concurrently.
///
/// ```test
/// +--------+-----+-----+
/// | header | key | val |
/// +--------+-----+-----+
/// ```
pub struct ValueLog {
  kind: UnsafeCell<ValueLogKind>,
}

// Safety: ValueLog is thread safe and will not be used concurrently in this crate.
unsafe impl Send for ValueLog {}
unsafe impl Sync for ValueLog {}

impl ValueLog {
  pub fn placeholder(fid: Fid) -> Self {
    Self {
      kind: UnsafeCell::new(ValueLogKind::Placeholder(fid)),
    }
  }

  pub fn create(opts: CreateOptions) -> Result<Self, ValueLogError> {
    Ok(Self {
      kind: UnsafeCell::new(ValueLogKind::Mmap(MmapValueLog::create(opts)?)),
    })
  }

  #[cfg(feature = "std")]
  pub fn open(opts: OpenOptions) -> Result<Self, ValueLogError> {
    Ok(Self {
      kind: UnsafeCell::new(ValueLogKind::Mmap(MmapValueLog::open(opts)?)),
    })
  }

  #[cfg(feature = "std")]
  pub fn remove(&self) -> Result<(), ValueLogError> {
    match self.kind_mut() {
      ValueLogKind::Mmap(vlf) => vlf.remove(),
      ValueLogKind::MmapAnon(vlf) => vlf.remove(),
      ValueLogKind::Placeholder(_) => Ok(()),
    }
  }

  pub fn write(
    &self,
    version: u64,
    key: &[u8],
    value: &[u8],
    checksum: u32,
  ) -> Result<Pointer, ValueLogError> {
    match self.kind_mut() {
      ValueLogKind::Mmap(vlf) => vlf.write(version, key, value, checksum),
      ValueLogKind::MmapAnon(vlf) => vlf.write(version, key, value, checksum),
      ValueLogKind::Placeholder(_) => Err(ValueLogError::NotEnoughSpace {
        required: self.encoded_entry_size(version, key, value, checksum) as u64,
        remaining: 0,
      }),
    }
  }

  /// Returns a byte slice which contains header, key and value.
  pub(crate) fn read(&self, offset: usize, size: usize) -> Result<&[u8], ValueLogError> {
    match self.kind() {
      ValueLogKind::Mmap(vlf) => vlf.read(offset, size),
      ValueLogKind::MmapAnon(vlf) => vlf.read(offset, size),
      ValueLogKind::Placeholder(_) => Err(ValueLogError::OutOfBound {
        offset,
        len: size,
        size: 0,
      }),
    }
  }

  /// Returns the encoded entry size for the given key and value.
  pub(crate) fn encoded_entry_size(&self, version: u64, key: &[u8], val: &[u8], cks: u32) -> usize {
    let kl = key.len();
    let vl = val.len();
    let h = Header::new(version, kl, vl, cks);
    h.encoded_len() + kl + vl
  }

  #[inline]
  pub fn rewind(&self, size: usize) -> Result<(), ValueLogError> {
    match self.kind_mut() {
      ValueLogKind::Mmap(vlf) => vlf.rewind(size),
      ValueLogKind::MmapAnon(vlf) => vlf.rewind(size),
      ValueLogKind::Placeholder(_) => Ok(()),
    }
  }

  #[inline]
  pub fn len(&self) -> usize {
    match self.kind() {
      ValueLogKind::Mmap(vlf) => vlf.len(),
      ValueLogKind::MmapAnon(vlf) => vlf.len(),
      ValueLogKind::Placeholder(_) => 0,
    }
  }

  #[inline]
  pub fn capacity(&self) -> u64 {
    match self.kind() {
      ValueLogKind::Mmap(vlf) => vlf.capacity(),
      ValueLogKind::MmapAnon(vlf) => vlf.capacity(),
      ValueLogKind::Placeholder(_) => 0,
    }
  }

  #[inline]
  pub fn remaining(&self) -> u64 {
    match self.kind() {
      ValueLogKind::Mmap(vlf) => vlf.remaining(),
      ValueLogKind::MmapAnon(vlf) => vlf.remaining(),
      ValueLogKind::Placeholder(_) => 0,
    }
  }

  #[inline]
  pub fn fid(&self) -> Fid {
    match self.kind() {
      ValueLogKind::Mmap(vlf) => vlf.fid(),
      ValueLogKind::MmapAnon(vlf) => vlf.fid(),
      ValueLogKind::Placeholder(fid) => *fid,
    }
  }

  #[allow(clippy::mut_from_ref)]
  #[inline]
  fn kind_mut(&self) -> &mut ValueLogKind {
    // Safety: ValueLog is not thread safe and will not be used concurrently in this crate.
    unsafe { &mut *self.kind.get() }
  }

  #[inline]
  fn kind(&self) -> &ValueLogKind {
    // Safety: ValueLog is not thread safe and will not be used concurrently in this crate.
    unsafe { &*self.kind.get() }
  }
}
