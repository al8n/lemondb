use super::{
  error::{DecodeHeaderError, ValueLogError},
  options::{CreateOptions, OpenOptions},
  util::{decode_varint, encode_varint, encoded_len_varint},
  *,
};

use core::cell::UnsafeCell;

#[cfg(feature = "std")]
mod mmap;
use error::EncodeHeaderError;
#[cfg(feature = "std")]
use mmap::*;

#[derive(derive_more::From)]
enum ValueLogKind {
  Mmap(MmapValueLog),
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
  kp: Pointer,
  vl: u32,
  version: u64,
}

impl Header {
  const MAX_ENCODED_SIZE: usize = Pointer::MAX_ENCODING_SIZE + 5 + 10;
  const MIN_ENCODED_SIZE: usize = 1 + 1 + 1;

  #[inline]
  const fn new(version: u64, kp: Pointer, vl: usize) -> Self {
    Self {
      kp,
      vl: vl as u32,
      version,
    }
  }

  fn encode(&self) -> Result<EncodedHeader, ValueLogError> {
    let mut buf = [0; Self::MAX_ENCODED_SIZE];

    let mut cur = 0;

    // encode key length
    cur += self.kp.encode(&mut buf[cur..]).map_err(|e| match e {
      PointerError::VarintError(e) => EncodeHeaderError::VarintError(e),
      PointerError::BufferTooSmall => EncodeHeaderError::BufferTooSmall,
      PointerError::NotEnoughBytes => unreachable!(),
    })?;
    // encode value length
    cur +=
      encode_varint(self.vl as u64, &mut buf[cur..]).map_err(EncodeHeaderError::VarintError)?;

    // encode version
    cur += encode_varint(self.version, &mut buf[cur..]).map_err(EncodeHeaderError::VarintError)?;

    Ok(EncodedHeader { buf, len: cur })
  }

  fn decode(buf: &[u8]) -> Result<(usize, Self), ValueLogError> {
    if buf.len() < Self::MIN_ENCODED_SIZE {
      return Err(DecodeHeaderError::NotEnoughBytes.into());
    }

    let mut readed = 0;
    let (kp_size, kp) = Pointer::decode(&buf[readed..]).map_err(|e| match e {
      PointerError::VarintError(e) => DecodeHeaderError::VarintError(e),
      PointerError::NotEnoughBytes => DecodeHeaderError::NotEnoughBytes,
      PointerError::BufferTooSmall => unreachable!(),
    })?;
    readed += kp_size;

    let (vl_size, vl) = decode_varint(&buf[readed..]).map_err(DecodeHeaderError::VarintError)?;
    readed += vl_size;

    let (version_size, version) =
      decode_varint(&buf[readed..]).map_err(DecodeHeaderError::VarintError)?;
    readed += version_size;

    Ok((
      readed,
      Self {
        kp,
        vl: vl as u32,
        version,
      },
    ))
  }

  #[inline]
  const fn encoded_len(&self) -> usize {
    self.kp.encoded_size() + encoded_len_varint(self.vl as u64) + encoded_len_varint(self.version)
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

impl ValueLog {
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
    match self.kind() {
      ValueLogKind::Mmap(vlf) => vlf.remove(),
    }
  }

  pub fn write(
    &mut self,
    version: u64,
    key: &[u8],
    value: &[u8],
  ) -> Result<Pointer, ValueLogError> {
    match self.kind_mut() {
      ValueLogKind::Mmap(vlf) => vlf.write(version, key, value),
    }
  }

  pub fn read(&self, offset: usize, size: usize) -> Result<&[u8], ValueLogError> {
    match self.kind() {
      ValueLogKind::Mmap(vlf) => vlf.read(offset, size),
    }
  }

  #[inline]
  pub fn rewind(&mut self, size: usize) -> Result<(), ValueLogError> {
    match self.kind_mut() {
      ValueLogKind::Mmap(vlf) => vlf.rewind(size),
    }
  }

  #[inline]
  pub fn len(&self) -> usize {
    match self.kind() {
      ValueLogKind::Mmap(vlf) => vlf.len(),
    }
  }

  #[inline]
  pub fn capacity(&self) -> u64 {
    match self.kind() {
      ValueLogKind::Mmap(vlf) => vlf.capacity(),
    }
  }

  #[inline]
  pub fn remaining(&self) -> u64 {
    match self.kind() {
      ValueLogKind::Mmap(vlf) => vlf.remaining(),
    }
  }
  #[inline]
  pub fn fid(&self) -> u32 {
    match self.kind() {
      ValueLogKind::Mmap(vlf) => vlf.fid(),
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
