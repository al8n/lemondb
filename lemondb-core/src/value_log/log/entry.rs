use core::mem;

use super::{
  super::{merge_lengths, split_lengths},
  VMeta,
};

use dbutils::traits::{Type, TypeRef};

pub(super) struct PhantomEntry;

impl core::fmt::Debug for PhantomEntry {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("PhantomEntry").finish()
  }
}

impl Type for PhantomEntry {
  type Ref<'a> = EntryRef<'a>;

  type Error = ();

  #[inline(never)]
  #[cold]
  fn encoded_len(&self) -> usize {
    unreachable!()
  }

  #[inline(never)]
  #[cold]
  fn encode(&self, _buf: &mut [u8]) -> Result<usize, Self::Error> {
    unreachable!()
  }

  #[inline(never)]
  #[cold]
  fn encode_to_buffer(&self, _buf: &mut valog::VacantBuffer<'_>) -> Result<usize, Self::Error> {
    unreachable!()
  }
}

/// The generic entry in the value log.
pub(super) struct Entry<'a> {
  meta: VMeta,
  key: &'a [u8],
  value: Option<&'a [u8]>,
}

impl<'a> Entry<'a> {
  #[inline]
  pub(super) const fn new(meta: VMeta, key: &'a [u8], value: Option<&'a [u8]>) -> Self {
    Self { meta, key, value }
  }
}

impl<'a> core::fmt::Debug for Entry<'a> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Entry").finish()
  }
}

impl<'a> Type for Entry<'a> {
  type Ref<'b> = EntryRef<'b>;

  type Error = ();

  #[inline]
  fn encoded_len(&self) -> usize {
    let key_len = self.key.encoded_len();

    match self.value {
      Some(v) => {
        let value_len = v.encoded_len();
        VMeta::SIZE + mem::size_of::<u64>() + key_len + value_len
      }
      None => VMeta::SIZE + mem::size_of::<u32>() + key_len,
    }
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    const LEN_SIZE: usize = mem::size_of::<u64>();
    const HALF_LEN_SIZE: usize = LEN_SIZE / 2;

    let mut cursor = 0;
    self.meta.encode(&mut buf[..VMeta::SIZE]);
    cursor += VMeta::SIZE;

    let size = match self.value {
      Some(v) => {
        let klen = self.key.len();
        let vlen = v.len();
        let ko = cursor + LEN_SIZE;
        let vo = cursor + LEN_SIZE + klen;
        buf[ko..ko + klen].copy_from_slice(self.key);
        buf[vo..vo + vlen].copy_from_slice(v);
        let kvlen = merge_lengths(klen as u32, vlen as u32);
        buf[cursor..cursor + LEN_SIZE].copy_from_slice(&kvlen.to_le_bytes());
        cursor += LEN_SIZE + klen + vlen;
        cursor
      }
      None => {
        let klen = self.key.len();
        let ko = cursor + HALF_LEN_SIZE;
        buf[ko..ko + klen].copy_from_slice(self.key);
        buf[cursor..cursor + HALF_LEN_SIZE].copy_from_slice(&(klen as u32).to_le_bytes());
        cursor += HALF_LEN_SIZE + klen;
        cursor
      }
    };

    Ok(size)
  }

  fn encode_to_buffer(&self, buf: &mut valog::VacantBuffer<'_>) -> Result<usize, Self::Error> {
    let len = buf.len();
    self.meta.encode_to_buffer(buf);

    match self.value {
      Some(v) => {
        buf.put_u64_le_unchecked(merge_lengths(self.key.len() as u32, v.len() as u32));
        buf.put_slice_unchecked(self.key);
        buf.put_slice_unchecked(v);
      }
      None => {
        buf.put_u32_le_unchecked(self.key.len() as u32);
        buf.put_slice_unchecked(self.key);
      }
    }

    Ok(buf.len() - len)
  }
}

/// The generic entry reference in the value log.
pub struct EntryRef<'a> {
  meta: VMeta,
  key: &'a [u8],
  value: Option<&'a [u8]>,
}

impl<'a> core::fmt::Debug for EntryRef<'a> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("EntryRef")
      .field("meta", &self.meta)
      .field("key", &self.key)
      .field("value", &self.value)
      .finish()
  }
}

impl<'a> TypeRef<'a> for EntryRef<'a> {
  unsafe fn from_slice(src: &'a [u8]) -> Self {
    const LEN_SIZE: usize = mem::size_of::<u64>();
    const HALF_LEN_SIZE: usize = LEN_SIZE / 2;

    let mut cursor = 0;
    let meta = VMeta::decode(&src[..VMeta::SIZE]);
    cursor += VMeta::SIZE;

    if meta.is_tombstone() {
      let key_len = u32::from_le_bytes([
        src[cursor],
        src[cursor + 1],
        src[cursor + 2],
        src[cursor + 3],
      ]);
      cursor += HALF_LEN_SIZE;
      Self {
        meta,
        key: &src[cursor..cursor + key_len as usize],
        value: None,
      }
    } else {
      let (key_len, value_len) = split_lengths(u64::from_le_bytes([
        src[cursor],
        src[cursor + 1],
        src[cursor + 2],
        src[cursor + 3],
        src[cursor + 4],
        src[cursor + 5],
        src[cursor + 6],
        src[cursor + 7],
      ]));
      let key_len = key_len as usize;
      let value_len = value_len as usize;
      cursor += LEN_SIZE;

      let key = &src[cursor..cursor + key_len];
      cursor += key_len;
      let value = &src[cursor..cursor + value_len];

      Self {
        meta,
        key,
        value: Some(value),
      }
    }
  }
}
