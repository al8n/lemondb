use core::mem;

use super::{
  super::{merge_lengths, split_lengths},
  VMeta,
};

use dbutils::{
  buffer::VacantBuffer,
  types::{Type, TypeRef},
};
use skl::either::Either;

pub(super) struct PhantomGenericEntry<K: ?Sized, V: ?Sized> {
  _phantom: core::marker::PhantomData<(fn() -> K, fn() -> V)>,
}

impl<K: ?Sized, V: ?Sized> core::fmt::Debug for PhantomGenericEntry<K, V> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("PhantomGenericEntry").finish()
  }
}

impl<K, V> Type for PhantomGenericEntry<K, V>
where
  K: ?Sized + core::fmt::Debug + Type,
  V: ?Sized + core::fmt::Debug + Type,
{
  type Ref<'a> = GenericEntryRef<'a, K, V>;

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
  fn encode_to_buffer(
    &self,
    _buf: &mut dbutils::buffer::VacantBuffer<'_>,
  ) -> Result<usize, Self::Error> {
    unreachable!()
  }
}

/// The generic entry in the value log.
pub(super) struct GenericEntry<'a, K: ?Sized, V: ?Sized> {
  meta: VMeta,
  key: &'a K,
  value: Option<&'a V>,
}

impl<'a, K: ?Sized, V: ?Sized> GenericEntry<'a, K, V> {
  #[inline]
  pub(super) const fn new(meta: VMeta, key: &'a K, value: Option<&'a V>) -> Self {
    Self { meta, key, value }
  }
}

impl<K: ?Sized, V: ?Sized> core::fmt::Debug for GenericEntry<'_, K, V> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("GenericEntry").finish()
  }
}

impl<K, V> Type for GenericEntry<'_, K, V>
where
  K: ?Sized + core::fmt::Debug + Type,
  V: ?Sized + core::fmt::Debug + Type,
{
  type Ref<'b> = GenericEntryRef<'b, K, V>;

  type Error = Either<K::Error, V::Error>;

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
        let key_len = self
          .key
          .encode(&mut buf[cursor + LEN_SIZE..])
          .map_err(Either::Left)?;
        let value_len = v
          .encode(&mut buf[cursor + LEN_SIZE + key_len..])
          .map_err(Either::Right)?;
        let kvlen = merge_lengths(key_len as u32, value_len as u32);
        buf[cursor..cursor + LEN_SIZE].copy_from_slice(&kvlen.to_le_bytes());
        cursor += LEN_SIZE + key_len + value_len;
        cursor
      }
      None => {
        let key_len = self
          .key
          .encode(&mut buf[cursor + HALF_LEN_SIZE..])
          .map_err(Either::Left)?;
        buf[cursor..cursor + HALF_LEN_SIZE].copy_from_slice(&key_len.to_le_bytes());
        cursor += HALF_LEN_SIZE + key_len;
        cursor
      }
    };

    Ok(size)
  }

  fn encode_to_buffer(&self, buf: &mut VacantBuffer<'_>) -> Result<usize, Self::Error> {
    const LEN_SIZE: usize = mem::size_of::<u64>();
    const HALF_LEN_SIZE: usize = LEN_SIZE / 2;

    let start = buf.len();
    let mut cursor = start;
    self.meta.encode_to_buffer(buf);
    cursor += VMeta::SIZE;

    match self.value {
      Some(v) => {
        buf.put_u64_le_unchecked(0); // placeholder for the length
        cursor += LEN_SIZE;
        let key_len = self.key.encode_to_buffer(buf).map_err(Either::Left)?;
        let value_len = v.encode_to_buffer(buf).map_err(Either::Right)?;
        let kvlen = merge_lengths(key_len as u32, value_len as u32);
        buf[cursor - LEN_SIZE..cursor].copy_from_slice(&kvlen.to_le_bytes());
        cursor += key_len + value_len;
        Ok(cursor)
      }
      None => {
        buf.put_u32_le_unchecked(0); // placeholder for the length
        cursor += HALF_LEN_SIZE;
        let key_len = self.key.encode_to_buffer(buf).map_err(Either::Left)?;
        buf[cursor - HALF_LEN_SIZE..cursor].copy_from_slice(&(key_len as u32).to_le_bytes());
        cursor += key_len;
        Ok(cursor - start)
      }
    }
  }
}

/// The generic entry reference in the value log.
pub struct GenericEntryRef<'a, K: ?Sized + Type, V: ?Sized + Type> {
  meta: VMeta,
  key: K::Ref<'a>,
  value: Option<V::Ref<'a>>,
}

impl<K, V> core::fmt::Debug for GenericEntryRef<'_, K, V>
where
  K: ?Sized + core::fmt::Debug + Type,
  V: ?Sized + core::fmt::Debug + Type,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("GenericEntryRef")
      .field("meta", &self.meta)
      .field("key", &self.key)
      .field("value", &self.value)
      .finish()
  }
}

impl<'a, K, V> TypeRef<'a> for GenericEntryRef<'a, K, V>
where
  K: ?Sized + core::fmt::Debug + Type,
  V: ?Sized + core::fmt::Debug + Type,
{
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
      let key = <K::Ref<'_> as TypeRef<'_>>::from_slice(&src[cursor..cursor + key_len as usize]);
      Self {
        meta,
        key,
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

      let key = <K::Ref<'_> as TypeRef<'_>>::from_slice(&src[cursor..cursor + key_len]);
      cursor += key_len;
      let value = <V::Ref<'_> as TypeRef<'_>>::from_slice(&src[cursor..cursor + value_len]);

      Self {
        meta,
        key,
        value: Some(value),
      }
    }
  }
}
