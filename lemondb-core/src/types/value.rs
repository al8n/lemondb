use core::marker::PhantomData;

use among::Among;
use dbutils::{
  buffer::VacantBuffer,
  traits::{Type, TypeRef},
};

use skl::either::Either;

use super::pointer::Pointer;

///  value.
pub struct PhantomValue(PhantomData<[u8]>);

impl core::fmt::Debug for PhantomValue {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Value").finish()
  }
}

impl Type for PhantomValue {
  type Ref<'a> = ValueRef<'a>;

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
  fn encode_to_buffer(&self, _buf: &mut VacantBuffer<'_>) -> Result<usize, Self::Error> {
    unreachable!()
  }
}

/// The  value store in the database.
pub struct Value<'a>(Among<(), &'a [u8], Pointer>);

impl<'a> core::fmt::Debug for Value<'a> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self.0 {
      Among::Left(_) => f.debug_tuple("Value").field(&()).finish(),
      Among::Middle(ref value) => f.debug_tuple("Value").field(value).finish(),
      Among::Right(ref pointer) => f.debug_tuple("Value").field(pointer).finish(),
    }
  }
}

impl<'a> Value<'a> {
  /// Creates a new  value.
  #[inline]
  pub fn new(value: Option<Either<&'a [u8], Pointer>>) -> Self {
    match value {
      None => Self(Among::Left(())),
      Some(Either::Left(a)) => Self(Among::Middle(a)),
      Some(Either::Right(b)) => Self(Among::Right(b)),
    }
  }

  /// Adds a pointer to the  value.
  #[inline]
  pub fn with_pointer(self, pointer: Pointer) -> Self {
    Self(Among::Right(pointer))
  }
}

impl<'a> Type for Value<'a> {
  type Ref<'b> = ValueRef<'b>;

  type Error = ();

  #[inline]
  fn encoded_len(&self) -> usize {
    1 + match self.0 {
      Among::Left(_) => 0,
      Among::Middle(value) => value.len(),
      Among::Right(_) => Pointer::ENCODED_LEN,
    }
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    match self.0 {
      Among::Left(_) => {
        buf[0] = 0;
        Ok(1)
      }
      Among::Middle(value) => {
        buf[0] = 1;
        let vlen = value.len();
        buf[1..1 + vlen].copy_from_slice(value);
        Ok(1 + vlen)
      }
      Among::Right(ref pointer) => {
        buf[0] = 2;
        pointer
          .encode(&mut buf[1..])
          .expect("not enough space to encode pointer");
        Ok(1 + Pointer::ENCODED_LEN)
      }
    }
  }

  #[inline]
  fn encode_to_buffer(&self, buf: &mut VacantBuffer<'_>) -> Result<usize, Self::Error> {
    match self.0 {
      Among::Left(_) => {
        buf.put_u8_unchecked(0);
        Ok(1)
      }
      Among::Middle(value) => {
        buf.put_u8_unchecked(1);
        buf.put_slice_unchecked(value);
        Ok(1 + value.len())
      }
      Among::Right(ref pointer) => {
        buf.put_u8_unchecked(2);
        pointer
          .encode_to_buffer(buf)
          .expect("not enough space to encode pointer");
        Ok(1 + Pointer::ENCODED_LEN)
      }
    }
  }
}

/// The value reference to the value log.
pub struct ValueRef<'a> {
  value: Among<(), &'a [u8], Pointer>,
}

impl<'a> core::fmt::Debug for ValueRef<'a> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match &self.value {
      Among::Left(_) => f.debug_tuple("Value").field(&()).finish(),
      Among::Middle(ref value) => f.debug_tuple("Value").field(value).finish(),
      Among::Right(ref pointer) => f.debug_tuple("Value").field(pointer).finish(),
    }
  }
}

impl<'a> TypeRef<'a> for ValueRef<'a> {
  #[inline]
  unsafe fn from_slice(buf: &'a [u8]) -> Self {
    let value = match buf[0] {
      0 => Among::Left(()),
      1 => Among::Middle(&buf[1..]),
      2 => Among::Right(Pointer::decode(&buf[1..]).unwrap()),
      _ => unreachable!(),
    };

    Self { value }
  }
}
