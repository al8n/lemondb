use core::marker::PhantomData;

use among::Among;
use dbutils::{
  buffer::VacantBuffer,
  types::{MaybeStructured, Type, TypeRef},
};

use skl::either::Either;

use super::pointer::Pointer;

///  value.
pub struct PhantomValue<V: ?Sized>(PhantomData<V>);

impl<V: ?Sized> core::fmt::Debug for PhantomValue<V> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Value").finish()
  }
}

impl<V: ?Sized + Type> Type for PhantomValue<V> {
  type Ref<'a> = ValueRef<'a, V>;

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

/// The generic value store in the database.
pub struct Value<'a, V: ?Sized>(Either<MaybeStructured<'a, V>, Pointer>);

impl<V> core::fmt::Debug for Value<'_, V>
where
  V: ?Sized + Type + core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self.0 {
      Either::Left(ref value) => match value.data() {
        Either::Left(ref data) => f.debug_tuple("Value").field(data).finish(),
        Either::Right(ref raw) => f.debug_tuple("Value").field(raw).finish(),
      },
      Either::Right(ref pointer) => f.debug_tuple("Value").field(pointer).finish(),
    }
  }
}

impl<'a, V: ?Sized> Value<'a, V> {
  /// Creates a new generic value.
  #[inline]
  pub fn new(value: Either<MaybeStructured<'a, V>, Pointer>) -> Self {
    Self(value)
  }

  /// Adds a pointer to the generic value.
  #[inline]
  pub fn with_pointer(self, pointer: Pointer) -> Self {
    Self(Either::Right(pointer))
  }
}

impl<V> Type for Value<'_, V>
where
  V: ?Sized + Type,
{
  type Ref<'b> = ValueRef<'b, V>;

  // TODO: error optimization
  type Error = V::Error;

  #[inline]
  fn encoded_len(&self) -> usize {
    1 + match self.0 {
      Either::Left(ref value) => value.encoded_len(),
      Either::Right(_) => Pointer::ENCODED_LEN,
    }
  }

  #[inline]
  fn encode_to_buffer(&self, buf: &mut VacantBuffer<'_>) -> Result<usize, Self::Error> {
    match self.0 {
      Either::Left(ref value) => {
        buf.put_u8_unchecked(1);
        value.encode_to_buffer(buf).map(|len| 1 + len)
      }
      Either::Right(ref pointer) => {
        buf.put_u8_unchecked(2);
        pointer
          .encode_to_buffer(buf)
          .expect("not enough space to encode pointer");
        Ok(1 + Pointer::ENCODED_LEN)
      }
    }
  }
}

/// The generic value reference to the value log.
pub struct ValueRef<'a, V: ?Sized + Type> {
  value: Among<(), V::Ref<'a>, Pointer>,
}

impl<V: ?Sized + Type> Clone for ValueRef<'_, V> {
  #[inline]
  fn clone(&self) -> Self {
    *self
  }
}

impl<V: ?Sized + Type> Copy for ValueRef<'_, V> {}

impl<'a, V> core::fmt::Debug for ValueRef<'a, V>
where
  V: ?Sized + Type,
  V::Ref<'a>: core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match &self.value {
      Among::Left(_) => f.debug_tuple("Value").field(&()).finish(),
      Among::Middle(ref value) => f.debug_tuple("Value").field(value).finish(),
      Among::Right(ref pointer) => f.debug_tuple("Value").field(pointer).finish(),
    }
  }
}

impl<'a, V> TypeRef<'a> for ValueRef<'a, V>
where
  V: ?Sized + Type,
{
  #[inline]
  unsafe fn from_slice(buf: &'a [u8]) -> Self {
    let value = match buf[0] {
      0 => Among::Left(()),
      1 => Among::Middle(V::Ref::from_slice(&buf[1..])),
      2 => Among::Right(Pointer::decode(&buf[1..]).unwrap()),
      _ => unreachable!(),
    };

    Self { value }
  }
}
