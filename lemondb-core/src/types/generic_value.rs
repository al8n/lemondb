use core::marker::PhantomData;

use among::Among;
use dbutils::traits::{Type, TypeRef};

use orderwal::Generic;
use skl::either::Either;

use super::pointer::Pointer;

/// Generic value.
pub struct PhantomGenericValue<V: ?Sized>(PhantomData<V>);

impl<V: ?Sized> core::fmt::Debug for PhantomGenericValue<V> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("GenericValue").finish()
  }
}

impl<V: ?Sized + Type> Type for PhantomGenericValue<V> {
  type Ref<'a> = GenericValueRef<'a, V>;

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
}

/// The generic value store in the database.
pub struct GenericValue<'a, V: ?Sized>(Among<(), Generic<'a, V>, Pointer>);

impl<'a, V> core::fmt::Debug for GenericValue<'a, V>
where
  V: ?Sized + Type + core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self.0 {
      Among::Left(_) => f.debug_tuple("GenericValue").field(&()).finish(),
      Among::Middle(ref value) => match value.data() {
        Either::Left(ref data) => f.debug_tuple("GenericValue").field(data).finish(),
        Either::Right(ref raw) => f.debug_tuple("GenericValue").field(raw).finish(),
      },
      Among::Right(ref pointer) => f.debug_tuple("GenericValue").field(pointer).finish(),
    }
  }
}

impl<'a, V: ?Sized> GenericValue<'a, V> {
  /// Creates a new generic value.
  #[inline]
  pub fn new(value: Option<Either<Generic<'a, V>, Pointer>>) -> Self {
    match value {
      None => Self(Among::Left(())),
      Some(Either::Left(a)) => Self(Among::Middle(a)),
      Some(Either::Right(b)) => Self(Among::Right(b)),
    }
  }

  /// Adds a pointer to the generic value.
  #[inline]
  pub fn with_pointer(self, pointer: Pointer) -> Self {
    Self(Among::Right(pointer))
  }
}

impl<'a, V> Type for GenericValue<'a, V>
where
  V: ?Sized + Type,
{
  type Ref<'b> = GenericValueRef<'b, V>;

  type Error = V::Error;

  #[inline]
  fn encoded_len(&self) -> usize {
    1 + match self.0 {
      Among::Left(_) => 0,
      Among::Middle(ref value) => value.encoded_len(),
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
      Among::Middle(ref value) => {
        buf[0] = 1;
        value.encode(&mut buf[1..]).map(|len| 1 + len)
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
}

/// The generic value reference to the value log.
pub struct GenericValueRef<'a, V: ?Sized + Type> {
  value: Among<(), V::Ref<'a>, Pointer>,
}

impl<'a, V> core::fmt::Debug for GenericValueRef<'a, V>
where
  V: ?Sized + Type,
  V::Ref<'a>: core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match &self.value {
      Among::Left(_) => f.debug_tuple("GenericValue").field(&()).finish(),
      Among::Middle(ref value) => f.debug_tuple("GenericValue").field(value).finish(),
      Among::Right(ref pointer) => f.debug_tuple("GenericValue").field(pointer).finish(),
    }
  }
}

impl<'a, V> TypeRef<'a> for GenericValueRef<'a, V>
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
