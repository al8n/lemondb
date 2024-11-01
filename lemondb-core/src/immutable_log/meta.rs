use skl::Trailer;

#[cfg(feature = "ttl")]
mod ttl;
#[cfg(feature = "ttl")]
pub use ttl::Ttl;

mod plain;
pub use plain::Plain;

bitflags::bitflags! {
  #[derive(Debug, Copy, Clone, Eq, PartialEq)]
  struct Flags: u8 {
    /// The first bit is set to 1 to indicate the value is stored as a pointer.
    const POINTER = 0b0000_0001;
  }
}

pub trait ImmutableMeta: Copy {}
