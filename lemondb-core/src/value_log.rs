// use core::marker::PhantomData;

// use dbutils::checksum::Crc32;
// use valog::sync::ValueLog as VaLog;

// use super::types::{fid::Fid, immutable_meta::Meta};
// use meta::Meta as VMeta;

// // mod generic;
// // pub use generic::ValueLog;

// // mod log;
// // pub use log::ValueLog;

// /// The meta type for an entry in the value log.
// mod meta;

// /// The value log
// struct ValueLogCore<E, C = Crc32> {
//   log: VaLog<Fid, C>,
//   _phantom: PhantomData<E>,
// }

// impl<E, C> core::ops::Deref for ValueLogCore<E, C> {
//   type Target = VaLog<Fid, C>;

//   #[inline]
//   fn deref(&self) -> &Self::Target {
//     &self.log
//   }
// }

// /// Merge two `u32` into a `u64`.
// ///
// /// - high 32 bits: `a`
// /// - low 32 bits: `b`
// #[inline]
// const fn merge_lengths(a: u32, b: u32) -> u64 {
//   (a as u64) << 32 | b as u64
// }

// /// Split a `u64` into two `u32`.
// ///
// /// - high 32 bits: the first `u32`
// /// - low 32 bits: the second `u32`
// #[inline]
// const fn split_lengths(len: u64) -> (u32, u32) {
//   ((len >> 32) as u32, len as u32)
// }
