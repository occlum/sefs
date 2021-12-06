#![feature(vec_spare_capacity)]
#![cfg_attr(not(any(test, feature = "std")), no_std)]

#[macro_use]
extern crate bitflags;
extern crate alloc;

pub mod dev;
pub mod dirty;
pub mod file;
pub mod util;
pub mod vfs;

#[cfg(any(test, feature = "std"))]
mod std;
