#![recursion_limit = "1024"]

extern crate ceph_rust;
extern crate chrono;
#[macro_use]
extern crate error_chain;
extern crate futures;
extern crate libc;


macro_rules! c {
    ($s:expr) => {
        CString::new($s)
            .expect(concat!("Could not convert `", $s, "` to an FFI-compatible CString!"))
    };
}


pub mod async;
pub mod errors;
pub mod rados;

pub use errors::*;
pub use rados::{RadosConnectionBuilder, RadosCluster, RadosContext, RadosStat};
