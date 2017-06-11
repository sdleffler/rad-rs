//! A high-level wrapper library for talking to Ceph RADOS clusters.
//!
//! Only certain features are currently available, mainly writing and reading to
//! RADOS objects, as well as a limited set of other operations (object stats,
//! for example.) However, the operations that are provided are supplied in
//! an idiomatic, Rusty fashion, using the futures library for asynchronous
//! operations.
//!
//! Missing functionality includes MON/OSD/PGS commands.
//!
//! Current features:
//! - Read, write, full-write, append
//! - Automatic `rados_shutdown` on drop of the `RadosCluster` type
//! - Asynchronous read/write/etc. using futures
//! - Synchronous read and asynchronous write using the `RadosObject` wrapper,
//!   which provides `Read`, `Write`, and `Seek` operations in a manner similar
//!   to files
//!
//! Planned features:
//! - Fully asynchronous read/write using `tokio_io::{AsyncRead, AsyncWrite}`
//! - Implementations of more futures traits (`Stream`, `Sink`)

#![recursion_limit = "1024"]

extern crate ceph_rust;
extern crate chrono;
#[macro_use]
extern crate error_chain;
extern crate futures;
extern crate libc;


#[macro_export]
macro_rules! c {
    ($s:expr) => {
        CString::new($s)
            .expect(concat!("Could not convert `", $s, "` to an FFI-compatible CString!"))
    };
}


pub mod async;
pub mod errors;
pub mod rados;
pub mod stream;

pub use errors::*;
pub use rados::{RadosConnectionBuilder, RadosCluster, RadosContext, RadosStat};
