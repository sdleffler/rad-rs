//! # `rad-rs` - A high-level wrapper library for talking to Ceph RADOS clusters.
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
//! - Automatic `rados_shutdown` on drop of the reference-counted `RadosCluster` type
//! - Asynchronous read/write/etc. using futures
//!
//! ## Examples
//!
//! ### Connecting to a cluster
//!
//! ```rust,no_run
//! # extern crate rad;
//! use std::path::Path;
//!
//! use rad::ConnectionBuilder;
//! # fn dummy() -> ::rad::errors::Result<()> {
//!
//! let cluster = ConnectionBuilder::with_user("admin")?
//!     .read_conf_file(Path::new("/etc/ceph.conf"))?
//!     .conf_set("keyring", "/etc/ceph.client.admin.keyring")?
//!     .connect()?;
//! # Ok(()) } fn main() {}
//! ```
//!
//! ### Synchronous cluster operations
//!
//! ```rust,no_run
//! # extern crate rad;
//! # fn dummy() -> ::rad::errors::Result<()> {
//! use std::fs::File;
//! use std::io::Read;
//! use std::path::Path;
//!
//! use rad::ConnectionBuilder;
//!
//! let mut cluster = ConnectionBuilder::with_user("admin")?
//!     .read_conf_file(Path::new("/etc/ceph.conf"))?
//!     .conf_set("keyring", "/etc/ceph.client.admin.keyring")?
//!     .connect()?;
//!
//! // Read in bytes from some file to send to the cluster.
//! let mut file = File::open("/path/to/file")?;
//! let mut bytes = Vec::new();
//! file.read_to_end(&mut bytes)?;
//!
//! let mut pool = cluster.get_pool_context("rbd")?;
//!
//! pool.write_full("object-name", &bytes)?;
//!
//! // Our file is now in the cluster! We can check for its existence:
//! assert!(pool.exists("object-name")?);
//!
//! // And we can also check that it contains the bytes we wrote to it.
//! let mut bytes_from_cluster = vec![0u8; bytes.len()];
//! let bytes_read = pool.read("object-name", &mut bytes_from_cluster, 0)?;
//! assert_eq!(bytes_read, bytes_from_cluster.len());
//! assert!(bytes_from_cluster == bytes);
//! # Ok(()) } fn main() {}
//! ```
//!
//! ### Asynchronous cluster I/O
//!
//! ```rust,no_run
//! # extern crate futures;
//! # extern crate rad;
//! # extern crate rand;
//! # fn dummy() -> ::rad::errors::Result<()> {
//! use std::path::Path;
//! use std::io::Read;
//!
//! use futures::prelude::*;
//! use futures::stream;
//! use rand::{Rng, SeedableRng, XorShiftRng};
//!
//! use rad::ConnectionBuilder;
//!
//! const NUM_OBJECTS: usize = 8;
//!
//! let mut cluster = ConnectionBuilder::with_user("admin")?
//!     .read_conf_file(Path::new("/etc/ceph.conf"))?
//!     .conf_set("keyring", "/etc/ceph.client.admin.keyring")?
//!     .connect()?;
//!
//! let mut pool = cluster.get_pool_context("rbd")?;
//!
//! stream::iter_ok((0..NUM_OBJECTS)
//!     .map(|i| {
//!         let bytes = XorShiftRng::from_seed([i as u32 + 1, 2, 3, 4])
//!             .gen_iter::<u8>()
//!             .take(1 << 16)
//!             .collect::<Vec<u8>>();
//!
//!         let name = format!("object-{}", i);
//!
//!         pool.write_full_async(&name, &bytes)
//!     }))
//!     .buffer_unordered(NUM_OBJECTS)
//!     .collect()
//!     .wait()?;
//! # Ok(()) } fn main() {}
//! ```

#![recursion_limit = "1024"]

extern crate ceph_rust;
extern crate chrono;
#[macro_use]
extern crate error_chain;
extern crate ffi_pool;
extern crate futures;
#[macro_use]
extern crate lazy_static;
extern crate libc;
extern crate stable_deref_trait;


pub use stable_deref_trait::StableDeref;


#[macro_export]
macro_rules! c {
    ($s:expr) => {
        CString::new($s)
            .expect(concat!("Could not convert `", $s, "` to an FFI-compatible CString!"))
    };
}


mod async;
pub mod errors;
pub mod rados;

pub use errors::*;
pub use rados::{ConnectionBuilder, Connection, Context, Stat};
