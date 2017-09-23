[![Docs Status](https://docs.rs/rad/badge.svg)](https://docs.rs/rad)
[![On crates.io](https://img.shields.io/crates/v/rad.svg)](https://crates.io/crates/rad)

# rad: High-level Rust library for interfacing with RADOS

This library provides a typesafe and extremely high-level Rust interface to
RADOS, the Reliable Autonomous Distributed Object Store. It uses the raw C
bindings from `ceph-rust`.

# Examples

## Connecting to a cluster

The following shows how to connect to a RADOS cluster, by providing a path to a
`ceph.conf` file, a path to the `client.admin` keyring, and requesting to
connect with the `admin` user. This API bares little resemblance to the
bare-metal librados API, but it *is* easy to trace what's happening under the
hood: `RadosConnectionBuilder::with_user` or `RadosConnectionBuilder::new`
allocates a new `rados_t`. `read_conf_file` calls `rados_conf_read_file`,
`conf_set` calls `rados_conf_set`, and `connect` calls `rados_connect`.

```rust
use rad::RadosConnectionBuilder;

let cluster = RadosConnectionBuilder::with_user("admin").unwrap()
    .read_conf_file("/etc/ceph.conf").unwrap()
    .conf_set("keyring", "/etc/ceph.client.admin.keyring").unwrap()
    .connect()?;
```

The type returned from `.connect()` is a `RadosCluster` handle, which is a wrapper around a `rados_t` which guarantees a `rados_shutdown` on the connection when dropped.

## Writing a file to a cluster with synchronous I/O

```rust
use std::fs::File;
use std::io::Read;

use rad::RadosConnectionBuilder;

let cluster = RadosConnectionBuilder::with_user("admin")?
    .read_conf_file("/etc/ceph.conf")?
    .conf_set("keyring", "/etc/ceph.client.admin.keyring")?
    .connect()?;

// Read in bytes from some file to send to the cluster.
let file = File::open("/path/to/file")?;
let mut bytes = Vec::new();
file.read_to_end(&mut bytes)?;

let pool = cluster.get_pool_context("rbd")?;

pool.write_full("object-name", &bytes)?;

// Our file is now in the cluster! We can check for its existence:
assert!(pool.exists("object-name")?);

// And we can also check that it contains the bytes we wrote to it.
let mut bytes_from_cluster = vec![0u8; bytes.len()];
let bytes_read = pool.read("object-name", &mut bytes_from_cluster, 0)?;
assert_eq!(bytes_read, bytes_from_cluster.len());
assert!(bytes_from_cluster == bytes);
```

## Writing multiple objects to a cluster with asynchronous I/O and `futures-rs`

`rad-rs` also supports the librados AIO interface, using the `futures` crate.
This example will start `NUM_OBJECTS` writes concurrently and then wait for
them all to finish.

```rust
use std::fs::File;
use std::io::Read;

use rand::{Rng, SeedableRng, XorShiftRng};

use rad::RadosConnectionBuilder;

const NUM_OBJECTS: usize = 8;

let cluster = RadosConnectionBuilder::with_user("admin")?
    .read_conf_file("/etc/ceph.conf")?
    .conf_set("keyring", "/etc/ceph.client.admin.keyring")?
    .connect()?;

let pool = cluster.get_pool_context("rbd")?;

stream::iter_ok((0..NUM_OBJECTS)
    .map(|i| {
        let bytes = XorShiftRng::from_seed([i as u32 + 1, 2, 3, 4])
            .gen_iter::<u8>()
            .take(1 << 16).collect();

        let name = format!("object-{}", i);

        pool.write_full_async(name, &bytes)
    }))
    .buffer_unordered(NUM_OBJECTS)
    .collect()
    .wait()?;
```

# Running tests

Integration tests against a demo cluster are provided, and the test suite
(which is admittedly a little bare at the moment) uses Docker and a container
derived from the Ceph `ceph/demo` container to bring a small Ceph cluster
online, locally. A script is provided for launching the test suite:

```sh
./tests/run-all-tests.sh
```

Launching the test suite requires Docker to be installed.

# License

This project is licensed under the [Mozilla Public License, version 2.0.](https://www.mozilla.org/en-US/MPL/2.0/)
