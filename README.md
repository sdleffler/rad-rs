# rad: High-level Rust library for interfacing with RADOS

This library provides a typesafe and extremely high-level Rust interface to
RADOS, the Reliable Autonomous Distributed Object Store. It uses the raw C
bindings from `ceph-rust`.

# Examples

## Connecting to a cluster

The following shows how to connect to a RADOS cluster, by providing a path to a `ceph.conf` file, a path to the `client.admin` keyring, and requesting to connect with the `admin` user. Several things are notable here: first, the `c!` macro provides a convenient way to create a `CString` from a literal. Second of all, this API bares little resemblance to the bare-metal librados API, but it *is* easy to trace what's happening under the hood: `RadosConnectionBuilder::with_user` or `RadosConnectionBuilder::new` allocates a new `rados_t`. `read_conf_file` calls `rados_conf_read_file`, `conf_set` calls `rados_conf_set`, and `connect` calls `rados_connect`.

```rust
let cluster = RadosConnectionBuilder::with_user(c!("admin")).unwrap()
    .read_conf_file("/etc/ceph.conf").unwrap()
    .conf_set(c!("keyring"), c!("/etc/ceph.client.admin.keyring")).unwrap()
    .connect();
```

The type returned from `.connect()` is a `RadosCluster` handle, which is a wrapper around a `rados_t` which guarantees a `rados_shutdown` on the connection when dropped.

## Writing a file to a cluster

The following example shows how to write a file to a cluster using the `RadosWriter` wrapper, which implements the `Write` trait:

```rust
use std::io::{BufReader, BufWriter};

let cluster = ...;

let pool = cluster.get_pool_context(c!("rbd")).unwrap();
let object = BufReader::new(pool.object(c!("test_file.obj")).unwrap());

let file = File::open("test_file.txt").unwrap();
let reader = BufReader::new(file);

loop {
    let buf = reader.fill_buf().unwrap();
    
    if buf.len() == 0 {
        break;
    }

    object.write(buf);
}

object.flush().unwrap();
```

Reading is similarly simple; see `tests/integration/reader_writer/mod.rs` for more details.

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

This project is licensed under the [Mozilla Public License, version 2.0.](https://www.mozilla.org/en-US/MPL/2.0/).
