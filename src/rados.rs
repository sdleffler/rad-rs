//! Wrappers for Ceph cluster connections, Ceph I/O contexts, and associated operations.
//!
//! ## Connection building example
//!
//! The following example connects to a Ceph cluster using the `ceph.conf` file at
//! `/etc/ceph/ceph.conf`, as the `admin` user, and setting the RADOS config `keyring`
//! option to the value of `/etc/ceph/ceph.client.admin.keyring` (which is necessary
//! for connecting to Ceph using config files/keyrings located on nonstandard paths.)
//!
//! ```rust,no_run
//! # extern crate rad;
//! # fn dummy() -> ::rad::errors::Result<()> {
//! use std::path::Path;
//! use rad::ConnectionBuilder;
//!
//! let mut cluster = ConnectionBuilder::with_user("admin")?
//!                   .read_conf_file(Path::new("/etc/ceph/ceph.conf"))?
//!                   .conf_set("keyring", "/etc/ceph/ceph.client.admin.keyring")?
//!                   .connect()?;
//! # Ok(()) } fn main() {}
//! ```

use std::ops::DerefMut;
use std::mem;
use std::path::Path;
use std::ptr;
use std::result::Result as StdResult;
use std::sync::Arc;

use ceph::rados::{self, rados_completion_t, rados_ioctx_t, rados_t,
                       Struct_rados_cluster_stat_t};
use chrono::{DateTime, Local, TimeZone};
use ffi_pool::CStringPool;
use futures::prelude::*;
use libc;
use stable_deref_trait::StableDeref;

use async::Completion;
use errors::{self, Error, ErrorKind, Result};

lazy_static! {
    /// A pool of `CString`s used for converting Rust strings which need to be passed into
    /// librados.
    static ref POOL: CStringPool = CStringPool::new(128);
}

/// A wrapper around a `rados_t` providing methods for configuring the connection before finalizing
/// it.
pub struct ConnectionBuilder {
    handle: rados_t,
}

impl ConnectionBuilder {
    /// Start building a new connection. By default the client to connect as is `client.admin`.
    pub fn new() -> Result<ConnectionBuilder> {
        let mut handle = ptr::null_mut();

        errors::librados(unsafe { rados::rados_create(&mut handle, ptr::null()) })?;

        Ok(ConnectionBuilder { handle })
    }

    /// Start building a new connection with a specified user.
    pub fn with_user(user: &str) -> Result<ConnectionBuilder> {
        let user_cstr = POOL.get_str(user)?;
        let mut handle = ptr::null_mut();

        errors::librados(unsafe { rados::rados_create(&mut handle, user_cstr.as_ptr()) })?;

        mem::drop(user_cstr);

        Ok(ConnectionBuilder { handle })
    }

    /// Read a configuration file from a given path.
    pub fn read_conf_file(self, path: &Path) -> Result<ConnectionBuilder> {
        let path_cstr = POOL.get_str(&path.to_string_lossy())?;

        errors::librados(unsafe { rados::rados_conf_read_file(self.handle, path_cstr.as_ptr()) })?;

        Ok(self)
    }

    /// Set an individual configuration option. Useful options include `keyring` if you are trying
    /// to set up Ceph without storing everything inside `/etc/ceph`.
    pub fn conf_set(self, option: &str, value: &str) -> Result<ConnectionBuilder> {
        let option_cstr = POOL.get_str(option)?;
        let value_cstr = POOL.get_str(value)?;

        errors::librados(unsafe {
            rados::rados_conf_set(self.handle, option_cstr.as_ptr(), value_cstr.as_ptr())
        })?;

        Ok(self)
    }

    /// Finish building the connection configuration and connect to the cluster.
    pub fn connect(self) -> Result<Connection> {
        errors::librados(unsafe { rados::rados_connect(self.handle) })?;

        Ok(Connection {
            _dummy: ptr::null(),
            conn: Arc::new(ClusterHandle {
                handle: self.handle,
            }),
        })
    }
}

/// Statistics for a Ceph cluster: total storage in kibibytes, the amount of storage used in
/// kibibytes, the amount of available storage in kibibytes, and the number of stored objects.
///
/// Note that yes, these are [kibibytes](https://en.wikipedia.org/wiki/Kibibyte), not
/// [kilobytes](https://en.wikipedia.org/wiki/Kilobyte)! Kilo/mega/giga/terabytes are measured in
/// powers of ten, so a kilobyte is literally a thousand bytes, a megabyte is a million bytes, and
/// so forth. A kibibyte, however, is `2^10` bytes, a mebibyte is `2^20` bytes, and so on.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClusterStat {
    pub kb: u64,
    pub kb_used: u64,
    pub kb_avail: u64,
    pub num_objects: u64,
}

/// A wrapper over a `rados_t` which is guaranteed to have successfully connected to a
/// cluster.
///
/// On drop, `rados_shutdown` is called on the wrapped `rados_t`.
#[derive(Clone)]
struct ClusterHandle {
    handle: rados_t,
}

impl Drop for ClusterHandle {
    fn drop(&mut self) {
        unsafe {
            rados::rados_shutdown(self.handle);
        }
    }
}

/// A wrapper over a connection to a Ceph cluster.
pub struct Connection {
    // Since opt-in builtin traits are not yet stable, use of a dummy pointer will prevent
    // `Connection` from being `Sync`.
    _dummy: *const (),

    // Although `Connection` isn't cloneable, since we still need to reference-count the
    // underlying cluster connection handle, we use an `Arc` and on creation of I/O contexts we
    // clone the `Arc` and give a reference to each I/O context. This allows us to `rados_shutdown`
    // the cluster once all references are dropped.
    conn: Arc<ClusterHandle>,
}

// OIBITs not yet stable.
//
unsafe impl Send for Connection {}
// impl !Sync for Connection {}

impl Connection {
    /// Fetch the stats of the entire cluster, using `rados_cluster_stat`.
    pub fn stat(&mut self) -> Result<ClusterStat> {
        let mut cluster_stat = Struct_rados_cluster_stat_t {
            kb: 0,
            kb_used: 0,
            kb_avail: 0,
            num_objects: 0,
        };

        errors::librados(unsafe {
            rados::rados_cluster_stat(self.conn.handle, &mut cluster_stat)
        })?;

        Ok(ClusterStat {
            kb: cluster_stat.kb,
            kb_used: cluster_stat.kb_used,
            kb_avail: cluster_stat.kb_avail,
            num_objects: cluster_stat.num_objects,
        })
    }

    /// Fetch the `rados_ioctx_t` for the relevant pool, using `rados_ioctx_create`.
    pub fn get_pool_context(&mut self, pool_name: &str) -> Result<Context> {
        let pool_name_cstr = POOL.get_str(pool_name)?;
        let mut ioctx_handle = ptr::null_mut();

        errors::librados(unsafe {
            rados::rados_ioctx_create(self.conn.handle, pool_name_cstr.as_ptr(), &mut ioctx_handle)
        })?;

        Ok(Context {
            _conn: self.conn.clone(),
            handle: ioctx_handle,
        })
    }

    /// Fetch the `rados_ioctx_t` for the relevant pool, using `rados_ioctx_create2`.
    pub fn get_pool_context_from_id(&mut self, pool_id: u64) -> Result<Context> {
        let mut ioctx_handle = ptr::null_mut();

        errors::librados(unsafe {
            rados::rados_ioctx_create2(
                self.conn.handle,
                *(&pool_id as *const u64 as *const i64),
                &mut ioctx_handle,
            )
        })?;

        Ok(Context {
            _conn: self.conn.clone(),
            handle: ioctx_handle,
        })
    }
}

/// The type of a RADOS AIO operation which has yet to complete.
#[derive(Debug)]
pub struct UnitFuture {
    completion_res: StdResult<Completion<()>, Option<Error>>,
}

impl UnitFuture {
    fn new<F>(init: F) -> UnitFuture
    where
        F: FnOnce(rados_completion_t) -> Result<()>,
    {
        UnitFuture {
            completion_res: Completion::new((), init).map_err(Some),
        }
    }
}

impl Future for UnitFuture {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.completion_res.as_mut() {
            Ok(completion) => completion.poll().map(|async| async.map(|_| ())),
            Err(error) => Err(error.take().unwrap()),
        }
    }
}

#[derive(Debug)]
pub struct DataFuture<T> {
    completion_res: StdResult<Completion<T>, Option<Error>>,
}

impl<T> DataFuture<T> {
    fn new<F>(data: T, init: F) -> DataFuture<T>
    where
        F: FnOnce(rados_completion_t) -> Result<()>,
    {
        DataFuture {
            completion_res: Completion::new(data, init).map_err(Some),
        }
    }
}

impl<T> Future for DataFuture<T> {
    type Item = T;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.completion_res.as_mut() {
            Ok(completion) => completion.poll().map(|async| async.map(|ret| ret.data)),
            Err(error) => Err(error.take().unwrap()),
        }
    }
}

pub struct ReadFuture<B>
where
    B: StableDeref + DerefMut<Target = [u8]>,
{
    completion_res: StdResult<Completion<B>, Option<Error>>,
}

impl<B> ReadFuture<B>
where
    B: StableDeref + DerefMut<Target = [u8]>,
{
    fn new<F>(buf: B, init: F) -> Self
    where
        F: FnOnce(rados_completion_t) -> Result<()>,
    {
        ReadFuture {
            completion_res: Completion::new(buf, init).map_err(Some),
        }
    }
}

impl<B> Future for ReadFuture<B>
where
    B: StableDeref + DerefMut<Target = [u8]>,
{
    type Item = (u32, B);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.completion_res.as_mut() {
            Ok(completion) => completion
                .poll()
                .map(|async| async.map(|ret| (ret.value, ret.data))),
            Err(error) => Err(error.take().unwrap()),
        }
    }
}

pub struct StatFuture {
    data_future: DataFuture<Box<(u64, libc::time_t)>>,
}

impl Future for StatFuture {
    type Item = Stat;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.data_future.poll().map(|async| {
            async.map(|boxed| {
                let (size, last_modified) = *boxed;

                Stat {
                    size,
                    last_modified: Local.timestamp(last_modified, 0),
                }
            })
        })
    }
}

pub struct ExistsFuture {
    unit_future: UnitFuture,
}

impl Future for ExistsFuture {
    type Item = bool;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.unit_future.poll() {
            Ok(Async::Ready(())) => Ok(Async::Ready(true)),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(Error(ErrorKind::Rados(err_code), _)) if err_code == libc::ENOENT as u32 => {
                Ok(Async::Ready(false))
            }
            Err(err) => Err(err),
        }
    }
}

/// Statistics for a single RADOS object.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Stat {
    pub size: u64,
    pub last_modified: DateTime<Local>,
}

/// A wrapper around a `rados_ioctx_t`, which also counts as a reference to the underlying
/// `Connection`.
pub struct Context {
    _conn: Arc<ClusterHandle>,
    handle: rados_ioctx_t,
}

// `Context` is safe to `Send`, but not `Sync`; this is because nothing about the
// `rados_ioctx_t` specifically ties it to the current thread. Thus it is safe to `Send`. However,
// it is good practice [citation needed] to not use the same `rados_ioctx_t` from different
// threads; hence `Send` but not `Clone` nor `Sync`.
//
// The #ceph-devel IRC channel on OFTC has also confirmed that access to `rados_ioctx_t` should be
// synchronized, but it is safe to send across threads.
unsafe impl Send for Context {}
// !impl Sync for Context {}

impl Drop for Context {
    fn drop(&mut self) {
        unsafe {
            rados::rados_ioctx_destroy(self.handle);
        }
    }
}

impl Context {
    /// Fetch an extended attribute on a given RADOS object using `rados_getxattr`.
    ///
    /// * `size` - the size in bytes of the extended attribute.
    pub fn get_xattr(&mut self, obj: &str, key: &str, size: usize) -> Result<Vec<u8>> {
        let obj_cstr = POOL.get_str(obj)?;
        let key_cstr = POOL.get_str(key)?;

        let mut buf = vec![0u8; size];

        errors::librados(unsafe {
            rados::rados_getxattr(
                self.handle,
                obj_cstr.as_ptr(),
                key_cstr.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len(),
            )
        })?;

        mem::drop(obj_cstr);
        mem::drop(key_cstr);

        Ok(buf)
    }

    /// Set an extended attribute on a given RADOS object using `rados_setxattr`.
    pub fn set_xattr(&mut self, obj: &str, key: &str, value: &[u8]) -> Result<()> {
        let obj_cstr = POOL.get_str(obj)?;
        let key_cstr = POOL.get_str(key)?;

        errors::librados(unsafe {
            rados::rados_setxattr(
                self.handle,
                obj_cstr.as_ptr(),
                key_cstr.as_ptr(),
                value.as_ptr() as *const libc::c_char,
                value.len(),
            )
        })?;

        mem::drop(obj_cstr);
        mem::drop(key_cstr);

        Ok(())
    }

    /// Write to a RADOS object using `rados_write`.
    pub fn write(&mut self, obj: &str, buf: &[u8], offset: u64) -> Result<()> {
        let object_id = POOL.get_str(obj)?;

        errors::librados(unsafe {
            rados::rados_write(
                self.handle,
                object_id.as_ptr(),
                buf.as_ptr() as *const libc::c_char,
                buf.len(),
                offset,
            )
        })?;

        mem::drop(object_id);

        Ok(())
    }

    /// Write the entirety of a RADOS object, overwriting if necessary, using `rados_write_full`.
    pub fn write_full(&mut self, obj: &str, buf: &[u8]) -> Result<()> {
        let object_id = POOL.get_str(obj)?;

        errors::librados(unsafe {
            rados::rados_write_full(
                self.handle,
                object_id.as_ptr(),
                buf.as_ptr() as *const libc::c_char,
                buf.len(),
            )
        })?;

        mem::drop(object_id);

        Ok(())
    }

    /// Append to a RADOS object using `rados_append`.
    pub fn append(&mut self, obj: &str, buf: &[u8]) -> Result<()> {
        let object_id = POOL.get_str(obj)?;

        errors::librados(unsafe {
            rados::rados_append(
                self.handle,
                object_id.as_ptr(),
                buf.as_ptr() as *const libc::c_char,
                buf.len(),
            )
        })?;

        mem::drop(object_id);

        Ok(())
    }

    /// Read from a RADOS object using `rados_read`.
    pub fn read(&mut self, obj: &str, buf: &mut [u8], offset: u64) -> Result<usize> {
        let object_id = POOL.get_str(obj)?;

        let read = errors::librados_res(unsafe {
            rados::rados_read(
                self.handle,
                object_id.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len(),
                offset,
            )
        })?;

        mem::drop(object_id);

        Ok(read as usize)
    }

    /// Delete a RADOS object using `rados_remove`.
    pub fn remove(&mut self, obj: &str) -> Result<()> {
        let object_id = POOL.get_str(obj)?;

        errors::librados(unsafe { rados::rados_remove(self.handle, object_id.as_ptr()) })?;

        mem::drop(object_id);

        Ok(())
    }

    /// Resize a RADOS object, filling with zeroes if necessary, using `rados_trunc`.
    pub fn resize(&mut self, obj: &str, size: u64) -> Result<()> {
        let object_id = POOL.get_str(obj)?;

        errors::librados(unsafe { rados::rados_trunc(self.handle, object_id.as_ptr(), size) })?;

        mem::drop(object_id);

        Ok(())
    }

    /// Get the statistics of a given RADOS object using `rados_stat`.
    pub fn stat(&mut self, obj: &str) -> Result<Stat> {
        let object_id = POOL.get_str(obj)?;

        let mut size = 0;
        let mut time = 0;

        errors::librados(unsafe {
            rados::rados_stat(self.handle, object_id.as_ptr(), &mut size, &mut time)
        })?;

        mem::drop(object_id);

        Ok(Stat {
            size,
            last_modified: Local.timestamp(time, 0),
        })
    }

    /// Asynchronously write to a RADOS object using `rados_aio_write`.
    pub fn write_async(&mut self, obj: &str, buf: &[u8], offset: u64) -> UnitFuture {
        UnitFuture::new(|completion_handle| {
            let object_id = POOL.get_str(obj)?;

            errors::librados({
                unsafe {
                    rados::rados_aio_write(
                        self.handle,
                        object_id.as_ptr(),
                        completion_handle,
                        buf.as_ptr() as *const libc::c_char,
                        buf.len(),
                        offset,
                    )
                }
            })?;

            mem::drop(object_id);

            Ok(())
        })
    }

    /// Asynchronously append to a RADOS object using `rados_aio_append`.
    pub fn append_async(&mut self, obj: &str, buf: &[u8]) -> UnitFuture {
        UnitFuture::new(|completion_handle| {
            let object_id = POOL.get_str(obj)?;

            errors::librados({
                unsafe {
                    rados::rados_aio_append(
                        self.handle,
                        object_id.as_ptr(),
                        completion_handle,
                        buf.as_ptr() as *const libc::c_char,
                        buf.len(),
                    )
                }
            })?;

            mem::drop(object_id);

            Ok(())
        })
    }

    /// Asynchronously set the contents of a RADOS object using `rados_aio_write_full`.
    pub fn write_full_async(&mut self, obj: &str, buf: &[u8]) -> UnitFuture {
        UnitFuture::new(|completion_handle| {
            let object_id = POOL.get_str(obj)?;

            errors::librados({
                unsafe {
                    rados::rados_aio_write_full(
                        self.handle,
                        object_id.as_ptr(),
                        completion_handle,
                        buf.as_ptr() as *const libc::c_char,
                        buf.len(),
                    )
                }
            })?;

            mem::drop(object_id);

            Ok(())
        })
    }

    /// Asynchronously remove a RADOS object from the cluster using `rados_aio_remove`.
    pub fn remove_async(&mut self, obj: &str) -> UnitFuture {
        UnitFuture::new(|completion_handle| {
            let object_id = POOL.get_str(obj)?;

            errors::librados({
                unsafe {
                    rados::rados_aio_remove(self.handle, object_id.as_ptr(), completion_handle)
                }
            })?;

            mem::drop(object_id);

            Ok(())
        })
    }

    /// Asynchronously read from a RADOS object using `rados_aio_read`.
    pub fn read_async<B>(&mut self, obj: &str, mut buf: B, offset: u64) -> ReadFuture<B>
    where
        B: StableDeref + DerefMut<Target = [u8]>,
    {
        let buf_ptr = buf.as_mut_ptr() as *mut libc::c_char;
        let buf_len = buf.len();

        ReadFuture::new(buf, |completion_handle| {
            let object_id = POOL.get_str(obj)?;

            errors::librados(unsafe {
                rados::rados_aio_read(
                    self.handle,
                    object_id.as_ptr(),
                    completion_handle,
                    buf_ptr,
                    buf_len,
                    offset,
                )
            })?;

            mem::drop(object_id);

            Ok(())
        })
    }

    /// Asynchronously retrieve statistics of a specific object from the cluster using
    /// `rados_aio_stat`.
    pub fn stat_async(&mut self, obj: &str) -> StatFuture {
        let mut boxed = Box::new((0, 0));
        let size_ptr = &mut boxed.0 as *mut u64;
        let time_ptr = &mut boxed.1 as *mut libc::time_t;

        let data_future = DataFuture::new(boxed, |completion_handle| {
            let object_id = POOL.get_str(obj)?;

            errors::librados(unsafe {
                rados::rados_aio_stat(
                    self.handle,
                    object_id.as_ptr(),
                    completion_handle,
                    size_ptr,
                    time_ptr,
                )
            })?;

            mem::drop(object_id);

            Ok(())
        });

        StatFuture { data_future }
    }

    /// Check whether or not a RADOS object exists under a given name, using `rados_stat` and
    /// checking the error code for `ENOENT`.
    pub fn exists(&mut self, obj: &str) -> Result<bool> {
        let object_id = POOL.get_str(obj)?;

        let result = errors::librados(unsafe {
            rados::rados_stat(
                self.handle,
                object_id.as_ptr(),
                ptr::null_mut(),
                ptr::null_mut(),
            )
        });

        match result {
            Ok(()) => Ok(true),
            Err(Error(ErrorKind::Rados(err_code), _)) if err_code == libc::ENOENT as u32 => {
                Ok(false)
            }
            Err(err) => Err(err),
        }
    }

    /// Asynchronously check for object existence by using `rados_aio_stat` and checking for
    /// `ENOENT`.
    pub fn exists_async(&mut self, obj: &str) -> ExistsFuture {
        let unit_future = UnitFuture::new(|completion_handle| {
            let object_id = POOL.get_str(obj)?;

            errors::librados(unsafe {
                rados::rados_aio_stat(
                    self.handle,
                    object_id.as_ptr(),
                    completion_handle,
                    ptr::null_mut(),
                    ptr::null_mut(),
                )
            })?;

            mem::drop(object_id);

            Ok(())
        });

        ExistsFuture { unit_future }
    }

    /// Flush all asynchronous I/O actions on the given context, blocking until they are complete.
    pub fn flush(&mut self) -> Result<()> {
        // BUG: `rados_aio_flush` always returns 0
        // http://docs.ceph.com/docs/master/rados/api/librados/#rados_aio_flush
        //
        // This function returns a `Result` because in the future `rados_aio_flush`
        // may change to return an error code.

        errors::librados(unsafe { rados::rados_aio_flush(self.handle) })
    }

    /// Construct a future which will complete when all I/O actions on the given context are
    /// complete.
    pub fn flush_async(&mut self) -> UnitFuture {
        UnitFuture::new(|completion_handle| {
            errors::librados(unsafe {
                rados::rados_aio_flush_async(self.handle, completion_handle)
            })
        })
    }
}
