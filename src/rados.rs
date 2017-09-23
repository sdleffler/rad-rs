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
//! use rad::RadosConnectionBuilder;
//!
//! let mut cluster = RadosConnectionBuilder::with_user("admin")?
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

use ceph_rust::rados::{self, rados_t, rados_ioctx_t, rados_completion_t,
                       Struct_rados_cluster_stat_t};
use chrono::{DateTime, Local, TimeZone};
use ffi_pool::CStringPool;
use futures::prelude::*;
use futures::future::{Map, Then};
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
pub struct RadosConnectionBuilder {
    handle: rados_t,
}


impl RadosConnectionBuilder {
    /// Start building a new connection. By default the client to connect as is `client.admin`.
    pub fn new() -> Result<RadosConnectionBuilder> {
        let mut handle = ptr::null_mut();

        errors::librados(unsafe { rados::rados_create(&mut handle, ptr::null()) })?;

        Ok(RadosConnectionBuilder { handle })
    }


    /// Start building a new connection with a specified user.
    pub fn with_user(user: &str) -> Result<RadosConnectionBuilder> {
        let user_cstr = POOL.get_str(user)?;
        let mut handle = ptr::null_mut();

        errors::librados(unsafe {
            rados::rados_create(&mut handle, user_cstr.as_ptr())
        })?;

        mem::drop(user_cstr);

        Ok(RadosConnectionBuilder { handle })
    }


    /// Read a configuration file from a given path.
    pub fn read_conf_file(self, path: &Path) -> Result<RadosConnectionBuilder> {
        let path_cstr = POOL.get_str(&path.to_string_lossy())?;

        errors::librados(unsafe {
            rados::rados_conf_read_file(self.handle, path_cstr.as_ptr())
        })?;

        Ok(self)
    }


    /// Set an individual configuration option. Useful options include `keyring` if you are trying
    /// to set up Ceph without storing everything inside `/etc/ceph`.
    pub fn conf_set(self, option: &str, value: &str) -> Result<RadosConnectionBuilder> {
        let option_cstr = POOL.get_str(option)?;
        let value_cstr = POOL.get_str(value)?;

        errors::librados(unsafe {
            rados::rados_conf_set(self.handle, option_cstr.as_ptr(), value_cstr.as_ptr())
        })?;

        Ok(self)
    }


    /// Finish building the connection configuration and connect to the cluster.
    pub fn connect(self) -> Result<RadosConnection> {
        errors::librados(unsafe { rados::rados_connect(self.handle) })?;

        Ok(RadosConnection {
            _dummy: ptr::null(),
            conn: Arc::new(RadosHandle { handle: self.handle }),
        })
    }
}


/// Statistics for a Ceph cluster: total storage in kilobytes, the amount of storage used in
/// kilobytes, the amount of available storage in kilobytes, and the number of stored objects.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct RadosClusterStat {
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
struct RadosHandle {
    handle: rados_t,
}


impl Drop for RadosHandle {
    fn drop(&mut self) {
        unsafe {
            rados::rados_shutdown(self.handle);
        }
    }
}


// `Send` and `Sync` are unsafely implemented for `RadosHandle` because it is only ever *used* in a
// thread-safe manner, and needs to be able to be sent across threads. The rationale for this is as
// follows: we wish to (atomically) reference-count the handle for the connection to the Ceph
// cluster, so that we can call `rados_shutdown` on it as necessary. RADOS I/O contexts count as
// references against the underlying connection, because if the underlying connection is shut down,
// the I/O contexts become invalid [citation needed]. As such, we give I/O contexts
// `Arc<RadosHandle>`s, which are never actually accessed but instead only used for the reference
// count. The actual `RadosConnection` struct - which *does* call functions on the underlying
// `RadosHandle` and `rados_t` - is neither `Clone` nor `Sync`. Thus, since the underlying
// `rados_t` will only ever be accessed single-threadedly (as it is *never* accessed through the
// references kept in I/O contexts) it is completely safe to give it `Send` and `Sync`
// capabilities.
unsafe impl Send for RadosHandle {}
unsafe impl Sync for RadosHandle {}


/// A wrapper over a connection to a Ceph cluster.
pub struct RadosConnection {
    // Since opt-in builtin traits are not yet stable, use of a dummy pointer will prevent
    // `RadosConnection` from being `Sync`.
    _dummy: *const (),

    // Although `RadosConnection` isn't cloneable, since we still need to reference-count the
    // underlying cluster connection handle, we use an `Arc` and on creation of I/O contexts we
    // clone the `Arc` and give a reference to each I/O context. This allows us to `rados_shutdown`
    // the cluster once all references are dropped.
    conn: Arc<RadosHandle>,
}


// OIBITs not yet stable.
//
unsafe impl Send for RadosConnection {}
// impl !Sync for RadosConnection {}


impl RadosConnection {
    /// Fetch the stats of the entire cluster, using `rados_cluster_stat`.
    pub fn stat(&mut self) -> Result<RadosClusterStat> {
        let mut cluster_stat = Struct_rados_cluster_stat_t {
            kb: 0,
            kb_used: 0,
            kb_avail: 0,
            num_objects: 0,
        };

        errors::librados(unsafe {
            rados::rados_cluster_stat(self.conn.handle, &mut cluster_stat)
        })?;

        Ok(RadosClusterStat {
            kb: cluster_stat.kb,
            kb_used: cluster_stat.kb_used,
            kb_avail: cluster_stat.kb_avail,
            num_objects: cluster_stat.num_objects,
        })
    }


    /// Fetch the `rados_ioctx_t` for the relevant pool, using `rados_ioctx_create`.
    pub fn get_pool_context(&mut self, pool_name: &str) -> Result<RadosContext> {
        let pool_name_cstr = POOL.get_str(pool_name)?;
        let mut ioctx_handle = ptr::null_mut();

        errors::librados(unsafe {
            rados::rados_ioctx_create(self.conn.handle, pool_name_cstr.as_ptr(), &mut ioctx_handle)
        })?;

        Ok(RadosContext {
            _conn: self.conn.clone(),
            handle: ioctx_handle,
        })
    }


    /// Fetch the `rados_ioctx_t` for the relevant pool, using `rados_ioctx_create2`.
    pub fn get_pool_context_from_id(&mut self, pool_id: u64) -> Result<RadosContext> {
        let mut ioctx_handle = ptr::null_mut();

        errors::librados(unsafe {
            rados::rados_ioctx_create2(
                self.conn.handle,
                *(&pool_id as *const u64 as *const i64),
                &mut ioctx_handle,
            )
        })?;

        Ok(RadosContext {
            _conn: self.conn.clone(),
            handle: ioctx_handle,
        })
    }
}


/// The type of a RADOS AIO operation which has yet to complete.
#[derive(Debug)]
pub struct RadosFuture<T> {
    completion_res: StdResult<Completion<T>, Option<Error>>,
}


impl<T> RadosFuture<T> {
    fn new<F>(data: T, init: F) -> RadosFuture<T>
    where
        F: Fn(rados_completion_t) -> Result<()>,
    {
        RadosFuture { completion_res: Completion::new(data, init).map_err(Some) }
    }
}


impl<T> Future for RadosFuture<T> {
    type Item = T;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.completion_res.as_mut() {
            Ok(completion) => completion.poll(),
            Err(error) => Err(error.take().unwrap()),
        }
    }
}


/// Statistics for a single RADOS object.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct RadosStat {
    pub size: u64,
    pub last_modified: DateTime<Local>,
}


/// A wrapper around a `rados_ioctx_t`, which also counts as a reference to the underlying
/// `RadosConnection`.
pub struct RadosContext {
    _conn: Arc<RadosHandle>,
    handle: rados_ioctx_t,
}


// `RadosContext` is safe to `Send`, but not `Sync`; this is because nothing about the
// `rados_ioctx_t` specifically ties it to the current thread. Thus it is safe to `Send`. However,
// it is good practice [citation needed] to not use the same `rados_ioctx_t` from different
// threads; hence `Send` but not `Clone` nor `Sync`.
//
// The #ceph-devel IRC channel on OFTC has also confirmed that access to `rados_ioctx_t` should be
// synchronized, but it is safe to send across threads.
unsafe impl Send for RadosContext {}
// !impl Sync for RadosContext {}


impl Drop for RadosContext {
    fn drop(&mut self) {
        unsafe {
            rados::rados_ioctx_destroy(self.handle);
        }
    }
}


impl RadosContext {
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

        errors::librados(unsafe {
            rados::rados_remove(self.handle, object_id.as_ptr())
        })?;

        mem::drop(object_id);

        Ok(())
    }


    /// Resize a RADOS object, filling with zeroes if necessary, using `rados_trunc`.
    pub fn resize(&mut self, obj: &str, size: u64) -> Result<()> {
        let object_id = POOL.get_str(obj)?;

        errors::librados(unsafe {
            rados::rados_trunc(self.handle, object_id.as_ptr(), size)
        })?;

        mem::drop(object_id);

        Ok(())
    }


    /// Get the statistics of a given RADOS object using `rados_stat`.
    pub fn stat(&mut self, obj: &str) -> Result<RadosStat> {
        let object_id = POOL.get_str(obj)?;

        let mut size = 0;
        let mut time = 0;

        errors::librados(unsafe {
            rados::rados_stat(self.handle, object_id.as_ptr(), &mut size, &mut time)
        })?;

        mem::drop(object_id);

        Ok(RadosStat {
            size,
            last_modified: Local.timestamp(time, 0),
        })
    }


    /// Asynchronously write to a RADOS object using `rados_aio_write`.
    pub fn write_async(&mut self, obj: &str, buf: &[u8], offset: u64) -> RadosFuture<()> {
        RadosFuture::new((), |completion_handle| {
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
    pub fn append_async(&mut self, obj: &str, buf: &[u8]) -> RadosFuture<()> {
        RadosFuture::new((), |completion_handle| {
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
    pub fn write_full_async(&mut self, obj: &str, buf: &[u8]) -> RadosFuture<()> {
        RadosFuture::new((), |completion_handle| {
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
    pub fn remove_async(&mut self, obj: &str) -> RadosFuture<()> {
        RadosFuture::new((), |completion_handle| {
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
    pub fn read_async<B>(&mut self, obj: &str, mut buf: B, offset: u64) -> RadosFuture<B>
    where
        B: StableDeref + DerefMut<Target = [u8]>,
    {
        let buf_ptr = buf.as_mut_ptr() as *mut libc::c_char;
        let buf_len = buf.len();

        RadosFuture::new(buf, |completion_handle| {
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
    pub fn stat_async(
        &mut self,
        obj: &str,
    ) -> Map<RadosFuture<Box<(u64, libc::time_t)>>, fn(Box<(u64, libc::time_t)>) -> RadosStat> {
        fn deref_move(boxed: Box<(u64, libc::time_t)>) -> RadosStat {
            RadosStat {
                size: boxed.0,
                last_modified: Local.timestamp(boxed.1, 0),
            }
        }

        let mut boxed = Box::new((0, 0));
        let size_ptr = &mut boxed.0 as *mut u64;
        let time_ptr = &mut boxed.1 as *mut libc::time_t;

        RadosFuture::new(boxed, |completion_handle| {
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
        }).map(deref_move)
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
    pub fn exists_async(
        &mut self,
        obj: &str,
    ) -> Then<RadosFuture<()>, Result<bool>, fn(Result<()>) -> Result<bool>> {
        fn catch_enoent(result: Result<()>) -> Result<bool> {
            match result {
                Ok(()) => Ok(true),
                Err(Error(ErrorKind::Rados(err_code), _)) if err_code == libc::ENOENT as u32 => {
                    Ok(false)
                }
                Err(err) => Err(err),
            }
        }

        RadosFuture::new((), |completion_handle| {
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
        }).then(catch_enoent)
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
    pub fn flush_async(&mut self) -> RadosFuture<()> {
        RadosFuture::new((), |completion_handle| {
            errors::librados(unsafe {
                rados::rados_aio_flush_async(self.handle, completion_handle)
            })
        })
    }
}
