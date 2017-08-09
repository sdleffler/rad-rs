//! Wrappers for Ceph cluster connections, Ceph I/O contexts, and associated operations.
//!
//! ## Connection building example
//!
//! The following example connects to a Ceph cluster using the `ceph.conf` file at
//! `/etc/ceph/ceph.conf`, as the `admin` user, and setting the RADOS config `keyring`
//! option to the value of `/etc/ceph/ceph.client.admin.keyring` (which is necessary
//! for connecting to Ceph using config files/keyrings located on nonstandard paths.)
//!
//! ```rust,ignore
//! let cluster = RadosConnectionBuilder::with_user(c!("admin"))?
//!                   .read_conf_file(c!("/etc/ceph/ceph.conf"))?
//!                   .conf_set(c!("keyring"), c!("/etc/ceph/ceph.client.admin.keyring"))?
//!                   .connect()?;
//! ```
//!
//! ## File writing example
//!
//! The following example shows how to write to a file, assuming you have already connected
//! to a cluster.
//!
//! ```rust,ignore
//! use std::io::{BufReader, BufWriter};
//!
//! let cluster = ...;
//!
//! let pool = cluster.get_pool_context(c!("rbd")).unwrap();
//! let object = BufReader::new(pool.object(c!("test_file.obj")).unwrap());
//!
//! let file = File::open("test_file.txt").unwrap();
//! let reader = BufReader::new(file);
//!
//! loop {
//!     let buf = reader.fill_buf().unwrap();
//!
//!     if buf.len() == 0 {
//!         break;
//!     }
//!
//!     object.write(buf);
//! }
//!
//! object.flush().unwrap();
//! ```

use std::ffi::{CStr, CString};
use std::ptr;
use std::sync::Arc;

use ceph_rust::rados::{self, rados_t, rados_ioctx_t, Struct_rados_cluster_stat_t};
use chrono::{DateTime, Local, TimeZone};
use libc::{c_char, time_t, ENOENT};

use async::{RadosCaution, RadosFuture, RadosFinishWrite, RadosFinishAppend, RadosFinishFullWrite,
            RadosFinishRemove, RadosFinishFlush, RadosFinishRead, RadosFinishStat,
            RadosFinishExists};
use errors::{self, ErrorKind, Result};
use stream::RadosObject;


pub const RADOS_READ_BUFFER_SIZE: usize = 4096;


/// A wrapper around a `rados_t` providing methods for configuring the connection before finalizing
/// it.
pub struct RadosConnectionBuilder {
    handle: rados_t,
}


impl RadosConnectionBuilder {
    /// Start building a new connection. By default the client to connect as is `client.admin`.
    pub fn new() -> Result<RadosConnectionBuilder> {
        let mut handle = ptr::null_mut();

        let err = unsafe { rados::rados_create(&mut handle, ptr::null()) };

        if err < 0 {
            Err(ErrorKind::CreateClusterHandleFailed(c!("client.admin {DEFAULT}"),
                                                     try!(errors::get_error_string(-err as u32)))
                        .into())
        } else {
            Ok(RadosConnectionBuilder { handle })
        }
    }


    /// Start building a new connection with a specified user.
    pub fn with_user<T: AsRef<CStr> + Into<CString>>(user: T) -> Result<RadosConnectionBuilder> {
        let mut handle = ptr::null_mut();

        let err = unsafe { rados::rados_create(&mut handle, user.as_ref().as_ptr()) };

        if err < 0 {
            Err(ErrorKind::CreateClusterHandleFailed(user.into(),
                                                     try!(errors::get_error_string(-err as u32)))
                        .into())
        } else {
            Ok(RadosConnectionBuilder { handle })
        }
    }


    /// Read a configuration file from a given path.
    pub fn read_conf_file<T: AsRef<CStr> + Into<CString>>(self,
                                                          path: T)
                                                          -> Result<RadosConnectionBuilder> {
        let err = unsafe { rados::rados_conf_read_file(self.handle, path.as_ref().as_ptr()) };

        if err < 0 {
            Err(ErrorKind::ReadConfFromFileFailed(path.into(),
                                                  try!(errors::get_error_string(-err as u32)))
                        .into())
        } else {
            Ok(self)
        }
    }


    /// Set an individual configuration option. Useful options include `keyring` if you are trying
    /// to set up Ceph without storing everything inside `/etc/ceph`.
    pub fn conf_set<T, U>(self, option: T, value: U) -> Result<RadosConnectionBuilder>
        where T: AsRef<CStr> + Into<CString>,
              U: AsRef<CStr> + Into<CString>
    {
        let err = unsafe {
            rados::rados_conf_set(self.handle,
                                  option.as_ref().as_ptr(),
                                  value.as_ref().as_ptr())
        };

        if err < 0 {
            Err(ErrorKind::ConfSetFailed(option.into(),
                                         value.into(),
                                         try!(errors::get_error_string(-err as u32)))
                        .into())
        } else {
            Ok(self)
        }
    }


    /// Finish building the connection configuration and connect to the cluster.
    pub fn connect(self) -> Result<RadosConnection> {
        let err = unsafe { rados::rados_connect(self.handle) };

        if err < 0 {
            Err(ErrorKind::ConnectFailed(try!(errors::get_error_string(-err as u32))).into())
        } else {
            Ok(RadosConnection { _dummy: ptr::null(), conn: Arc::new(RadosHandle { handle: self.handle }) })
        }
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
    pub fn stat(&self) -> Result<RadosClusterStat> {
        let mut cluster_stat = Struct_rados_cluster_stat_t {
            kb: 0,
            kb_used: 0,
            kb_avail: 0,
            num_objects: 0,
        };

        let err = unsafe { rados::rados_cluster_stat(self.conn.handle, &mut cluster_stat) };

        if err < 0 {
            Err(ErrorKind::ClusterStatFailed(try!(errors::get_error_string(-err as u32))).into())
        } else {
            Ok(RadosClusterStat {
                   kb: cluster_stat.kb,
                   kb_used: cluster_stat.kb_used,
                   kb_avail: cluster_stat.kb_avail,
                   num_objects: cluster_stat.num_objects,
               })
        }
    }


    /// Fetch the `rados_ioctx_t` for the relevant pool, using `rados_ioctx_create`.
    pub fn get_pool_context<T: AsRef<CStr> + Into<CString>>(&self,
                                                                pool_name: T)
                                                                -> Result<RadosContext> {
        let mut ioctx_handle = ptr::null_mut();

        let err = unsafe {
            rados::rados_ioctx_create(self.conn.handle, pool_name.as_ref().as_ptr(), &mut ioctx_handle)
        };

        if err < 0 {
            Err(ErrorKind::IoCtxFailed(pool_name.into(),
                                       try!(errors::get_error_string(-err as u32)))
                        .into())
        } else {
            Ok(RadosContext {
                   _conn: self.conn.clone(),
                   handle: ioctx_handle,
               })
        }
    }


    /// Fetch the `rados_ioctx_t` for the relevant pool, using `rados_ioctx_create2`.
    pub fn get_pool_context_from_id(&self, pool_id: u64) -> Result<RadosContext> {
        let mut ioctx_handle = ptr::null_mut();

        let err = unsafe {
            rados::rados_ioctx_create2(self.conn.handle,
                                       *(&pool_id as *const u64 as *const i64),
                                       &mut ioctx_handle)
        };

        if err < 0 {
            // It's okay to unwrap the new CString here because we're guaranteed
            // to get a valid CString - we're printing a number.
            Err(ErrorKind::IoCtxFailed(CString::new(pool_id.to_string()).unwrap(),
                                       try!(errors::get_error_string(-err as u32)))
                        .into())
        } else {
            Ok(RadosContext {
                   _conn: self.conn.clone(),
                   handle: ioctx_handle,
               })
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
    pub fn get_xattr<T, U>(&self, obj: T, key: U) -> Result<Vec<u8>>
        where T: AsRef<CStr> + Into<CString>,
              U: AsRef<CStr> + Into<CString>
    {
        let mut buf = [0u8; RADOS_READ_BUFFER_SIZE];

        let err = unsafe {
            rados::rados_getxattr(self.handle,
                                  obj.as_ref().as_ptr(),
                                  key.as_ref().as_ptr(),
                                  buf.as_mut_ptr() as *mut c_char,
                                  buf.len())
        };

        if err < 0 {
            let error_string = try!(errors::get_error_string(-err as u32));

            Err(ErrorKind::GetXAttrFailed(obj.into(), key.into(), error_string).into())
        } else {
            Ok(buf.iter().cloned().take(err as usize).collect())
        }
    }


    /// Set an extended attribute on a given RADOS object using `rados_setxattr`.
    pub fn set_xattr<T, U>(&self, obj: T, key: U, value: &[u8]) -> Result<()>
        where T: AsRef<CStr> + Into<CString>,
              U: AsRef<CStr> + Into<CString>
    {
        let err = unsafe {
            rados::rados_setxattr(self.handle,
                                  obj.as_ref().as_ptr(),
                                  key.as_ref().as_ptr(),
                                  value.as_ptr() as *const c_char,
                                  value.len())
        };

        if err < 0 {
            let error_string = try!(errors::get_error_string(-err as u32));

            Err(ErrorKind::SetXAttrFailed(obj.into(), key.into(), value.len(), error_string).into())
        } else {
            Ok(())
        }
    }


    /// Write to a RADOS object using `rados_write`.
    pub fn write<T>(&self, obj: T, buf: &[u8], offset: u64) -> Result<()>
        where T: AsRef<CStr> + Into<CString>
    {
        let err = unsafe {
            rados::rados_write(self.handle,
                               obj.as_ref().as_ptr(),
                               buf.as_ptr() as *const c_char,
                               buf.len(),
                               offset)
        };

        if err < 0 {
            let error_string = try!(errors::get_error_string(-err as u32));

            Err(ErrorKind::WriteFailed(obj.into(), buf.len(), offset, error_string).into())
        } else {
            Ok(())
        }
    }


    /// Write the entirety of a RADOS object, overwriting if necessary, using `rados_write_full`.
    pub fn write_full<T>(&self, obj: T, buf: &[u8]) -> Result<()>
        where T: AsRef<CStr> + Into<CString>
    {
        let err = unsafe {
            rados::rados_write_full(self.handle,
                                    obj.as_ref().as_ptr(),
                                    buf.as_ptr() as *const c_char,
                                    buf.len())
        };

        if err < 0 {
            let error_string = try!(errors::get_error_string(-err as u32));

            Err(ErrorKind::FullWriteFailed(obj.into(), buf.len(), error_string).into())
        } else {
            Ok(())
        }
    }


    /// Append to a RADOS object using `rados_append`.
    pub fn append<T>(&self, obj: T, buf: &[u8]) -> Result<()>
        where T: AsRef<CStr> + Into<CString>
    {
        let err = unsafe {
            rados::rados_append(self.handle,
                                obj.as_ref().as_ptr(),
                                buf.as_ptr() as *const c_char,
                                buf.len())
        };

        if err < 0 {
            let error_string = try!(errors::get_error_string(-err as u32));

            Err(ErrorKind::AppendFailed(obj.into(), buf.len(), error_string).into())
        } else {
            Ok(())
        }
    }


    /// Read from a RADOS object using `rados_read`.
    pub fn read<T>(&self, obj: T, buf: &mut [u8], offset: u64) -> Result<usize>
        where T: AsRef<CStr> + Into<CString>
    {
        let err = unsafe {
            rados::rados_read(self.handle,
                              obj.as_ref().as_ptr(),
                              buf.as_mut_ptr() as *mut c_char,
                              buf.len(),
                              offset)
        };

        if err < 0 {
            let error_string = try!(errors::get_error_string(-err as u32));

            Err(ErrorKind::ReadFailed(obj.into(), buf.len(), offset, error_string).into())
        } else {
            Ok(err as usize)
        }
    }


    /// The current implementation of `read_full` is very inefficient and just a placeholder -
    /// eventually `read_full` should use asynchronous I/O instead of synchronously waiting for
    /// *every* single requested block.
    ///
    /// TODO: read in the length with `RadosContext::stat` and then asynchronously read chunks.
    pub fn read_full<T>(&self, obj: T, vec: &mut Vec<u8>) -> Result<usize>
        where T: AsRef<CStr> + Into<CString>
    {
        let mut buf = [0u8; RADOS_READ_BUFFER_SIZE];

        let mut res;
        let mut offset = 0;

        let c_obj = obj.into();

        loop {
            res = self.read(c_obj.as_ref(), &mut buf, offset as u64);

            match res {
                Ok(n) if n == 0 => {
                    return Ok(offset);
                }
                Ok(n) => {
                    vec.extend(buf[0..n].iter().cloned());
                    offset += n;
                }
                Err(..) => {
                    return res;
                }
            }
        }
    }


    /// Delete a RADOS object using `rados_remove`.
    pub fn remove<T: AsRef<CStr> + Into<CString>>(&self, obj: T) -> Result<()> {
        let err = unsafe { rados::rados_remove(self.handle, obj.as_ref().as_ptr()) };

        if err < 0 {
            let error_string = try!(errors::get_error_string(-err as u32));

            Err(ErrorKind::RemoveFailed(obj.into(), error_string).into())
        } else {
            Ok(())
        }
    }


    /// Resize a RADOS object, filling with zeroes if necessary, using `rados_trunc`.
    pub fn resize<T: AsRef<CStr> + Into<CString>>(&self, obj: T, size: u64) -> Result<()> {
        let err = unsafe { rados::rados_trunc(self.handle, obj.as_ref().as_ptr(), size) };

        if err < 0 {
            let error_string = try!(errors::get_error_string(-err as u32));

            Err(ErrorKind::TruncFailed(obj.into(), size, error_string).into())
        } else {
            Ok(())
        }
    }


    /// Get the statistics of a given RADOS object using `rados_stat`.
    pub fn stat<T>(&self, obj: T) -> Result<RadosStat>
        where T: AsRef<CStr> + Into<CString>
    {
        let mut size: u64 = 0;
        let mut time: time_t = 0;

        let err =
            unsafe { rados::rados_stat(self.handle, obj.as_ref().as_ptr(), &mut size, &mut time) };

        if err < 0 {
            let error_string = try!(errors::get_error_string(-err as u32));

            Err(ErrorKind::StatFailed(obj.into(), error_string).into())
        } else {
            Ok(RadosStat {
                   size,
                   last_modified: Local.timestamp(time, 0),
               })
        }
    }


    /// Asynchronously write to a RADOS object using `rados_aio_write`.
    pub fn write_async<T>(&self,
                          caution: RadosCaution,
                          obj: T,
                          buf: &[u8],
                          offset: u64)
                          -> Result<RadosFuture<RadosFinishWrite>>
        where T: AsRef<CStr> + Into<CString>
    {
        let c_obj = obj.into();

        let c_obj_ptr = c_obj.as_ptr();

        let write_info = RadosFinishWrite {
            oid: c_obj,
            len: buf.len(),
            off: offset,
        };

        RadosFuture::new(caution, write_info, |completion_handle| {
            let err = unsafe {
                rados::rados_aio_write(self.handle,
                                       c_obj_ptr,
                                       completion_handle,
                                       buf.as_ptr() as *const c_char,
                                       buf.len(),
                                       offset)
            };

            if err < 0 {
                Err(try!(errors::get_error_string(-err as u32)).into())
            } else {
                Ok(())
            }
        })
    }


    /// Asynchronously append to a RADOS object using `rados_aio_append`.
    pub fn append_async<T>(&self,
                           caution: RadosCaution,
                           obj: T,
                           buf: &[u8])
                           -> Result<RadosFuture<RadosFinishAppend>>
        where T: AsRef<CStr> + Into<CString>
    {
        let c_obj = obj.into();

        let c_obj_ptr = c_obj.as_ref().as_ptr();

        let append_info = RadosFinishAppend {
            oid: c_obj,
            len: buf.len(),
        };

        RadosFuture::new(caution, append_info, |completion_handle| {
            let err = unsafe {
                rados::rados_aio_append(self.handle,
                                        c_obj_ptr,
                                        completion_handle,
                                        buf.as_ptr() as *const c_char,
                                        buf.len())
            };

            if err < 0 {
                Err(try!(errors::get_error_string(-err as u32)).into())
            } else {
                Ok(())
            }
        })
    }


    /// Asynchronously set the contents of a RADOS object using `rados_aio_write_full`.
    pub fn write_full_async<T>(&self,
                               caution: RadosCaution,
                               obj: T,
                               buf: &[u8])
                               -> Result<RadosFuture<RadosFinishFullWrite>>
        where T: AsRef<CStr> + Into<CString>
    {
        let c_obj = obj.into();

        let c_obj_ptr = c_obj.as_ref().as_ptr();

        let full_write_info = RadosFinishFullWrite {
            oid: c_obj,
            len: buf.len(),
        };

        RadosFuture::new(caution, full_write_info, |completion_handle| {
            let err = unsafe {
                rados::rados_aio_write_full(self.handle,
                                            c_obj_ptr,
                                            completion_handle,
                                            buf.as_ptr() as *const c_char,
                                            buf.len())
            };

            if err < 0 {
                Err(try!(errors::get_error_string(-err as u32)).into())
            } else {
                Ok(())
            }
        })
    }


    /// Asynchronously delete a RADOS object using `rados_aio_remove`.
    pub fn remove_async<T>(&self,
                           caution: RadosCaution,
                           obj: T)
                           -> Result<RadosFuture<RadosFinishRemove>>
        where T: AsRef<CStr> + Into<CString>
    {
        let c_obj = obj.into();

        let c_obj_ptr = c_obj.as_ref().as_ptr();

        let remove_info = RadosFinishRemove { oid: c_obj };

        RadosFuture::new(caution, remove_info, |completion_handle| {
            let err = unsafe { rados::rados_aio_remove(self.handle, c_obj_ptr, completion_handle) };

            if err < 0 {
                Err(try!(errors::get_error_string(-err as u32)).into())
            } else {
                Ok(())
            }
        })
    }


    /// Asynchronously read a RADOS object using `rados_aio_read`.
    ///
    /// Since this function takes a mutable reference to the relevant buffer, the future must live
    /// as long as the buffer you are reading into.
    pub fn read_async<'ctx, 'buf, T>(&'ctx self,
                                     obj: T,
                                     buf: &'buf mut [u8],
                                     offset: u64)
                                     -> Result<RadosFuture<RadosFinishRead<'buf>>>
        where T: AsRef<CStr> + Into<CString>
    {
        let c_obj = obj.into();

        let c_obj_ptr = c_obj.as_ref().as_ptr();

        let buf_ptr = buf.as_mut_ptr() as *mut c_char;
        let buf_len = buf.len();

        let read_info = RadosFinishRead {
            bytes: buf,
            oid: c_obj,
            off: offset,
        };

        RadosFuture::new(RadosCaution::Complete, read_info, |completion_handle| {
            let err = unsafe {
                rados::rados_aio_read(self.handle,
                                      c_obj_ptr,
                                      completion_handle,
                                      buf_ptr,
                                      buf_len,
                                      offset)
            };

            if err < 0 {
                Err(try!(errors::get_error_string(-err as u32)).into())
            } else {
                Ok(())
            }
        })
    }


    /// Asynchronously fetch the statistics of a RADOS object using `rados_aio_stat`.
    pub fn stat_async<T>(&self, obj: T) -> Result<RadosFuture<RadosFinishStat>>
        where T: AsRef<CStr> + Into<CString>
    {
        let c_obj = obj.into();

        let c_obj_ptr = c_obj.as_ref().as_ptr();

        let mut boxed = Box::new((0, 0));

        let (size_ptr, time_ptr) = (&mut boxed.0 as *mut u64, &mut boxed.1 as *mut time_t);

        let read_info = RadosFinishStat { oid: c_obj, boxed };

        RadosFuture::new(RadosCaution::Complete, read_info, |completion_handle| {
            let err = unsafe {
                rados::rados_aio_stat(self.handle,
                                      c_obj_ptr,
                                      completion_handle,
                                      size_ptr,
                                      time_ptr)
            };

            if err < 0 {
                Err(try!(errors::get_error_string(-err as u32)).into())
            } else {
                Ok(())
            }
        })
    }


    /// Check whether or not a RADOS object exists under a given name, using `rados_stat` and
    /// checking the error code for `ENOENT`.
    pub fn exists<T>(&self, obj: T) -> Result<bool>
        where T: AsRef<CStr> + Into<CString>
    {
        let mut size: u64 = 0;
        let mut time: time_t = 0;

        let err =
            unsafe { rados::rados_stat(self.handle, obj.as_ref().as_ptr(), &mut size, &mut time) };

        if -err == ENOENT {
            Ok(false)
        } else if err < 0 {
            let error_string = try!(errors::get_error_string(-err as u32));

            Err(ErrorKind::StatFailed(obj.into(), error_string).into())
        } else {
            Ok(true)
        }
    }


    /// Asynchronously check for RADOS object existence using `rados_aio_stat` and checking the
    /// error code for `ENOENT`.
    pub fn exists_async<T: AsRef<CStr> + Into<CString>>
        (&self,
         obj: T)
         -> Result<RadosFuture<RadosFinishExists>> {
        let c_obj = obj.into();
        let c_obj_ptr = c_obj.as_ptr();

        let read_info = RadosFinishExists { oid: c_obj };

        RadosFuture::new(RadosCaution::Complete, read_info, |completion_handle| {
            let err = unsafe {
                rados::rados_aio_stat(self.handle,
                                      c_obj_ptr,
                                      completion_handle,
                                      ptr::null_mut(),
                                      ptr::null_mut())
            };

            if err < 0 {
                Err(try!(errors::get_error_string(-err as u32)).into())
            } else {
                Ok(())
            }
        })
    }


    /// Flush all asynchronous I/O actions on the given context, blocking until they are complete.
    pub fn flush(&self) -> Result<()> {
        // BUG: `rados_aio_flush` always returns 0
        // http://docs.ceph.com/docs/master/rados/api/librados/#rados_aio_flush
        //
        // This function returns a `Result` because in the future `rados_aio_flush`
        // may change to return an error code.

        let err = unsafe { rados::rados_aio_flush(self.handle) };

        if err < 0 {
            Err(ErrorKind::FlushFailed(try!(errors::get_error_string(-err as u32))).into())
        } else {
            Ok(())
        }
    }


    /// Asynchronously flush all asynchronous I/O actions on the given context.  The resulting
    /// future will complete when all I/O actions are complete.
    pub fn flush_async(&self) -> Result<RadosFuture<RadosFinishFlush>> {
        RadosFuture::new(RadosCaution::Safe, RadosFinishFlush, |completion_handle| {
            let err = unsafe { rados::rados_aio_flush_async(self.handle, completion_handle) };

            if err < 0 {
                Err(try!(errors::get_error_string(-err as u32)).into())
            } else {
                Ok(())
            }
        })
    }


    /// This function is a convenient wrapper around `RadosContext::object_with_caution` which uses
    /// a default caution value of `RadosCaution::Complete`.
    pub fn object<'a, T>(&'a mut self, obj: T) -> RadosObject<'a>
        where T: AsRef<CStr> + Into<CString>
    {
        self.object_with_caution(RadosCaution::Complete, obj)
    }


    /// Get a wrapper for a given object which implements `Read`, `Write`, and `Seek`.  Write
    /// operations are done asynchronously; reads synchronously, and `Seek` only runs a librados
    /// operation in the case of `SeekFrom::End`.
    pub fn object_with_caution<'a, T>(&'a mut self, caution: RadosCaution, obj: T) -> RadosObject<'a>
        where T: AsRef<CStr> + Into<CString>
    {
        RadosObject::new(self, caution.into(), obj.into())
    }
}
