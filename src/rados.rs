use std::ffi::CStr;
use std::marker::PhantomData;
use std::mem;
use std::ptr;

use ceph_rust::rados::{self, rados_t, rados_ioctx_t, Struct_rados_cluster_stat_t};
use chrono::{DateTime, Local, TimeZone};
use libc::{c_char, time_t, ENOENT};

use async::{RadosCaution, RadosFuture, RadosFinishWrite, RadosFinishAppend, RadosFinishFullWrite,
            RadosFinishRemove, RadosFinishFlush, RadosFinishRead, RadosFinishStat,
            RadosFinishExists};
use errors::{self, ErrorKind, Result};
use into_ffi::{IntoFfiString, IntoRustString};


pub const RADOS_READ_BUFFER_SIZE: usize = 4096;


pub struct RadosConnectionBuilder {
    handle: rados_t,
}


impl RadosConnectionBuilder {
    pub fn new() -> Result<RadosConnectionBuilder> {
        let mut handle = ptr::null_mut();

        let err = unsafe { rados::rados_create(&mut handle, ptr::null()) };

        if err < 0 {
            Err(ErrorKind::CreateClusterHandleFailed("client.admin {DEFAULT}".to_string(),
                                                     try!(errors::get_error_string(err)))
                        .into())
        } else {
            Ok(RadosConnectionBuilder { handle })
        }
    }


    pub fn with_user<T: IntoFfiString>(user: T) -> Result<RadosConnectionBuilder> {
        let mut handle = ptr::null_mut();

        let c_user = user.into_ffi_string()
            .expect("error while converting user string into a C-compatible string!");

        let err = unsafe { rados::rados_create(&mut handle, c_user.as_ref().as_ptr()) };

        if err < 0 {
            Err(ErrorKind::CreateClusterHandleFailed(c_user.into_rust_string()?.into(),
                                                     try!(errors::get_error_string(err)))
                        .into())
        } else {
            Ok(RadosConnectionBuilder { handle })
        }
    }


    pub fn read_conf_file<T: IntoFfiString>(self, path: T) -> Result<RadosConnectionBuilder> {
        let c_path = path.into_ffi_string()
            .expect("error while converting path into a C-compatible string!");

        let err = unsafe { rados::rados_conf_read_file(self.handle, c_path.as_ref().as_ptr()) };

        if err < 0 {
            Err(ErrorKind::ReadConfFromFileFailed(c_path.into_rust_string()?.into(),
                                                  try!(errors::get_error_string(err)))
                        .into())
        } else {
            Ok(self)
        }
    }


    pub fn conf_set<T: IntoFfiString, U: IntoFfiString>(self,
                                                        option: T,
                                                        value: U)
                                                        -> Result<RadosConnectionBuilder> {
        let c_option = option
            .into_ffi_string()
            .expect("error while converting option into a C-compatible string!");
        let c_value = value
            .into_ffi_string()
            .expect("error while converting value into a C-compatible string!");

        let err =
            unsafe { rados::rados_conf_set(self.handle, c_option.as_ref().as_ptr(), c_value.as_ref().as_ptr()) };

        if err < 0 {
            Err(ErrorKind::ConfSetFailed(c_option.into_rust_string()?.into(),
                                         c_value.into_rust_string()?.into(),
                                         try!(errors::get_error_string(err)))
                        .into())
        } else {
            Ok(self)
        }
    }


    pub fn connect(self) -> Result<RadosCluster> {
        let err = unsafe { rados::rados_connect(self.handle) };

        if err < 0 {
            bail!(ErrorKind::ConnectFailed(try!(errors::get_error_string(err))));
        } else {
            Ok(RadosCluster { handle: self.handle })
        }
    }
}


#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct RadosClusterStat {
    pub kb: u64,
    pub kb_used: u64,
    pub kb_avail: u64,
    pub num_objects: u64,
}


pub struct RadosCluster {
    handle: rados_t,
}


impl Drop for RadosCluster {
    fn drop(&mut self) {
        unsafe {
            rados::rados_shutdown(self.handle);
        }
    }
}


impl RadosCluster {
    pub fn stat(&self) -> Result<RadosClusterStat> {
        let mut cluster_stat = Struct_rados_cluster_stat_t {
            kb: 0,
            kb_used: 0,
            kb_avail: 0,
            num_objects: 0,
        };

        let err = unsafe { rados::rados_cluster_stat(self.handle, &mut cluster_stat) };

        if err < 0 {
            Err(ErrorKind::ClusterStatFailed(try!(errors::get_error_string(err))).into())
        } else {
            Ok(RadosClusterStat {
                   kb: cluster_stat.kb,
                   kb_used: cluster_stat.kb_used,
                   kb_avail: cluster_stat.kb_avail,
                   num_objects: cluster_stat.num_objects,
               })
        }
    }


    pub fn get_pool_context<'a, T: IntoFfiString>(&'a self,
                                                  pool_name: T)
                                                  -> Result<RadosContext<'a>> {
        let mut ioctx_handle = ptr::null_mut();

        let c_pool_name =
            pool_name
                .into_ffi_string()
                .expect("error while converting pool name into a C-compatible string!");

        let err = unsafe {
            rados::rados_ioctx_create(self.handle, c_pool_name.as_ref().as_ptr(), &mut ioctx_handle)
        };

        if err < 0 {
            Err(ErrorKind::IoCtxFailed(c_pool_name.into_rust_string()?.into(),
                                       try!(errors::get_error_string(err)))
                        .into())
        } else {
            Ok(RadosContext {
                   conn: PhantomData,
                   handle: ioctx_handle,
               })
        }
    }


    pub fn get_pool_context_from_id<'a>(&'a self, pool_id: u64) -> Result<RadosContext<'a>> {
        let mut ioctx_handle = ptr::null_mut();

        let err = unsafe {
            rados::rados_ioctx_create2(self.handle,
                                       *(&pool_id as *const u64 as *const i64),
                                       &mut ioctx_handle)
        };

        if err < 0 {
            Err(ErrorKind::IoCtxFailed(pool_id.to_string(), try!(errors::get_error_string(err)))
                    .into())
        } else {
            Ok(RadosContext {
                   conn: PhantomData,
                   handle: ioctx_handle,
               })
        }
    }
}


#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct RadosStat {
    pub size: u64,
    pub last_modified: DateTime<Local>,
}


pub struct RadosContext<'a> {
    conn: PhantomData<&'a RadosCluster>,
    handle: rados_ioctx_t,
}


impl<'a> Drop for RadosContext<'a> {
    fn drop(&mut self) {
        unsafe {
            rados::rados_ioctx_destroy(self.handle);
        }
    }
}


impl<'a> RadosContext<'a> {
    pub fn get_xattr<T: IntoFfiString, U: IntoFfiString>(&self, obj: T, key: U) -> Result<Vec<u8>> {
        let c_obj = obj.into_ffi_string()?;
        let c_key = key.into_ffi_string()?;

        let mut buf = [0u8; RADOS_READ_BUFFER_SIZE];

        let err = unsafe {
            rados::rados_getxattr(self.handle,
                                  c_obj.as_ref().as_ptr(),
                                  c_key.as_ref().as_ptr(),
                                  buf.as_mut_ptr() as *mut c_char,
                                  buf.len())
        };

        if err < 0 {
            let error_string = try!(errors::get_error_string(err));

            Err(ErrorKind::GetXAttrFailed(c_obj.into_rust_string().unwrap().into(),
                                          c_key.into_rust_string().unwrap().into(),
                                          error_string)
                        .into())
        } else {
            Ok(buf.iter().cloned().take(err as usize).collect())
        }
    }


    pub fn set_xattr<T: IntoFfiString, U: IntoFfiString>(&self,
                                                         obj: T,
                                                         key: U,
                                                         value: &[u8])
                                                         -> Result<()> {
        let c_obj = obj.into_ffi_string()?;
        let c_key = key.into_ffi_string()?;

        let err = unsafe {
            rados::rados_setxattr(self.handle,
                                  c_obj.as_ref().as_ptr(),
                                  c_key.as_ref().as_ptr(),
                                  value.as_ptr() as *const c_char,
                                  value.len())
        };

        if err < 0 {
            let error_string = try!(errors::get_error_string(err));

            Err(ErrorKind::SetXAttrFailed(c_obj.into_rust_string().unwrap().into(),
                                          c_key.into_rust_string().unwrap().into(),
                                          value.len(),
                                          error_string)
                        .into())
        } else {
            Ok(())
        }
    }


    pub fn write<T: IntoFfiString>(&self, obj: T, buf: &[u8], offset: u64) -> Result<()> {
        let c_obj = obj.into_ffi_string()?;

        let err = unsafe {
            rados::rados_write(self.handle,
                               c_obj.as_ref().as_ptr(),
                               buf.as_ptr() as *const c_char,
                               buf.len(),
                               offset)
        };

        if err < 0 {
            let error_string = try!(errors::get_error_string(err));

            Err(ErrorKind::WriteFailed(c_obj.into_rust_string().unwrap().into(),
                                       buf.len(),
                                       offset,
                                       error_string)
                        .into())
        } else {
            Ok(())
        }
    }


    pub fn write_full<T: IntoFfiString>(&self, obj: T, buf: &[u8]) -> Result<()> {
        let c_obj = obj.into_ffi_string()?;

        let err = unsafe {
            rados::rados_write_full(self.handle,
                                    c_obj.as_ref().as_ptr(),
                                    buf.as_ptr() as *const c_char,
                                    buf.len())
        };

        if err < 0 {
            let error_string = try!(errors::get_error_string(err));

            Err(ErrorKind::FullWriteFailed(c_obj.into_rust_string().unwrap().into(),
                                           buf.len(),
                                           error_string)
                        .into())
        } else {
            Ok(())
        }
    }


    pub fn append<T: IntoFfiString>(&self, obj: T, buf: &[u8]) -> Result<()> {
        let c_obj = obj.into_ffi_string()?;

        let err = unsafe {
            rados::rados_append(self.handle,
                                c_obj.as_ref().as_ptr(),
                                buf.as_ptr() as *const c_char,
                                buf.len())
        };

        if err < 0 {
            let error_string = try!(errors::get_error_string(err));

            Err(ErrorKind::AppendFailed(c_obj.into_rust_string().unwrap().into(),
                                        buf.len(),
                                        error_string)
                        .into())
        } else {
            Ok(())
        }
    }


    pub fn read<T: IntoFfiString>(&self, obj: T, buf: &mut [u8], offset: u64) -> Result<usize> {
        let c_obj = obj.into_ffi_string()?;

        let err = unsafe {
            rados::rados_read(self.handle,
                              c_obj.as_ref().as_ptr(),
                              buf.as_mut_ptr() as *mut c_char,
                              buf.len(),
                              offset)
        };

        if err < 0 {
            let error_string = try!(errors::get_error_string(err));

            bail!(ErrorKind::ReadFailed(c_obj.into_rust_string().unwrap().into(),
                                        buf.len(),
                                        offset,
                                        error_string));
        } else {
            Ok(err as usize)
        }
    }


    /// The current implementation of `read_full` is very inefficient and just a
    /// placeholder - eventually `read_full` should use asynchronous I/O instead
    /// of synchronously waiting for *every* single requested block. Very slow!
    pub fn read_full<T: IntoFfiString>(&self, obj: T, vec: &mut Vec<u8>) -> Result<usize> {
        let mut buf = [0u8; RADOS_READ_BUFFER_SIZE];

        let mut res;
        let mut offset = 0;

        let ffi_obj = obj.into_ffi_string()?;
        let c_obj = unsafe { CStr::from_ptr(ffi_obj.as_ref().as_ptr()) };

        loop {
            res = self.read(c_obj, &mut buf, offset as u64);

            match res {
                Ok(n) if n == 0 => {
                    mem::drop(c_obj);
                    return Ok(offset);
                }
                Ok(n) => {
                    vec.extend(buf[0..n].iter().cloned());
                    offset += n;
                }
                Err(..) => {
                    mem::drop(c_obj);
                    return res;
                }
            }
        }
    }


    pub fn remove<T: IntoFfiString>(&self, obj: T) -> Result<()> {
        let c_obj = obj.into_ffi_string()?;

        let err = unsafe { rados::rados_remove(self.handle, c_obj.as_ref().as_ptr()) };

        if err < 0 {
            let error_string = try!(errors::get_error_string(err));

            Err(ErrorKind::RemoveFailed(c_obj.into_rust_string().unwrap().into(), error_string)
                    .into())
        } else {
            Ok(())
        }
    }


    pub fn resize<T: IntoFfiString>(&self, obj: T, size: u64) -> Result<()> {
        let c_obj = obj.into_ffi_string()?;

        let err = unsafe { rados::rados_trunc(self.handle, c_obj.as_ref().as_ptr(), size) };

        if err < 0 {
            let error_string = try!(errors::get_error_string(err));

            Err(ErrorKind::TruncFailed(c_obj.into_rust_string().unwrap().into(),
                                       size,
                                       error_string)
                        .into())
        } else {
            Ok(())
        }
    }


    pub fn stat<T: IntoFfiString>(&self, obj: T) -> Result<RadosStat> {
        let c_obj = obj.into_ffi_string()?;

        let mut size: u64 = 0;
        let mut time: time_t = 0;

        let err = unsafe { rados::rados_stat(self.handle, c_obj.as_ref().as_ptr(), &mut size, &mut time) };

        if err < 0 {
            let error_string = try!(errors::get_error_string(err));

            Err(ErrorKind::StatFailed(c_obj.into_rust_string().unwrap().into(), error_string)
                    .into())
        } else {
            Ok(RadosStat {
                   size,
                   last_modified: Local.timestamp(time, 0),
               })
        }
    }


    pub fn write_async<T: IntoFfiString>(&self,
                                         caution: RadosCaution,
                                         obj: T,
                                         buf: &[u8],
                                         offset: u64)
                                         -> Result<RadosFuture<RadosFinishWrite>> {
        let c_obj = obj.into_ffi_string()?;

        let c_obj_ptr = c_obj.as_ref().as_ptr();

        let write_info = RadosFinishWrite {
            oid: c_obj.into_rust_string().unwrap().into(),
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
                Err(try!(errors::get_error_string(err)).into())
            } else {
                Ok(())
            }
        })
    }


    pub fn append_async<T: IntoFfiString>(&self,
                                          caution: RadosCaution,
                                          obj: T,
                                          buf: &[u8])
                                          -> Result<RadosFuture<RadosFinishAppend>> {
        let c_obj = obj.into_ffi_string()?;

        let c_obj_ptr = c_obj.as_ref().as_ptr();

        let append_info = RadosFinishAppend {
            oid: c_obj.into_rust_string().unwrap().into(),
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
                Err(try!(errors::get_error_string(err)).into())
            } else {
                Ok(())
            }
        })
    }


    pub fn write_full_async<T: IntoFfiString>(&self,
                                              caution: RadosCaution,
                                              obj: T,
                                              buf: &[u8])
                                              -> Result<RadosFuture<RadosFinishFullWrite>> {
        let c_obj = obj.into_ffi_string()?;

        let c_obj_ptr = c_obj.as_ref().as_ptr();

        let full_write_info = RadosFinishFullWrite {
            oid: c_obj.into_rust_string().unwrap().into(),
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
                Err(try!(errors::get_error_string(err)).into())
            } else {
                Ok(())
            }
        })
    }


    pub fn remove_async<T: IntoFfiString>(&self,
                                          caution: RadosCaution,
                                          obj: T)
                                          -> Result<RadosFuture<RadosFinishRemove>> {
        let c_obj = obj.into_ffi_string()?;

        let c_obj_ptr = c_obj.as_ref().as_ptr();

        let remove_info = RadosFinishRemove { oid: c_obj.into_rust_string().unwrap().into() };

        RadosFuture::new(caution, remove_info, |completion_handle| {
            let err = unsafe { rados::rados_aio_remove(self.handle, c_obj_ptr, completion_handle) };

            if err < 0 {
                Err(try!(errors::get_error_string(err)).into())
            } else {
                Ok(())
            }
        })
    }


    pub fn read_async<'ctx, 'buf, T: IntoFfiString>
        (&'ctx self,
         obj: T,
         buf: &'buf mut [u8],
         offset: u64)
         -> Result<RadosFuture<RadosFinishRead<'buf>>> {
        let c_obj = obj.into_ffi_string()?;

        let c_obj_ptr = c_obj.as_ref().as_ptr();

        let buf_ptr = buf.as_mut_ptr() as *mut c_char;
        let buf_len = buf.len();

        let read_info = RadosFinishRead {
            bytes: buf,
            oid: c_obj.into_rust_string().unwrap().into(),
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
                Err(try!(errors::get_error_string(err)).into())
            } else {
                Ok(())
            }
        })
    }


    pub fn stat_async<T: IntoFfiString>(&self, obj: T) -> Result<RadosFuture<RadosFinishStat>> {
        let c_obj = obj.into_ffi_string()?;

        let c_obj_ptr = c_obj.as_ref().as_ptr();

        let mut boxed = Box::new((0, 0));

        let (size_ptr, time_ptr) = (&mut boxed.0 as *mut u64, &mut boxed.1 as *mut time_t);

        let read_info = RadosFinishStat {
            oid: c_obj.into_rust_string().unwrap().into(),
            boxed,
        };

        RadosFuture::new(RadosCaution::Complete, read_info, |completion_handle| {
            let err = unsafe {
                rados::rados_aio_stat(self.handle,
                                      c_obj_ptr,
                                      completion_handle,
                                      size_ptr,
                                      time_ptr)
            };

            if err < 0 {
                Err(try!(errors::get_error_string(err)).into())
            } else {
                Ok(())
            }
        })
    }


    pub fn exists<T: IntoFfiString>(&self, obj: T) -> Result<bool> {
        let c_obj = obj.into_ffi_string()?;

        let mut size: u64 = 0;
        let mut time: time_t = 0;

        let err = unsafe { rados::rados_stat(self.handle, c_obj.as_ref().as_ptr(), &mut size, &mut time) };

        if -err == ENOENT {
            Ok(false)
        } else if err < 0 {
            let error_string = try!(errors::get_error_string(err));

            Err(ErrorKind::StatFailed(c_obj.into_rust_string().unwrap().into(), error_string)
                    .into())
        } else {
            Ok(true)
        }
    }


    pub fn exists_async<T: IntoFfiString>(&self, obj: T) -> Result<RadosFuture<RadosFinishExists>> {
        let c_obj = obj.into_ffi_string()?;

        let c_obj_ptr = c_obj.as_ref().as_ptr();

        let mut boxed = Box::new((0, 0));

        let (size_ptr, time_ptr) = (&mut boxed.0 as *mut u64, &mut boxed.1 as *mut time_t);

        let read_info = RadosFinishExists {
            oid: c_obj.into_rust_string().unwrap().into(),
            boxed,
        };

        RadosFuture::new(RadosCaution::Complete, read_info, |completion_handle| {
            let err = unsafe {
                rados::rados_aio_stat(self.handle,
                                      c_obj_ptr,
                                      completion_handle,
                                      size_ptr,
                                      time_ptr)
            };

            if err < 0 {
                Err(try!(errors::get_error_string(err)).into())
            } else {
                Ok(())
            }
        })
    }


    pub fn flush(&self) -> Result<()> {
        // BUG: `rados_aio_flush` always returns 0
        // http://docs.ceph.com/docs/master/rados/api/librados/#rados_aio_flush
        //
        // This function returns a `Result` because in the future `rados_aio_flush`
        // may change to return an error code.

        let err = unsafe { rados::rados_aio_flush(self.handle) };

        if err < 0 {
            Err(ErrorKind::FlushFailed(try!(errors::get_error_string(err))).into())
        } else {
            Ok(())
        }
    }


    pub fn flush_async(&self) -> Result<RadosFuture<RadosFinishFlush>> {
        RadosFuture::new(RadosCaution::Safe, RadosFinishFlush, |completion_handle| {
            let err = unsafe { rados::rados_aio_flush_async(self.handle, completion_handle) };

            if err < 0 {
                Err(try!(errors::get_error_string(err)).into())
            } else {
                Ok(())
            }
        })
    }
}
