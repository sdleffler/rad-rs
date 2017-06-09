use std::ptr;

use ceph_rust::rados::{self, rados_completion_t};
use chrono::{Local, TimeZone};
use futures::{Async, Future, Poll};
use libc::{time_t, ENOENT};

use errors::{self, Error, ErrorKind, Result, get_error_string};
use rados::RadosStat;


pub trait Finalize {
    type Output;

    fn on_success(self) -> Self::Output;
    fn on_failure(self, i32) -> Result<Self::Output>;
}


pub struct RadosFuture<F: Finalize> {
    caution: RadosCaution,
    finalize: Option<F>,
    handle: rados_completion_t,
}


#[derive(Clone, Copy, PartialEq, Debug)]
pub enum RadosCaution {
    Complete,
    Safe,
}


impl<F: Finalize> Drop for RadosFuture<F> {
    fn drop(&mut self) {
        unsafe {
            rados::rados_aio_release(self.handle);
        }
    }
}


impl<F: Finalize> RadosFuture<F> {
    pub fn new<G>(caution: RadosCaution, finalize: F, init: G) -> Result<RadosFuture<F>>
        where G: FnOnce(rados_completion_t) -> Result<()>
    {
        let mut completion_handle = ptr::null_mut();

        let err = unsafe {
            rados::rados_aio_create_completion(ptr::null_mut(), None, None, &mut completion_handle)
        };

        if err < 0 {
            Err(ErrorKind::CompletionFailed(try!(errors::get_error_string(err))).into())
        } else {
            init(completion_handle).map(|()| {
                RadosFuture {
                    caution,
                    finalize: Some(finalize),
                    handle: completion_handle,
                }
            })
        }
    }


    pub fn is_complete(&self) -> bool {
        unsafe { rados::rados_aio_is_complete(self.handle) != 0 }
    }


    pub fn is_safe(&self) -> bool {
        unsafe { rados::rados_aio_is_safe(self.handle) != 0 }
    }


    pub fn get_result_value(&self) -> Option<i32> {
        let err = unsafe { rados::rados_aio_get_return_value(self.handle) };

        if err < 0 { Some(err) } else { None }
    }


    fn take_finalize(&mut self) -> F {
        self.finalize
            .take()
            .expect("RadosFuture should not be polled a second time after completing!")
    }
}


impl<F: Finalize> Future for RadosFuture<F> {
    type Item = F::Output;
    type Error = Error;

    fn poll(&mut self) -> Poll<F::Output, Error> {
        match (self.caution, self.get_result_value()) {
            (_, Some(error)) => {
                // Cannot bind by-move into a pattern guard
                if self.is_complete() || self.is_safe() {
                    self.take_finalize().on_failure(error).map(Async::Ready)
                } else {
                    Ok(Async::NotReady)
                }
            }

            (RadosCaution::Complete, None) if self.is_complete() => {
                Ok(Async::Ready(self.take_finalize().on_success()))
            }
            (RadosCaution::Safe, None) if self.is_safe() => {
                Ok(Async::Ready(self.take_finalize().on_success()))
            }

            _ => Ok(Async::NotReady),
        }
    }
}


pub struct RadosFinishWrite {
    pub oid: String,
    pub len: usize,
    pub off: u64,
}


impl Finalize for RadosFinishWrite {
    type Output = ();

    fn on_success(self) -> () {
        ()
    }
    fn on_failure(self, error: i32) -> Result<()> {
        Err(ErrorKind::AsyncWriteFailed(self.oid, self.len, self.off, get_error_string(error)?)
                .into())
    }
}


pub struct RadosFinishAppend {
    pub oid: String,
    pub len: usize,
}


impl Finalize for RadosFinishAppend {
    type Output = ();

    fn on_success(self) -> () {
        ()
    }
    fn on_failure(self, error: i32) -> Result<()> {
        Err(ErrorKind::AsyncAppendFailed(self.oid, self.len, get_error_string(error)?).into())
    }
}


pub struct RadosFinishFullWrite {
    pub oid: String,
    pub len: usize,
}


impl Finalize for RadosFinishFullWrite {
    type Output = ();

    fn on_success(self) -> () {
        ()
    }
    fn on_failure(self, error: i32) -> Result<()> {
        Err(ErrorKind::AsyncFullWriteFailed(self.oid, self.len, get_error_string(error)?).into())
    }
}


pub struct RadosFinishRemove {
    pub oid: String,
}


impl Finalize for RadosFinishRemove {
    type Output = ();

    fn on_success(self) -> () {
        ()
    }
    fn on_failure(self, error: i32) -> Result<()> {
        Err(ErrorKind::AsyncRemoveFailed(self.oid, get_error_string(error)?).into())
    }
}


pub struct RadosFinishFlush;


impl Finalize for RadosFinishFlush {
    type Output = ();

    fn on_success(self) -> () {
        ()
    }
    fn on_failure(self, error: i32) -> Result<()> {
        Err(ErrorKind::AsyncFlushFailed(get_error_string(error)?).into())
    }
}


pub struct RadosFinishRead<'a> {
    pub bytes: &'a mut [u8],
    pub oid: String,
    pub off: u64,
}


impl<'a> Finalize for RadosFinishRead<'a> {
    type Output = &'a mut [u8];

    fn on_success(self) -> &'a mut [u8] {
        self.bytes
    }

    fn on_failure(self, error: i32) -> Result<&'a mut [u8]> {
        Err(ErrorKind::AsyncReadFailed(self.oid,
                                       self.bytes.len(),
                                       self.off,
                                       get_error_string(error)?)
                    .into())
    }
}


pub struct RadosFinishStat {
    pub oid: String,
    pub boxed: Box<(u64, time_t)>,
}


impl Finalize for RadosFinishStat {
    type Output = RadosStat;

    fn on_success(self) -> RadosStat {
        let (size, time) = *self.boxed;

        RadosStat {
            size,
            last_modified: Local.timestamp(time, 0),
        }
    }

    fn on_failure(self, error: i32) -> Result<RadosStat> {
        Err(ErrorKind::AsyncStatFailed(self.oid, get_error_string(error)?).into())
    }
}


pub struct RadosFinishExists {
    pub oid: String,
    pub boxed: Box<(u64, time_t)>,
}


impl Finalize for RadosFinishExists {
    type Output = bool;

    fn on_success(self) -> bool {
        true
    }

    fn on_failure(self, error: i32) -> Result<bool> {
        if -error == ENOENT {
            Ok(false)
        } else {
            Err(ErrorKind::AsyncExistsFailed(self.oid, get_error_string(error)?).into())
        }
    }
}
