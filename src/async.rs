//! Wrappers around `rados_completion_t`, providing a safe, futures-based API
//! for asynchronous RADOS operations.

use std::ffi::CString;
use std::ptr;
use std::sync::Mutex;

use ceph_rust::rados::{self, rados_completion_t};
use chrono::{Local, TimeZone};
use futures::{Async, Future, Poll};
use futures::task::{self, Task};
use libc::{c_void, time_t, ENOENT};

use errors::{self, Error, ErrorKind, Result, get_error_string};
use rados::RadosStat;


/// An abstraction over the handling of RADOS completions which have finished execution.
pub trait Finalize {
    type Output;

    /// On success, we produce output from the state stored in the `Self` type.
    fn on_success(self) -> Self::Output;

    /// On failure, we may either recover from the raw error code or convert the raw error code
    /// into a nicely readable error.
    fn on_failure(self, u32) -> Result<Self::Output>;
}


struct CompletionInfo {
    caution: RadosCaution,
    task: Mutex<Option<Task>>,
}


extern "C" fn complete_callback(_handle: rados_completion_t, info_ptr: *mut c_void) {
    unsafe {
        let info = &*(info_ptr as *const CompletionInfo);

        match *info.task.lock().unwrap() {
            Some(ref task) if info.caution == RadosCaution::Complete => task.notify(),
            _ => {}
        }
    }
}


extern "C" fn safe_callback(_handle: rados_completion_t, info_ptr: *mut c_void) {
    unsafe {
        let info = &*(info_ptr as *const CompletionInfo);

        match *info.task.lock().unwrap() {
            Some(ref task) => task.notify(),
            _ => {}
        }
    }
}


/// Asynchronous I/O through RADOS is abstracted behind the `RadosFuture` type.  Different
/// behaviors corresponding to different AIO operations are implemented as differing type
/// parameters implementing `Finalize`. Use of the `Finalize` trait instead of directly
/// implementing `Future` for every unique AIO operation reduces the risk of incorrectly
/// implementing the different completion modes (called `RadosCaution` here, we allow AIO
/// operations to wait for either acknowledgement from the cluster or for the assurance that the
/// operation has been committed to stable memory.)
pub struct RadosFuture<F: Finalize> {
    info: Box<CompletionInfo>,
    finalize: Option<F>,
    handle: rados_completion_t,
}


/// The `RadosCaution` enum is used to denote the level of safety the caller desires from an
/// asynchronous operation.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum RadosCaution {
    /// Wait for the operation to be acknowledged by the cluster. This indicates
    /// the operation is in-memory in the cluster, but has not yet necessarily
    /// been written to disk.
    Complete,

    /// Wait for the operation to be guaranteed to be successfully written to
    /// disk.
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
    /// Create a new `RadosFuture` given a function to initialize the completion, a caution level,
    /// and a finalizer. `rados_aio_release_completion` should *not* be called on the
    /// `rados_completion_t` passed into the initializer.
    pub fn new<G>(caution: RadosCaution, finalize: F, init: G) -> Result<RadosFuture<F>>
        where G: FnOnce(rados_completion_t) -> Result<()>
    {
        let mut completion_handle = ptr::null_mut();

        let mut info = Box::new(CompletionInfo {
                                    task: Mutex::new(None),
                                    caution,
                                });
        let info_ptr = info.as_mut() as *mut CompletionInfo;

        let err = unsafe {
            rados::rados_aio_create_completion(info_ptr as *mut c_void,
                                               Some(complete_callback),
                                               Some(safe_callback),
                                               &mut completion_handle)
        };

        if err < 0 {
            Err(ErrorKind::CompletionFailed(try!(errors::get_error_string(-err as u32))).into())
        } else {
            init(completion_handle).map(|()| {
                RadosFuture {
                    info,
                    finalize: Some(finalize),
                    handle: completion_handle,
                }
            })
        }
    }


    /// Poll the underlying `rados_completion_t` to check whether the operation has been acked by
    /// the cluster.
    pub fn is_complete(&self) -> bool {
        unsafe { rados::rados_aio_is_complete(self.handle) != 0 }
    }


    /// Poll the underlying `rados_completion_t` to check whether the operation has been committed
    /// to stable memory.
    pub fn is_safe(&self) -> bool {
        unsafe { rados::rados_aio_is_safe(self.handle) != 0 }
    }


    /// Get the error value of the completion, if the completion has errored.
    ///
    /// *Precondition:* this function should only be called after the completion is complete or
    /// safe.
    ///
    /// If an error has occurred, the raw error code is returned as a u32.
    pub fn get_error_value(&self) -> Option<u32> {
        let err = unsafe { rados::rados_aio_get_return_value(self.handle) };

        if err < 0 { Some(-err as u32) } else { None }
    }


    /// Take the finalizer value out of the `RadosFuture`. This is called when the future is
    /// complete and will not be `poll`ed again.
    ///
    /// *Precondition:* the future is done executing, and has not had its value read yet. If
    /// `take_finalize` is called again after the future is finished executing, it will panic due
    /// to the underlying call to `Option::take`.
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
        println!("Polling future with caution {:?}... Complete? {}; Safe? {}; Error value: {:?}",
                 self.info.caution,
                 self.is_complete(),
                 self.is_safe(),
                 self.get_error_value());
        match (self.info.caution, self.get_error_value()) {
            // If `self.get_error_value()` is `Some`, an error has occurred. We can only be certain
            // that an error has occurred if the operation has been acked or is safe, so we check
            // to ensure that one of the two is true before returning an error.
            (_, Some(error)) => {
                // Since we cannot bind by-move into a pattern guard, we must resort to an
                // if-statement here.
                if self.is_complete() || self.is_safe() {
                    self.take_finalize().on_failure(error).map(Async::Ready)
                } else {
                    Ok(Async::NotReady)
                }
            }

            (RadosCaution::Complete, None) if self.is_complete() || self.is_safe() => {
                Ok(Async::Ready(self.take_finalize().on_success()))
            }
            (RadosCaution::Safe, None) if self.is_safe() => {
                Ok(Async::Ready(self.take_finalize().on_success()))
            }

            _ => {
                *self.info.task.lock().unwrap() = Some(task::current());
                Ok(Async::NotReady)
            }
        }
    }
}


/// The finalizer for a `rados_aio_write`.
pub struct RadosFinishWrite {
    pub oid: CString,
    pub len: usize,
    pub off: u64,
}


impl Finalize for RadosFinishWrite {
    type Output = ();

    fn on_success(self) -> () {
        ()
    }

    fn on_failure(self, error: u32) -> Result<()> {
        Err(ErrorKind::AsyncWriteFailed(self.oid, self.len, self.off, get_error_string(error)?)
                .into())
    }
}


/// The finalizer for a `rados_aio_append`.
pub struct RadosFinishAppend {
    pub oid: CString,
    pub len: usize,
}


impl Finalize for RadosFinishAppend {
    type Output = ();

    fn on_success(self) -> () {
        ()
    }

    fn on_failure(self, error: u32) -> Result<()> {
        Err(ErrorKind::AsyncAppendFailed(self.oid, self.len, get_error_string(error)?).into())
    }
}


/// The finalizer for a `rados_aio_write_full`.
pub struct RadosFinishFullWrite {
    pub oid: CString,
    pub len: usize,
}


impl Finalize for RadosFinishFullWrite {
    type Output = ();

    fn on_success(self) -> () {
        ()
    }
    fn on_failure(self, error: u32) -> Result<()> {
        Err(ErrorKind::AsyncFullWriteFailed(self.oid, self.len, get_error_string(error)?).into())
    }
}


/// The finalizer for a `rados_aio_remove`.
pub struct RadosFinishRemove {
    pub oid: CString,
}


impl Finalize for RadosFinishRemove {
    type Output = ();

    fn on_success(self) -> () {
        ()
    }
    fn on_failure(self, error: u32) -> Result<()> {
        Err(ErrorKind::AsyncRemoveFailed(self.oid, get_error_string(error)?).into())
    }
}


/// The finalizer for a `rados_aio_flush`.
pub struct RadosFinishFlush;


impl Finalize for RadosFinishFlush {
    type Output = ();

    fn on_success(self) -> () {
        ()
    }
    fn on_failure(self, error: u32) -> Result<()> {
        Err(ErrorKind::AsyncFlushFailed(get_error_string(error)?).into())
    }
}


/// The finalizer for a `rados_aio_read`.
///
/// NOTE: as per the librados documentation, [`rados_aio_read` does not ever become "safe" - only
/// "complete"/acked - because it makes no sense for a read operation to become committed to
/// cluster memory.](http://docs.ceph.com/docs/master/rados/api/librados/#rados_aio_read)
pub struct RadosFinishRead<'a> {
    // The location in which to store the read data. We hold onto these until
    // they are guaranteed written.
    pub bytes: &'a mut [u8],

    pub oid: CString,
    pub off: u64,
}


impl<'a> Finalize for RadosFinishRead<'a> {
    type Output = &'a mut [u8];

    fn on_success(self) -> &'a mut [u8] {
        self.bytes
    }

    fn on_failure(self, error: u32) -> Result<&'a mut [u8]> {
        Err(ErrorKind::AsyncReadFailed(self.oid,
                                       self.bytes.len(),
                                       self.off,
                                       get_error_string(error)?)
                    .into())
    }
}


/// The finalizer for a `rados_aio_stat`.
pub struct RadosFinishStat {
    pub oid: CString,

    // We keep the size and time boxed so that their locations remain stable until their values are
    // finally read. Alternatively this could be done with references and lifetime parameters like
    // `RadosFinishRead` but this would be unintuitive to the user as `RadosContext::stat` does not
    // require input in which to store the read stats.
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

    fn on_failure(self, error: u32) -> Result<RadosStat> {
        Err(ErrorKind::AsyncStatFailed(self.oid, get_error_string(error)?).into())
    }
}


/// The finalizer for checking whether or not an object exists. This is implemented as a call to
/// `rados_aio_stat`.
pub struct RadosFinishExists {
    pub oid: CString,
}


impl Finalize for RadosFinishExists {
    type Output = bool;

    fn on_success(self) -> bool {
        true
    }

    fn on_failure(self, error: u32) -> Result<bool> {
        // We are checking for existence of the object. If we receive an `ENOENT` error (entry not
        // found) then we assume that indicates the object does not exist.
        if error as i32 == ENOENT {
            Ok(false)
        } else {
            Err(ErrorKind::AsyncExistsFailed(self.oid, get_error_string(error)?).into())
        }
    }
}
