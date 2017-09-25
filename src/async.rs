//! Wrappers around `rados_completion_t`, providing a safe, futures-based API
//! for asynchronous RADOS operations.

use std::mem;
use std::ptr;
use std::sync::Arc;

use ceph_rust::rados::{self, rados_completion_t};
use futures::{Async, Future, Poll};
use futures::task::AtomicTask;
use libc;

use errors::{self, Error, Result};


/// The result of a `Completion`'s successful execution.
#[derive(Debug)]
pub struct Return<T> {
    /// The data previously stored in the `Completion<T>`.
    pub data: T,

    /// The non-error return value of the RADOS completion.
    pub value: u32,
}


/// The info struct passed into a RADOS callback, providing a trigger to potentially deallocate
/// associated data and also an `AtomicTask` object for notifying all registered tasks.
struct CompletionInfo<T> {
    task: Arc<AtomicTask>,
    data: Arc<T>,
}


/// The callback passed into librados, and called on future completion.
extern "C" fn callback<T>(_handle: rados_completion_t, info_ptr: *mut libc::c_void) {
    let CompletionInfo { task, data } =
        *unsafe { Box::from_raw(info_ptr as *mut CompletionInfo<T>) };

    // Allow a poll to unwrap the contained data.
    mem::drop(data);

    // `AtomicTask` should be notified *after* data is produced. Data is produced, here, by
    // reducing the strong reference count of the `data: Arc<T>` to `1`, and thus allowing a
    // successful `.poll()` to `Arc::try_unwrap()` the data.
    task.notify();
}


/// The type of a wrapped `rados_completion_t`, with associated allocated custom data and
/// `AtomicTask`. This is a bare-metal `RadosFuture`.
#[derive(Debug)]
pub struct Completion<T> {
    task: Arc<AtomicTask>,
    data: Option<Arc<T>>,
    handle: rados_completion_t,
}


impl<T> Completion<T> {
    /// Construct a new `Completion` from a piece of data and an initialization function. The
    /// initialization function takes in a `rados_completion_t` and is intended to call a
    /// `rados_aio_*` function on it, which will manipulate the completion's internal state and
    /// return an error code, which can be reified to a `Result<()>` using `errors::librados`.
    pub fn new<F>(data: T, init: F) -> Result<Completion<T>>
    where
        F: FnOnce(rados_completion_t) -> Result<()>,
    {
        let mut completion_handle = ptr::null_mut();

        let task = Arc::new(AtomicTask::new());
        let data = Arc::new(data);

        let info_ptr = Box::into_raw(Box::new(CompletionInfo {
            task: task.clone(),
            data: data.clone(),
        }));

        let callback_ptr = callback::<T> as extern "C" fn(*mut libc::c_void, *mut libc::c_void);

        // Kraken and later Ceph releases *make no distinction* between the "acked" and "complete"
        // callbacks. As such, we are free to use strictly the "complete" callback.
        errors::librados(unsafe {
            rados::rados_aio_create_completion(
                info_ptr as *mut libc::c_void,
                Some(callback_ptr),
                None,
                &mut completion_handle,
            )
        })?;

        match init(completion_handle) {
            Ok(()) => {
                Ok(Completion {
                    task,
                    data: Some(data),
                    handle: completion_handle,
                })
            }
            Err(error) => {
                unsafe {
                    rados::rados_aio_release(completion_handle);
                }

                Err(error)
            }
        }
    }
}


impl<T> Future for Completion<T> {
    type Item = Return<T>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // `AtomicTask` should have `.register()` called before a consumer checks for produced data.
        self.task.register();

        let value =
            errors::librados_res(unsafe { rados::rados_aio_get_return_value(self.handle) })?;

        match Arc::try_unwrap(self.data.take().unwrap()) {
            Ok(data) => Ok(Async::Ready(Return { value, data })),
            Err(arc) => {
                self.data = Some(arc);
                Ok(Async::NotReady)
            }
        }
    }
}


impl<T> Drop for Completion<T> {
    fn drop(&mut self) {
        unsafe {
            rados::rados_aio_release(self.handle);
        }
    }
}


/// Conceptually, the `T` is only ever accessed from this `Completion`. Even when dropped, the
/// `T` is never accessed in a concurrent manner; and thus if `T` is `Send`, `Completion<T>`
/// is `Send`.
///
/// Another way to look at it is this: while `T` is inside an `Arc` at all, *it is only (if ever)
/// accessed by FFI code*, which has the responsibility of ensuring its accesses remain sane. As
/// such, we are free to simply mandate `T` is `Send`, as the responsibility of ensuring `Sync`
/// accesses to whatever buffer `T` is being used as falls to the foreign code.
unsafe impl<T: Send> Send for Completion<T> {}
