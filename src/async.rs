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


struct CompletionInfo<T> {
    task: Arc<AtomicTask>,
    data: Arc<T>,
}


extern "C" fn callback<T>(_handle: rados_completion_t, info_ptr: *mut libc::c_void) {
    let CompletionInfo { task, data } =
        *unsafe { Box::from_raw(info_ptr as *mut CompletionInfo<T>) };

    mem::drop(data);
    task.notify();
}


#[derive(Debug)]
pub struct Completion<T> {
    task: Arc<AtomicTask>,
    data: Option<Arc<T>>,
    handle: rados_completion_t,
}


impl<T> Completion<T> {
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

        init(completion_handle)?;

        Ok(Completion {
            task,
            data: Some(data),
            handle: completion_handle,
        })
    }
}


impl<T> Future for Completion<T> {
    type Item = T;
    type Error = Error;

    fn poll(&mut self) -> Poll<T, Error> {
        self.task.register();

        errors::librados(unsafe { rados::rados_aio_get_return_value(self.handle) })?;

        match Arc::try_unwrap(self.data.take().unwrap()) {
            Ok(data) => Ok(Async::Ready(data)),
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


// impl<T> RadosFuture<T> {
//     pub fn read<B: StableDeref + DerefMut<Target = [u8]>>(
//         handle: rados_ioctx_t,
//         object_id: &CStr,
//         buf: B,
//         offset: u64,
//     ) -> Read<B> {
//         let buf_ptr = buf.as_mut_ptr() as *mut lib::c_char;
//         let buf_len = buf.len();
//
//         let completion_res =
//             Completion::new(Caution::Complete, buf, |completion_handle| {
//                 errors::librados(unsafe {
//                     rados::rados_aio_read(handle, object_id.as_ptr(), buf_ptr, buf_len, offset)
//                 })
//             }).map_err(Some);
//
//         Read { completion_res }
//     }
//
//
//     pub fn write(
//         handle: rados_ioctx_t,
//         caution: Caution,
//         object_id: &Cstr,
//         buf: &[u8],
//         offset: u64,
//     ) -> Write {
//         let completion_res = Completion::new(caution, (), |completion_handle| {
//             errors::librados(unsafe {
//                 rados::rados_aio_write(
//                     handle,
//                     object_id.as_ptr(),
//                     completion_handle,
//                     buf.as_ptr() as *const libc::c_char,
//                     buf.len(),
//                     offset,
//                 )
//             })
//         }).map_err(Some);
//
//         Write { completion_res }
//     }
// }
//
//
// impl Future for Write {
//     type Item = ();
//     type Error = Error;
//
//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         match self.completion_res.as_mut() {
//             Ok(ref mut completion) => completion.poll(),
//             Err(ref mut error) => error.take().unwrap(),
//         }
//     }
// }
//
//
// pub struct Append {
//     completion_res: StdResult<Completion<()>, Option<Error>>,
// }
//
//
// impl Append {
//     pub fn new(
//         handle: rados_ioctx_t,
//         caution: Caution,
//         object_id: &CStr,
//         buf: &[u8],
//     ) -> Append {
//         Completion::new(caution, (), |completion_handle| {
//             errors::librados(unsafe {
//                 rados::rados_aio_append(
//                     handle,
//                     object_id.as_ptr(),
//                     completion_handle,
//                     buf.as_ptr(),
//                     buf.len(),
//                 )
//             })
//         }).into_future()
//             .flatten()
//     }
// }
//
//
// pub type Remove = Flatten<FutureResult<Completion<()>, Error>>;
//
//
// pub fn remove(handle: rados_ioctx_t, caution: Caution, object_id: &CStr) -> Remove {
//     Completion::new(caution, (), |completion_handle| {
//         errors::librados(unsafe {
//             rados::rados_aio_remove(handle, object_id.as_ptr(), completion_handle)
//         })
//     }).into_future()
//         .flatten()
// }
//
//
// pub type Flush = Flatten<FutureResult<Completion<()>, Error>>;
//
//
// pub fn flush(handle: rados_ioctx_t) -> Flush {
//     Completion::new(Caution::Safe, (), |completion_handle| {
//         errors::librados(unsafe {
//             rados::rados_aio_flush_async(handle, completion_handle)
//         })
//     }).into_future()
//         .flatten()
// }
//
//
// pub type Stat = Map<
//     Flatten<FutureResult<Completion<Box<(u64, libc::time_t)>>, Error>>,
//     fn(Box<(u64, libc::time_t)>) -> RadosStat,
// >;
//
//
// fn stat_map(data: Box<(u64, libc::time_t)>) -> RadosStat {
//     RadosStat {
//         size: data.0,
//         last_modified: Local.timestamp(data.1, 0),
//     }
// }
//
//
// pub fn stat(handle: rados_ioctx_t, object_id: &CStr) -> Stat {
//     let data = Box::new((0, 0));
//     let (size_ptr, time_ptr) = (&mut data.0 as *mut u64, &mut data.1 as *mut libc::time_t);
//
//     Completion::new(Caution::Complete, data, |completion_handle| {
//         errors::librados(unsafe {
//             rados::rados_aio_stat(
//                 handle,
//                 object_id.as_ptr(),
//                 completion_handle,
//                 size_ptr,
//                 time_ptr,
//             )
//         })
//     }).into_future()
//         .flatten()
//         .map(stat_map)
// }
//
//
// pub struct Exists {}
