//! Wrappers implementing `Read`, `Write`, and `Seek` for doing I/O on individual
//! RADOS objects.

use std::ffi::{CStr, CString};
use std::io::{self, SeekFrom, Read, Seek, Write};

use futures::Future;

use async::{RadosCaution, RadosFuture, RadosFinishWrite};
use errors::Result;
use rados::RadosContext;


struct WriteState {
    caution: RadosCaution,
    futures: Vec<RadosFuture<RadosFinishWrite>>,
}


impl WriteState {
    /// Asynchronously write to the RADOS cluster, and push the resulting
    /// future into a `Vec` so that all unresolved futures can be `.wait()`d
    /// later.
    fn do_write(&mut self,
                ctx: &RadosContext,
                obj: &CStr,
                buf: &[u8],
                offset: &mut u64)
                -> Result<usize> {
        let future = ctx.write_async(self.caution, obj, buf, *offset)?;
        self.futures.push(future);
        *offset += buf.len() as u64;
        Ok(buf.len())
    }


    /// Call `.wait()` on every saved future.
    fn do_flush(&mut self) -> Result<()> {
        for (i, future) in self.futures.drain(..).enumerate() {
            println!("Waiting on future number {}...", i);
            future.wait()?;
        }
        println!("All futures resolved.");

        Ok(())
    }
}


/// A `Read`/`Write`/`Seek` interface to a RADOS object.
///
/// `RadosObject` will perform asynchronous I/O calls on `Write`, and
/// synchronous calls on `Read`. If calling `Seek::seek` with
/// `SeekFrom::End`, a synchronous call to `rados_stat` will be made;
/// otherwise, seeking is very cheap and completely local.
pub struct RadosObject {
    ctx: RadosContext,
    obj: CString,
    offset: u64,
    write_state: WriteState,
}


impl RadosObject {
    pub fn new(ctx: RadosContext, caution: RadosCaution, obj: CString) -> RadosObject {
        RadosObject {
            ctx,
            obj,
            offset: 0,
            write_state: WriteState {
                caution,
                futures: Vec::new(),
            },
        }
    }


    /// Calling `finish` will *not* cause asynchronous writes to be
    /// flushed. To block on write completion, use `Write::flush`.
    pub fn finish(self) -> RadosContext {
        self.ctx
    }
}


impl Read for RadosObject {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let res = self.ctx
            .read(self.obj.as_ref(), buf, self.offset)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        self.offset += res as u64;
        Ok(res)
    }
}


impl Write for RadosObject {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_state
            .do_write(&self.ctx, self.obj.as_ref(), buf, &mut self.offset)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))
    }


    fn flush(&mut self) -> io::Result<()> {
        self.write_state
            .do_flush()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))
    }
}


impl Seek for RadosObject {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        match from {
            SeekFrom::Start(u) => self.offset = u,
            SeekFrom::End(i) => {
                let length = self.ctx
                    .stat(self.obj.as_ref())
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?
                    .size;

                self.offset = if i < 0 {
                    length - (-i as u64)
                } else {
                    length + (i as u64)
                };
            }
            SeekFrom::Current(i) => {
                if i < 0 {
                    self.offset -= -i as u64;
                } else {
                    self.offset += i as u64;
                }
            }
        }

        Ok(self.offset)
    }
}
