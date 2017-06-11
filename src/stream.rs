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
        for future in self.futures.drain(..) {
            future.wait()?;
        }

        Ok(())
    }
}


/// A type providing an interface through which to write to a RADOS object.
/// Calls to `Write::write` on `RadosWriter` will perform their writes
/// asynchronously. Flushing will cause the writes to wait for either completion
/// or safety, as specified by the `RadosCaution` value passed to the
/// constructor.
pub struct RadosWriter<'a> {
    ctx: RadosContext<'a>,
    obj: CString,
    offset: u64,
    state: WriteState,
}


impl<'a> RadosWriter<'a> {
    pub fn new(ctx: RadosContext<'a>,
               caution: RadosCaution,
               obj: CString,
               offset: u64)
               -> RadosWriter<'a> {
        RadosWriter {
            ctx,
            obj,
            offset,
            state: WriteState {
                caution,
                futures: Vec::new(),
            },
        }
    }


    pub fn finish(mut self) -> Result<RadosContext<'a>> {
        self.flush()?;
        Ok(self.ctx)
    }
}


impl<'a> Write for RadosWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.state
            .do_write(&self.ctx, self.obj.as_ref(), buf, &mut self.offset)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))
    }


    fn flush(&mut self) -> io::Result<()> {
        self.state
            .do_flush()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))
    }
}


pub struct RadosReader<'a> {
    ctx: RadosContext<'a>,
    obj: CString,
    offset: u64,
}


impl<'a> Read for RadosReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.ctx
            .read(self.obj.as_ref(), buf, self.offset)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))
    }
}


pub struct RadosObject<'a> {
    ctx: RadosContext<'a>,
    obj: CString,
    offset: u64,
    write_state: WriteState,
}


impl<'a> Read for RadosObject<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.ctx
            .read(self.obj.as_ref(), buf, self.offset)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))
    }
}


impl<'a> Write for RadosObject<'a> {
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


impl<'a> Seek for RadosObject<'a> {
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
