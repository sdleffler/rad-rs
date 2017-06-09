use std::ffi::CStr;

use libc::{self, c_int};


error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    links {}

    foreign_links {
        Nul(::std::ffi::NulError);
        FromBytesWithNul(::std::ffi::FromBytesWithNulError);
        IntoString(::std::ffi::IntoStringError);
        Utf8(::std::str::Utf8Error);
        IO(::std::io::Error);
    }

    errors {
        ConnectFailed(e: String) {
            description("unable to connect to cluster")
            display("unable to connect to cluster: `{}`", e)
        }

        IoCtxFailed(pool: String, e: String) {
            description("unable to create I/O context")
            display("unable to create I/O context: `{}`", e)
        }

        CompletionFailed(e: String) {
            description("unable to create completion")
            display("unable to create completion: `{}`", e)
        }

        WriteFailed(oid: String, len: usize, offset: u64, e: String) {
            description("unable to write to object")
            display("unable to write {} bytes at offset {} to object `{}`: \
                     `{}`", len, offset, oid, e)
        }

        FullWriteFailed(oid: String, len: usize, e: String) {
            description("unable to full-write to object")
            display("unable to full-write {} bytes to object `{}`: `{}`", len, oid, e)
        }

        WriteSameFailed(oid: String, len: usize, write_len: usize, offset: u64, e: String) {
            description("unable to write-same to object")
            display("unable to write-same {} bytes repeated to cover {} bytes \
                     starting at offset {} to object `{}`: `{}`", len, write_len, offset, oid, e)
        }

        AppendFailed(oid: String, len: usize, e: String) {
            description("unable to append to object")
            display("unable to append {} bytes to object `{}`: `{}`", len, oid, e)
        }

        ReadFailed(oid: String, bufsize: usize, offset: u64, e: String) {
            description("unable to read from object")
            display("unable to read into a buffer of {} bytes at offset {} from \
                     object `{}`: `{}`", bufsize, offset, oid, e)
        }

        RemoveFailed(oid: String, e: String) {
            description("unable to delete object")
            display("unable to delete object `{}`: `{}`", oid, e)
        }

        TruncFailed(oid: String, size: u64, e: String) {
            description("unable to resize object")
            display("unable to resize object `{}` to a size of {} bytes: `{}`", oid, size, e)
        }

        AsyncWriteFailed(oid: String, len: usize, offset: u64, e: String) {
            description("unable to asynchronously write to object")
            display("unable to asynchronously write {} bytes at offset {} to object `{}`: `{}`",
                    len, offset, oid, e)
        }

        AsyncAppendFailed(oid: String, len: usize, e: String) {
            description("unable to asynchronously append to object")
            display("unable to asynchronously append {} bytes to object `{}`: `{}`", len, oid, e)
        }

        AsyncFullWriteFailed(oid: String, len: usize, e: String) {
            description("unable to asynchronously full-write to object")
            display("unable to asynchronously full-write {} bytes to object `{}`: `{}`", len, oid, e)
        }

        AsyncRemoveFailed(oid: String, e: String) {
            description("unable to asynchronously remove object")
            display("unable to asynchronously remove object `{}`: `{}`", oid, e)
        }

        FlushFailed(e: String) {
            description("unable to flush pending asynchronous operations")
            display("unable to flush pending asynchronous operations: `{}`", e)
        }

        AsyncFlushFailed(e: String) {
            description("unable to asynchronously flush pending asynchronous operations")
            display("unable to asynchronously flush pending asynchronous operations: `{}`", e)
        }

        AsyncReadFailed(oid: String, len: usize, off: u64, e: String) {
            description("unable to asynchronously read from object")
            display("unable to asynchronously read {} bytes at offset {} from object `{}`: `{}`",
                    len, off, oid, e)
        }

        StatFailed(oid: String, e: String) {
            description("unable to fetch object stats")
            display("unable to fetch stats for object `{}`: `{}`", oid, e)
        }

        ReadConfFromFileFailed(path: String, e: String) {
            description("unable to read configuration file from path")
            display("unable to read configuration file from path `{}`: `{}`", path, e)
        }

        CreateClusterHandleFailed(user: String, e: String) {
            description("unable to create cluster handle")
            display("unable to create cluster handle as user `{}`: `{}`", user, e)
        }

        ClusterStatFailed(e: String) {
            description("unable to retrieve cluster stats")
            display("unable to retrieve cluster stats: `{}`", e)
        }

        ConfSetFailed(option: String, value: String, e: String) {
            description("unable to set config option")
            display("unable to set config option `{}` to value `{}`: `{}`", option, value, e)
        }

        GetXAttrFailed(oid: String, key: String, e: String) {
            description("unable to get extended attribute")
            display("unable to get extended attribute `{}` on object `{}`: `{}`", key, oid, e)
        }

        SetXAttrFailed(oid: String, key: String, len: usize, e: String) {
            description("unable to set extended attribute")
            display("unable to set extended attribute `{}` to {} bytes of data on \
                     object `{}`: `{}`", key, len, oid, e)
        }

        InvalidUnicode {
            description("cannot convert OsString to String, as it contains invalid unicode")
            display("cannot convert OsString to String, as it contains invalid unicode")
        }

        AsyncStatFailed(oid: String, e: String) {
            description("unable to asynchronously fetch object stats")
            display("unable to asynchronously fetch stats for object `{}`: `{}`", oid, e)
        }

        AsyncExistsFailed(oid: String, e: String) {
            description("unable to asynchronously check for existence of object")
            display("unable to asynchronously check existence of object `{}`: `{}`", oid, e)
        }
    }
}


pub fn get_error_string(err: c_int) -> Result<String> {
    let error = unsafe {
        let err_str = libc::strerror(-err);
        try!(CStr::from_ptr(err_str)
                 .to_str()
                 .chain_err(|| "while decoding error string"))
    };

    Ok(error.to_string())
}
