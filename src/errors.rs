use std::ffi::CStr;

use libc;

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    links {}

    foreign_links {
        Nul(::ffi_pool::NulError);
        NulStd(::std::ffi::NulError);
        FromBytesWithNul(::std::ffi::FromBytesWithNulError);
        IntoString(::std::ffi::IntoStringError);
        Utf8(::std::str::Utf8Error);
        IO(::std::io::Error);
    }

    errors {
        Rados(e: u32) {
            description("RADOS error")
            display("RADOS error code {}: `{}`", e, get_error_string(*e).unwrap())
        }
    }
}

/// Convert the integer output of a librados API function into a `Result<()>`.
pub fn librados(err: i32) -> Result<()> {
    if err < 0 {
        bail!(ErrorKind::Rados(-err as u32));
    } else {
        Ok(())
    }
}

/// Convert the integer output of a librados API function into a `Result<u32>`, returning the error
/// value casted to a `u32` if it's positive and returning `Err` otherwise.
pub fn librados_res(err: i32) -> Result<u32> {
    if err < 0 {
        bail!(ErrorKind::Rados(-err as u32));
    } else {
        Ok(err as u32)
    }
}

/// Get the registered error string for a given error number.
pub fn get_error_string(err: u32) -> Result<String> {
    let error = unsafe {
        let err_str = libc::strerror(err as i32);
        CStr::from_ptr(err_str)
            .to_str()
            .chain_err(|| "while decoding error string")?
    };

    Ok(error.to_string())
}
