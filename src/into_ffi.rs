use std::ffi::{CString, CStr};
use std::path::PathBuf;

use as_ptr::AsPtr;
use libc::c_char;

use errors::{ErrorKind, Result};


/// A C-FFI compatible type which can be *stably* converted into a Rust string -
/// i.e. pointers gleaned from the old value will not be invalidated by conversion.
/// For example, converting a `CString` into a `String` keeps the same allocation,
/// so it does not invalidate pointers taken from the old `CString`.
pub trait IntoRustString {
    type Str: AsRef<str> + Into<String>;

    fn into_rust_string(self) -> Result<Self::Str>;
}


impl IntoRustString for String {
    type Str = String;

    fn into_rust_string(self) -> Result<String> {
        Ok(self)
    }
}


impl<'a> IntoRustString for &'a str {
    type Str = &'a str;

    fn into_rust_string(self) -> Result<&'a str> {
        Ok(self)
    }
}


impl IntoRustString for CString {
    type Str = String;

    fn into_rust_string(self) -> Result<String> {
        self.into_string().map_err(Into::into)
    }
}


impl<'a> IntoRustString for &'a CStr {
    type Str = &'a str;

    fn into_rust_string(self) -> Result<&'a str> {
        self.to_str().map_err(Into::into)
    }
}


/// A type which can be converted, efficiently or not, into an FFI-compatible
/// string.
pub trait IntoFfiString {
    type Ffi: AsPtr<c_char> + IntoRustString;

    fn into_ffi_string(self) -> Result<Self::Ffi>;
}


impl IntoFfiString for String {
    type Ffi = CString;

    fn into_ffi_string(self) -> Result<CString> {
        CString::new(self).map_err(Into::into)
    }
}


impl<'a, T: AsRef<str>> IntoFfiString for &'a T {
    type Ffi = CString;

    fn into_ffi_string(self) -> Result<CString> {
        self.as_ref().into_ffi_string()
    }
}


impl<'a> IntoFfiString for &'a str {
    type Ffi = CString;

    fn into_ffi_string(self) -> Result<CString> {
        CString::new(self).map_err(Into::into)
    }
}


impl IntoFfiString for CString {
    type Ffi = CString;

    fn into_ffi_string(self) -> Result<CString> {
        Ok(self)
    }
}


impl<'a> IntoFfiString for &'a CStr {
    type Ffi = &'a CStr;

    fn into_ffi_string(self) -> Result<&'a CStr> {
        Ok(self)
    }
}


impl IntoFfiString for PathBuf {
    type Ffi = CString;

    fn into_ffi_string(self) -> Result<CString> {
        CString::new(try!(self.into_os_string().into_string().map_err(|_| ErrorKind::InvalidUnicode))).map_err(Into::into)
    }
}
