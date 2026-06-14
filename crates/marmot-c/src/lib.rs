//! C bindings for the Marmot runtime.
//!
//! This crate exposes [`marmot_app`]'s app runtime through a stable, minimal C
//! ABI so consumers that cannot pull in a UniFFI runtime — embedded targets,
//! C/C++ apps, and FFI from languages without UniFFI support (Zig, Nim, Go,
//! Lua, …) — can drive the same runtime the Swift/Kotlin bindings use.
//!
//! # ABI shape
//!
//! The surface is deliberately tiny so the hand-audited header stays small and
//! the ABI stays stable as the Rust runtime grows:
//!
//! - [`marmot_c_open`] / [`marmot_c_free`] construct and destroy an opaque
//!   handle (`MarmotC *`). The handle owns its own tokio runtime.
//! - [`marmot_c_start`] / [`marmot_c_shutdown`] / [`marmot_c_is_stopping`]
//!   drive the runtime lifecycle.
//! - [`marmot_c_call`] is the single command entrypoint: it takes a method name
//!   plus a JSON request string and returns a JSON response string. New runtime
//!   methods are additive dispatch arms — they never change the exported symbol
//!   set. See [`dispatch`] for the method catalogue.
//! - [`marmot_c_string_free`] frees any `char *` the library returned
//!   (responses and error messages). Callers MUST use this, not `free(3)`.
//!
//! # Memory & threading
//!
//! - Every `char *` returned through an out-parameter is heap-allocated by Rust
//!   (`CString`) and must be released with [`marmot_c_string_free`]. Passing a
//!   pointer not produced by this library, or freeing twice, is undefined
//!   behaviour.
//! - The handle is `Send + Sync`-safe to call from multiple threads; the owned
//!   tokio runtime serializes async work via `block_on`. A single blocking call
//!   occupies the calling thread until it completes.
//! - All entrypoints are null-checked and return [`MARMOT_C_STATUS_INVALID_ARGUMENT`]
//!   rather than dereferencing a null pointer.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

mod dispatch;
mod runtime;

pub use dispatch::dispatch;
pub use runtime::{MarmotC, MarmotCError};

/// Status code: success.
pub const MARMOT_C_STATUS_OK: i32 = 0;
/// Status code: a required pointer was null or a string was not valid UTF-8.
pub const MARMOT_C_STATUS_INVALID_ARGUMENT: i32 = 1;
/// Status code: the dispatch method name is unknown.
pub const MARMOT_C_STATUS_UNKNOWN_METHOD: i32 = 2;
/// Status code: request/response JSON (de)serialization failed.
pub const MARMOT_C_STATUS_JSON: i32 = 3;
/// Status code: the underlying marmot runtime returned an error.
pub const MARMOT_C_STATUS_RUNTIME: i32 = 4;

/// Allocate a C string copy of `value`, returning a raw pointer the caller owns
/// and must release with [`marmot_c_string_free`]. Interior NULs are replaced
/// (they cannot occur in our JSON / UTF-8 payloads, but we never want to panic
/// across the FFI boundary).
fn into_c_string(value: String) -> *mut c_char {
    match CString::new(value) {
        Ok(cstring) => cstring.into_raw(),
        Err(err) => {
            let nul_pos = err.nul_position();
            let mut bytes = err.into_vec();
            bytes.truncate(nul_pos);
            // SAFETY: truncated at the first NUL, so the buffer is NUL-free.
            CString::new(bytes).unwrap_or_default().into_raw()
        }
    }
}

/// Write `message` into `*error_out` (if non-null) as an owned C string.
fn write_error(error_out: *mut *mut c_char, message: String) {
    if !error_out.is_null() {
        // SAFETY: caller guarantees `error_out` points to a writable pointer
        // slot (checked non-null above).
        unsafe { *error_out = into_c_string(message) };
    }
}

/// Read a borrowed `&str` from a C string pointer, or an error describing why
/// it could not be read.
///
/// SAFETY: `ptr` must be null or a valid NUL-terminated C string that stays
/// alive for the duration of the call.
unsafe fn str_from_ptr<'a>(ptr: *const c_char, name: &str) -> Result<&'a str, MarmotCError> {
    if ptr.is_null() {
        return Err(MarmotCError::InvalidArgument(format!("{name} is null")));
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|_| MarmotCError::InvalidArgument(format!("{name} is not valid UTF-8")))
}

/// Open a Marmot runtime rooted at `root_path` with newline-separated default
/// relay URLs in `relay_urls` (may be null/empty for none).
///
/// On success returns [`MARMOT_C_STATUS_OK`] and writes the handle to
/// `*handle_out`. On failure returns a non-zero status, leaves `*handle_out`
/// null, and (when `error_out` is non-null) writes an owned error string to
/// `*error_out`.
///
/// # Safety
///
/// `root_path` must be a valid C string. `handle_out` must be a valid,
/// writable pointer slot. `relay_urls`/`error_out` may be null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn marmot_c_open(
    root_path: *const c_char,
    relay_urls: *const c_char,
    handle_out: *mut *mut MarmotC,
    error_out: *mut *mut c_char,
) -> i32 {
    if handle_out.is_null() {
        write_error(error_out, "handle_out is null".to_owned());
        return MARMOT_C_STATUS_INVALID_ARGUMENT;
    }
    // SAFETY: checked non-null.
    unsafe { *handle_out = ptr::null_mut() };

    let root = match unsafe { str_from_ptr(root_path, "root_path") } {
        Ok(value) => value,
        Err(err) => {
            let code = err.code();
            write_error(error_out, err.to_string());
            return code;
        }
    };

    let relays: Vec<String> = if relay_urls.is_null() {
        Vec::new()
    } else {
        match unsafe { str_from_ptr(relay_urls, "relay_urls") } {
            Ok(value) => value
                .lines()
                .map(|line| line.trim().to_owned())
                .filter(|line| !line.is_empty())
                .collect(),
            Err(err) => {
                let code = err.code();
                write_error(error_out, err.to_string());
                return code;
            }
        }
    };

    match MarmotC::open(root, relays) {
        Ok(kit) => {
            let boxed = Box::new(kit);
            // SAFETY: checked non-null above.
            unsafe { *handle_out = Box::into_raw(boxed) };
            MARMOT_C_STATUS_OK
        }
        Err(err) => {
            let code = err.code();
            write_error(error_out, err.to_string());
            code
        }
    }
}

/// Bring the runtime online.
///
/// # Safety
///
/// `handle` must be a live handle from [`marmot_c_open`]; `error_out` may be
/// null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn marmot_c_start(handle: *mut MarmotC, error_out: *mut *mut c_char) -> i32 {
    let Some(kit) = (unsafe { handle.as_ref() }) else {
        write_error(error_out, "handle is null".to_owned());
        return MARMOT_C_STATUS_INVALID_ARGUMENT;
    };
    match kit.start() {
        Ok(()) => MARMOT_C_STATUS_OK,
        Err(err) => {
            let code = err.code();
            write_error(error_out, err.to_string());
            code
        }
    }
}

/// Tear the runtime down (does not free the handle).
///
/// # Safety
///
/// `handle` must be a live handle from [`marmot_c_open`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn marmot_c_shutdown(handle: *mut MarmotC) {
    if let Some(kit) = unsafe { handle.as_ref() } {
        kit.shutdown();
    }
}

/// Return 1 if the runtime is shutting down, 0 otherwise (or if `handle` is
/// null).
///
/// # Safety
///
/// `handle` must be null or a live handle from [`marmot_c_open`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn marmot_c_is_stopping(handle: *const MarmotC) -> i32 {
    match unsafe { handle.as_ref() } {
        Some(kit) if kit.is_stopping() => 1,
        _ => 0,
    }
}

/// Invoke `method` with the JSON `request_json` (may be null/empty, treated as
/// `{}`). On success returns [`MARMOT_C_STATUS_OK`] and writes an owned JSON
/// response string to `*response_out`. On failure returns a non-zero status and
/// (when `error_out` is non-null) writes an owned error string to `*error_out`.
///
/// Both returned strings must be released with [`marmot_c_string_free`].
///
/// # Safety
///
/// `handle` must be a live handle; `method` a valid C string;
/// `response_out` a valid writable pointer slot. `request_json`/`error_out`
/// may be null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn marmot_c_call(
    handle: *mut MarmotC,
    method: *const c_char,
    request_json: *const c_char,
    response_out: *mut *mut c_char,
    error_out: *mut *mut c_char,
) -> i32 {
    if response_out.is_null() {
        write_error(error_out, "response_out is null".to_owned());
        return MARMOT_C_STATUS_INVALID_ARGUMENT;
    }
    // SAFETY: checked non-null.
    unsafe { *response_out = ptr::null_mut() };

    let Some(kit) = (unsafe { handle.as_ref() }) else {
        write_error(error_out, "handle is null".to_owned());
        return MARMOT_C_STATUS_INVALID_ARGUMENT;
    };

    let method_str = match unsafe { str_from_ptr(method, "method") } {
        Ok(value) => value,
        Err(err) => {
            let code = err.code();
            write_error(error_out, err.to_string());
            return code;
        }
    };

    let request = if request_json.is_null() {
        ""
    } else {
        match unsafe { str_from_ptr(request_json, "request_json") } {
            Ok(value) => value,
            Err(err) => {
                let code = err.code();
                write_error(error_out, err.to_string());
                return code;
            }
        }
    };

    match dispatch(kit, method_str, request) {
        Ok(response) => {
            // SAFETY: checked non-null above.
            unsafe { *response_out = into_c_string(response) };
            MARMOT_C_STATUS_OK
        }
        Err(err) => {
            let code = err.code();
            write_error(error_out, err.to_string());
            code
        }
    }
}

/// Free a `char *` previously returned by this library (a response or error
/// string). Null is ignored.
///
/// # Safety
///
/// `ptr` must be null or a pointer returned by this library and not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn marmot_c_string_free(ptr: *mut c_char) {
    if !ptr.is_null() {
        // SAFETY: `ptr` came from `CString::into_raw`.
        drop(unsafe { CString::from_raw(ptr) });
    }
}

/// Free a handle returned by [`marmot_c_open`]. Implicitly shuts the runtime
/// down. Null is ignored.
///
/// # Safety
///
/// `handle` must be null or a handle from [`marmot_c_open`] not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn marmot_c_free(handle: *mut MarmotC) {
    if !handle.is_null() {
        // SAFETY: `handle` came from `Box::into_raw`.
        let kit = unsafe { Box::from_raw(handle) };
        kit.shutdown();
        drop(kit);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::sync::Once;

    /// `MarmotC::open` opens a keychain-backed secret store, which on the real
    /// targets is always present but in headless CI (Linux Secret Service, no
    /// D-Bus daemon) is not. Install an in-memory mock as the default keyring
    /// store before constructing; `AccountHome` short-circuits its own platform
    /// init when a default store already exists, so this exercises the real
    /// constructor path on every platform without touching a real keychain.
    /// Mirrors `marmot-uniffi`'s smoke-test setup.
    fn install_mock_keyring() {
        static KEYRING_INIT: Once = Once::new();
        KEYRING_INIT.call_once(|| {
            if keyring_core::get_default_store().is_none() {
                let store = keyring_core::mock::Store::new().expect("create mock keyring store");
                keyring_core::set_default_store(store);
            }
        });
    }

    fn take_string(ptr: *mut c_char) -> String {
        assert!(!ptr.is_null());
        let owned = unsafe { CStr::from_ptr(ptr) }.to_str().unwrap().to_owned();
        unsafe { marmot_c_string_free(ptr) };
        owned
    }

    fn open_kit() -> (*mut MarmotC, tempfile::TempDir) {
        install_mock_keyring();
        let dir = tempfile::tempdir().unwrap();
        let root = CString::new(dir.path().to_str().unwrap()).unwrap();
        let mut handle: *mut MarmotC = ptr::null_mut();
        let mut err: *mut c_char = ptr::null_mut();
        let status = unsafe { marmot_c_open(root.as_ptr(), ptr::null(), &mut handle, &mut err) };
        assert_eq!(status, MARMOT_C_STATUS_OK, "open failed: {:?}", unsafe {
            err.as_ref().map(|_| take_string(err))
        });
        assert!(!handle.is_null());
        (handle, dir)
    }

    #[test]
    fn open_call_list_and_free() {
        let (handle, _dir) = open_kit();

        // account.list on a fresh root returns an empty JSON array.
        let method = CString::new("account.list").unwrap();
        let mut response: *mut c_char = ptr::null_mut();
        let mut err: *mut c_char = ptr::null_mut();
        let status = unsafe {
            marmot_c_call(
                handle,
                method.as_ptr(),
                ptr::null(),
                &mut response,
                &mut err,
            )
        };
        assert_eq!(status, MARMOT_C_STATUS_OK);
        assert_eq!(take_string(response), "[]");
        assert!(err.is_null());

        unsafe { marmot_c_free(handle) };
    }

    #[test]
    fn unknown_method_reports_status() {
        let (handle, _dir) = open_kit();
        let method = CString::new("does.not.exist").unwrap();
        let mut response: *mut c_char = ptr::null_mut();
        let mut err: *mut c_char = ptr::null_mut();
        let status = unsafe {
            marmot_c_call(
                handle,
                method.as_ptr(),
                ptr::null(),
                &mut response,
                &mut err,
            )
        };
        assert_eq!(status, MARMOT_C_STATUS_UNKNOWN_METHOD);
        assert!(response.is_null());
        let message = take_string(err);
        assert!(message.contains("does.not.exist"), "got: {message}");
        unsafe { marmot_c_free(handle) };
    }

    #[test]
    fn null_handle_is_rejected_not_dereferenced() {
        let method = CString::new("account.list").unwrap();
        let mut response: *mut c_char = ptr::null_mut();
        let mut err: *mut c_char = ptr::null_mut();
        let status = unsafe {
            marmot_c_call(
                ptr::null_mut(),
                method.as_ptr(),
                ptr::null(),
                &mut response,
                &mut err,
            )
        };
        assert_eq!(status, MARMOT_C_STATUS_INVALID_ARGUMENT);
        assert!(response.is_null());
        assert!(!err.is_null());
        let _ = take_string(err);
    }

    #[test]
    fn invalid_group_id_hex_reports_runtime_argument_error() {
        let (handle, _dir) = open_kit();
        let method = CString::new("group.members").unwrap();
        let request =
            CString::new(r#"{"account_ref":"missing","group_id_hex":"zznothex"}"#).unwrap();
        let mut response: *mut c_char = ptr::null_mut();
        let mut err: *mut c_char = ptr::null_mut();
        let status = unsafe {
            marmot_c_call(
                handle,
                method.as_ptr(),
                request.as_ptr(),
                &mut response,
                &mut err,
            )
        };
        assert_eq!(status, MARMOT_C_STATUS_INVALID_ARGUMENT);
        assert!(response.is_null());
        let _ = take_string(err);
        unsafe { marmot_c_free(handle) };
    }
}
