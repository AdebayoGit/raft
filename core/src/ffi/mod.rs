//! C ABI layer — stable FFI interface for all platform bindings.
//!
//! The database is exposed as an opaque handle (`*mut RaftDb`) with
//! `rft_`-prefixed free functions. All errors are returned as a
//! [`RftError`] C enum. Memory ownership rules:
//!
//! - The caller owns the `RaftDb` handle and must call [`rft_close`] to
//!   free it.
//! - For [`rft_get`], the callee writes into a caller-provided buffer.
//!   If the buffer is too small, [`RftError::BufferTooSmall`] is
//!   returned and `out_len` is set to the required size.
//! - Key/value byte slices are borrowed for the duration of each call.
//!
//! Gated behind the `ffi` feature flag.

mod error;
mod handle;

pub use error::RftError;
pub use handle::RaftDb;

use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;
use std::slice;

use crate::{StorageConfig, StorageEngine};

/// Open or create a database at `path`.
///
/// Returns a non-null handle on success, or null on failure (check
/// `out_err` for the error code).
///
/// # Safety
///
/// - `path` must be a valid null-terminated UTF-8 C string.
/// - `out_err` must be a valid pointer to an `RftError`.
#[no_mangle]
pub unsafe extern "C" fn rft_open(path: *const c_char, out_err: *mut RftError) -> *mut RaftDb {
    if path.is_null() {
        if !out_err.is_null() {
            ptr::write(out_err, RftError::NullPointer);
        }
        return ptr::null_mut();
    }

    let c_str = match CStr::from_ptr(path).to_str() {
        Ok(s) => s,
        Err(_) => {
            if !out_err.is_null() {
                ptr::write(out_err, RftError::InvalidUtf8);
            }
            return ptr::null_mut();
        }
    };

    match StorageEngine::open(c_str, StorageConfig::default()) {
        Ok(engine) => {
            if !out_err.is_null() {
                ptr::write(out_err, RftError::Ok);
            }
            Box::into_raw(Box::new(RaftDb::new(engine)))
        }
        Err(_) => {
            if !out_err.is_null() {
                ptr::write(out_err, RftError::IoError);
            }
            ptr::null_mut()
        }
    }
}

/// Close and free a database handle.
///
/// # Safety
///
/// - `db` must be a handle returned by [`rft_open`], or null (no-op).
/// - After this call, `db` is dangling and must not be used.
#[no_mangle]
pub unsafe extern "C" fn rft_close(db: *mut RaftDb) {
    if !db.is_null() {
        drop(Box::from_raw(db));
    }
}

/// Insert or update a key-value pair.
///
/// # Safety
///
/// - `db` must be a valid, non-null handle from [`rft_open`].
/// - `key` must point to at least `key_len` readable bytes.
/// - `value` must point to at least `value_len` readable bytes.
#[no_mangle]
pub unsafe extern "C" fn rft_put(
    db: *mut RaftDb,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> RftError {
    let Some(handle) = ptr_to_handle(db) else {
        return RftError::NullPointer;
    };
    if key.is_null() || value.is_null() {
        return RftError::NullPointer;
    }

    let key_slice = slice::from_raw_parts(key, key_len);
    let value_slice = slice::from_raw_parts(value, value_len);

    match handle.engine_mut().put(key_slice.to_vec(), value_slice.to_vec()) {
        Ok(()) => RftError::Ok,
        Err(_) => RftError::IoError,
    }
}

/// Look up a key.
///
/// On success, writes the value into the caller-provided buffer at
/// `out_value` and sets `*out_len` to the number of bytes written.
///
/// If the buffer is too small, returns [`RftError::BufferTooSmall`] and
/// sets `*out_len` to the required size (no bytes are written).
///
/// If the key is not found, returns [`RftError::NotFound`].
///
/// # Safety
///
/// - `db` must be a valid, non-null handle from [`rft_open`].
/// - `key` must point to at least `key_len` readable bytes.
/// - `out_value` must point to a buffer of at least `*out_len` writable
///   bytes, or be null if only querying the required size.
/// - `out_len` must be a valid, non-null pointer to a `usize`.
#[no_mangle]
pub unsafe extern "C" fn rft_get(
    db: *mut RaftDb,
    key: *const u8,
    key_len: usize,
    out_value: *mut u8,
    out_len: *mut usize,
) -> RftError {
    let Some(handle) = ptr_to_handle(db) else {
        return RftError::NullPointer;
    };
    if key.is_null() || out_len.is_null() {
        return RftError::NullPointer;
    }

    let key_slice = slice::from_raw_parts(key, key_len);

    let value = match handle.engine().get(key_slice) {
        Ok(Some(v)) => v,
        Ok(None) => return RftError::NotFound,
        Err(_) => return RftError::IoError,
    };

    let required = value.len();
    let capacity = ptr::read(out_len);

    if out_value.is_null() || capacity < required {
        ptr::write(out_len, required);
        return RftError::BufferTooSmall;
    }

    ptr::copy_nonoverlapping(value.as_ptr(), out_value, required);
    ptr::write(out_len, required);

    RftError::Ok
}

/// Delete a key.
///
/// Returns [`RftError::Ok`] on success. Deleting a non-existent key is
/// not an error (it writes a tombstone).
///
/// # Safety
///
/// - `db` must be a valid, non-null handle from [`rft_open`].
/// - `key` must point to at least `key_len` readable bytes.
#[no_mangle]
pub unsafe extern "C" fn rft_delete(
    db: *mut RaftDb,
    key: *const u8,
    key_len: usize,
) -> RftError {
    let Some(handle) = ptr_to_handle(db) else {
        return RftError::NullPointer;
    };
    if key.is_null() {
        return RftError::NullPointer;
    }

    let key_slice = slice::from_raw_parts(key, key_len);

    match handle.engine_mut().delete(key_slice.to_vec()) {
        Ok(()) => RftError::Ok,
        Err(_) => RftError::IoError,
    }
}

/// Convert a raw pointer to a mutable handle reference, or `None` if null.
///
/// # Safety
///
/// The pointer must be null or a valid `RaftDb` pointer from `rft_open`.
unsafe fn ptr_to_handle<'a>(db: *mut RaftDb) -> Option<&'a mut RaftDb> {
    if db.is_null() {
        None
    } else {
        Some(&mut *db)
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir()
            .join("raft_db_ffi_tests")
            .join(name);
        if dir.exists() {
            std::fs::remove_dir_all(&dir).unwrap();
        }
        dir
    }

    /// Helper: open a DB via FFI, returning the handle and dir path.
    unsafe fn open_test_db(name: &str) -> (*mut RaftDb, std::path::PathBuf) {
        let dir = temp_dir(name);
        let path = CString::new(dir.to_str().unwrap()).unwrap();
        let mut err = RftError::Ok;
        let db = rft_open(path.as_ptr(), &mut err);
        assert!(!db.is_null(), "rft_open failed: {err:?}");
        assert_eq!(err, RftError::Ok);
        (db, dir)
    }

    #[test]
    fn open_and_close() {
        unsafe {
            let (db, dir) = open_test_db("open_close");
            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn close_null_is_noop() {
        unsafe {
            rft_close(ptr::null_mut());
        }
    }

    #[test]
    fn open_null_path_returns_null() {
        unsafe {
            let mut err = RftError::Ok;
            let db = rft_open(ptr::null(), &mut err);
            assert!(db.is_null());
            assert_eq!(err, RftError::NullPointer);
        }
    }

    #[test]
    fn open_invalid_utf8_returns_null() {
        unsafe {
            let bad = b"\xff\xfe\x00";
            let mut err = RftError::Ok;
            let db = rft_open(bad.as_ptr() as *const c_char, &mut err);
            assert!(db.is_null());
            assert_eq!(err, RftError::InvalidUtf8);
        }
    }

    #[test]
    fn put_and_get() {
        unsafe {
            let (db, dir) = open_test_db("put_get");

            let key = b"hello";
            let value = b"world";

            let err = rft_put(db, key.as_ptr(), key.len(), value.as_ptr(), value.len());
            assert_eq!(err, RftError::Ok);

            let mut buf = [0u8; 64];
            let mut out_len = buf.len();
            let err = rft_get(
                db,
                key.as_ptr(),
                key.len(),
                buf.as_mut_ptr(),
                &mut out_len,
            );
            assert_eq!(err, RftError::Ok);
            assert_eq!(out_len, 5);
            assert_eq!(&buf[..out_len], b"world");

            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn get_not_found() {
        unsafe {
            let (db, dir) = open_test_db("get_not_found");

            let key = b"missing";
            let mut buf = [0u8; 64];
            let mut out_len = buf.len();
            let err = rft_get(
                db,
                key.as_ptr(),
                key.len(),
                buf.as_mut_ptr(),
                &mut out_len,
            );
            assert_eq!(err, RftError::NotFound);

            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn get_buffer_too_small() {
        unsafe {
            let (db, dir) = open_test_db("get_buf_small");

            let key = b"k";
            let value = b"a_longer_value";
            rft_put(db, key.as_ptr(), key.len(), value.as_ptr(), value.len());

            let mut buf = [0u8; 4]; // too small
            let mut out_len = buf.len();
            let err = rft_get(
                db,
                key.as_ptr(),
                key.len(),
                buf.as_mut_ptr(),
                &mut out_len,
            );
            assert_eq!(err, RftError::BufferTooSmall);
            assert_eq!(out_len, 14); // required size

            // Now allocate the right size and try again.
            let mut buf2 = vec![0u8; out_len];
            let mut out_len2 = buf2.len();
            let err = rft_get(
                db,
                key.as_ptr(),
                key.len(),
                buf2.as_mut_ptr(),
                &mut out_len2,
            );
            assert_eq!(err, RftError::Ok);
            assert_eq!(&buf2[..out_len2], b"a_longer_value");

            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn get_null_buffer_returns_required_size() {
        unsafe {
            let (db, dir) = open_test_db("get_null_buf");

            let key = b"k";
            let value = b"data";
            rft_put(db, key.as_ptr(), key.len(), value.as_ptr(), value.len());

            let mut out_len: usize = 0;
            let err = rft_get(
                db,
                key.as_ptr(),
                key.len(),
                ptr::null_mut(),
                &mut out_len,
            );
            assert_eq!(err, RftError::BufferTooSmall);
            assert_eq!(out_len, 4);

            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn delete_existing_key() {
        unsafe {
            let (db, dir) = open_test_db("delete_existing");

            let key = b"k";
            let value = b"v";
            rft_put(db, key.as_ptr(), key.len(), value.as_ptr(), value.len());

            let err = rft_delete(db, key.as_ptr(), key.len());
            assert_eq!(err, RftError::Ok);

            let mut buf = [0u8; 64];
            let mut out_len = buf.len();
            let err = rft_get(
                db,
                key.as_ptr(),
                key.len(),
                buf.as_mut_ptr(),
                &mut out_len,
            );
            assert_eq!(err, RftError::NotFound);

            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn delete_nonexistent_is_ok() {
        unsafe {
            let (db, dir) = open_test_db("delete_missing");

            let key = b"ghost";
            let err = rft_delete(db, key.as_ptr(), key.len());
            assert_eq!(err, RftError::Ok);

            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn null_db_handle_returns_error() {
        unsafe {
            let key = b"k";
            let value = b"v";

            assert_eq!(
                rft_put(ptr::null_mut(), key.as_ptr(), key.len(), value.as_ptr(), value.len()),
                RftError::NullPointer
            );

            let mut buf = [0u8; 64];
            let mut out_len = buf.len();
            assert_eq!(
                rft_get(
                    ptr::null_mut(),
                    key.as_ptr(),
                    key.len(),
                    buf.as_mut_ptr(),
                    &mut out_len,
                ),
                RftError::NullPointer
            );

            assert_eq!(
                rft_delete(ptr::null_mut(), key.as_ptr(), key.len()),
                RftError::NullPointer
            );
        }
    }

    #[test]
    fn null_key_returns_error() {
        unsafe {
            let (db, dir) = open_test_db("null_key");

            assert_eq!(
                rft_put(db, ptr::null(), 0, b"v".as_ptr(), 1),
                RftError::NullPointer
            );

            let mut buf = [0u8; 64];
            let mut out_len = buf.len();
            assert_eq!(
                rft_get(db, ptr::null(), 0, buf.as_mut_ptr(), &mut out_len),
                RftError::NullPointer
            );

            assert_eq!(rft_delete(db, ptr::null(), 0), RftError::NullPointer);

            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn null_value_on_put_returns_error() {
        unsafe {
            let (db, dir) = open_test_db("null_value");

            assert_eq!(
                rft_put(db, b"k".as_ptr(), 1, ptr::null(), 0),
                RftError::NullPointer
            );

            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn null_out_len_on_get_returns_error() {
        unsafe {
            let (db, dir) = open_test_db("null_out_len");

            let key = b"k";
            let mut buf = [0u8; 64];
            assert_eq!(
                rft_get(db, key.as_ptr(), key.len(), buf.as_mut_ptr(), ptr::null_mut()),
                RftError::NullPointer
            );

            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn put_overwrite_and_get() {
        unsafe {
            let (db, dir) = open_test_db("put_overwrite");

            let key = b"key";
            rft_put(db, key.as_ptr(), key.len(), b"old".as_ptr(), 3);
            rft_put(db, key.as_ptr(), key.len(), b"new".as_ptr(), 3);

            let mut buf = [0u8; 64];
            let mut out_len = buf.len();
            rft_get(db, key.as_ptr(), key.len(), buf.as_mut_ptr(), &mut out_len);
            assert_eq!(&buf[..out_len], b"new");

            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn empty_key_and_value() {
        unsafe {
            let (db, dir) = open_test_db("empty_kv");

            // Empty key, empty value — unusual but valid.
            let key: &[u8] = b"";
            let value: &[u8] = b"";
            // For empty slices, as_ptr() is valid but we must not pass null.
            let err = rft_put(db, key.as_ptr(), 0, value.as_ptr(), 0);
            assert_eq!(err, RftError::Ok);

            let mut buf = [0u8; 1];
            let mut out_len = buf.len();
            let err = rft_get(db, key.as_ptr(), 0, buf.as_mut_ptr(), &mut out_len);
            assert_eq!(err, RftError::Ok);
            assert_eq!(out_len, 0);

            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }
}
