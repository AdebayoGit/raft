//! C ABI layer — stable FFI interface for all platform bindings.
//!
//! The database is exposed as an opaque handle (`*mut RaftDb`) with
//! `rft_`-prefixed free functions. All errors are returned as a
//! [`RftError`] C enum.
//!
//! ## Surface
//!
//! - **Open** — [`rft_open`] (validated absolute path) and
//!   [`rft_open_at`] (path confined to an app-sandbox root; preferred
//!   for bindings).
//! - **KV ops** — [`rft_put`] / [`rft_get`] / [`rft_delete`]: low-level
//!   byte-key/byte-value access on the underlying engine.
//! - **Collection ops** — `rft_collection_*`: typed document CRUD with
//!   JSON-encoded documents.
//! - **Query** — `rft_query_*`: predicate query execution returning an
//!   opaque result handle.
//! - **Transactions** — `rft_transaction_*`: optimistic-concurrency batch
//!   reads/writes that commit atomically or roll back.
//! - **Observers** — [`rft_observe`] / [`rft_unobserve`]: register a C
//!   callback that fires on collection mutations.
//! - **Dart observers** — [`rft_dart_init`] + [`rft_observe_dart_port`] /
//!   [`rft_observe_query_dart_port`]: deliver events to a Dart
//!   `SendPort` via `Dart_PostCObject_DL` (the VM copies each message,
//!   so no callback-lifetime hazard).
//!
//! ## Memory ownership rules
//!
//! - The caller owns the `RaftDb` handle and must call [`rft_close`] to
//!   free it. Closing aborts any pending observer tasks.
//! - Query result handles ([`RaftQueryResult`]) and transaction handles
//!   ([`RaftTransaction`]) are also caller-owned; each has a matching
//!   `*_free`/`*_commit`/`*_rollback` function.
//! - Returned bytes (JSON, values) use a buffer-too-small protocol: pass
//!   `out_buf` + `*out_len`; on
//!   [`RftError::BufferTooSmall`](RftError::BufferTooSmall), `*out_len`
//!   holds the required size and no bytes are copied.
//! - Key/value byte slices are borrowed for the duration of each call.
//!
//! Gated behind the `ffi` feature flag, which also turns on `async`
//! (required by observers) and `serde_json`.

mod bulk;
mod collection;
mod dart_port;
mod error;
mod handle;
mod observe;
mod query;
mod registry;
mod transaction;

pub use bulk::{
    rft_buf_data, rft_buf_free, rft_buf_len, rft_collection_delete_many, rft_collection_get_buf,
    rft_collection_put_many, rft_collection_scan, RftBuf,
};
pub use collection::{
    rft_collection_count, rft_collection_delete, rft_collection_get, rft_collection_list_ids,
    rft_collection_put, rft_collection_put_auto,
};
pub use dart_port::{rft_dart_init, rft_observe_dart_port, rft_observe_query_dart_port};
pub use error::RftError;
pub use handle::RaftDb;
pub use observe::{rft_observe, rft_observe_query, rft_unobserve, RftObserveCallback};
pub use query::{
    rft_query_execute, rft_query_result_count, rft_query_result_free, rft_query_result_get,
    RaftQueryResult,
};
pub use transaction::{
    rft_transaction_begin, rft_transaction_commit, rft_transaction_delete, rft_transaction_get,
    rft_transaction_put, rft_transaction_rollback, RaftTransaction,
};

use std::ffi::CStr;
use std::os::raw::c_char;
use std::path::{Component, Path, PathBuf};
use std::ptr;
use std::slice;

use crate::database::Database;

/// Maximum accepted size, in bytes, of a document JSON envelope
/// (`rft_collection_put`, `rft_collection_put_auto`,
/// `rft_transaction_put`). Larger payloads are rejected with
/// [`RftError::PayloadTooLarge`].
pub const RFT_MAX_DOC_JSON_LEN: usize = 16 * 1024 * 1024;

/// Maximum accepted size, in bytes, of a query-spec JSON envelope
/// (`rft_query_execute`, `rft_observe_query`,
/// `rft_observe_query_dart_port`). Larger payloads are rejected with
/// [`RftError::PayloadTooLarge`].
pub const RFT_MAX_QUERY_JSON_LEN: usize = 64 * 1024;

/// Parse a document JSON envelope, enforcing the size cap. Shared by
/// the collection and transaction put paths.
fn document_from_json(bytes: &[u8]) -> Result<crate::query::Document, RftError> {
    if bytes.len() > RFT_MAX_DOC_JSON_LEN {
        return Err(RftError::PayloadTooLarge);
    }
    serde_json::from_slice(bytes).map_err(|_| RftError::InvalidJson)
}

/// Reject database paths that are empty or contain `..` components —
/// the traversal vector for attacker-influenced paths (deep links etc.).
fn validate_open_path(raw: &str) -> Result<(), RftError> {
    if raw.is_empty() {
        return Err(RftError::InvalidPath);
    }
    if Path::new(raw)
        .components()
        .any(|c| matches!(c, Component::ParentDir))
    {
        return Err(RftError::InvalidPath);
    }
    Ok(())
}

/// Resolve `name` strictly inside `root`, defeating both `..` traversal
/// and symlink escapes.
///
/// `name` must be relative with only normal components. `root` must
/// exist; it is canonicalized, and the deepest existing ancestor of the
/// joined path is canonicalized and required to stay under the root —
/// so a symlink planted inside the sandbox cannot redirect the database
/// outside it.
fn resolve_confined(root: &str, name: &str) -> Result<PathBuf, RftError> {
    if root.is_empty() || name.is_empty() {
        return Err(RftError::InvalidPath);
    }
    let name = Path::new(name);
    if !name
        .components()
        .all(|c| matches!(c, Component::Normal(_) | Component::CurDir))
    {
        return Err(RftError::InvalidPath);
    }

    let root = std::fs::canonicalize(root).map_err(|_| RftError::InvalidPath)?;
    let joined = root.join(name);

    // Canonicalize the deepest existing ancestor (the DB dir itself may
    // not exist yet) and verify it still resolves under the root.
    let mut existing = joined.as_path();
    while !existing.exists() {
        existing = existing.parent().ok_or(RftError::InvalidPath)?;
    }
    let resolved = std::fs::canonicalize(existing).map_err(|_| RftError::InvalidPath)?;
    if !resolved.starts_with(&root) {
        return Err(RftError::InvalidPath);
    }
    Ok(joined)
}

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
    guard_or(ptr::null_mut(), || {
        if !out_err.is_null() {
            // Pre-set the panic code so a panic later in the body still
            // reports a meaningful error alongside the null return.
            unsafe { ptr::write(out_err, RftError::InternalPanic) };
        }
        unsafe { rft_open_impl(path, out_err) }
    })
}

/// # Safety
///
/// Same contract as [`rft_open`].
unsafe fn rft_open_impl(path: *const c_char, out_err: *mut RftError) -> *mut RaftDb {
    if path.is_null() {
        if !out_err.is_null() {
            unsafe { ptr::write(out_err, RftError::NullPointer) };
        }
        return ptr::null_mut();
    }

    let c_str = match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            if !out_err.is_null() {
                unsafe { ptr::write(out_err, RftError::InvalidUtf8) };
            }
            return ptr::null_mut();
        }
    };

    if let Err(e) = validate_open_path(c_str) {
        if !out_err.is_null() {
            unsafe { ptr::write(out_err, e) };
        }
        return ptr::null_mut();
    }

    unsafe { open_database(Path::new(c_str), out_err) }
}

/// Open or create a database named `name` strictly inside the directory
/// `root` — the app-sandbox variant of [`rft_open`].
///
/// Bindings should prefer this over `rft_open`, passing their platform's
/// app-private storage directory (`getFilesDir()` on Android,
/// Application Support on iOS/macOS) as `root`. `name` must be a
/// relative path; `..` components, absolute paths, and symlinks that
/// escape `root` are rejected with [`RftError::InvalidPath`].
///
/// # Safety
///
/// - `root` and `name` must be valid null-terminated UTF-8 C strings.
/// - `out_err` must be a valid pointer to an `RftError`, or null.
#[no_mangle]
pub unsafe extern "C" fn rft_open_at(
    root: *const c_char,
    name: *const c_char,
    out_err: *mut RftError,
) -> *mut RaftDb {
    guard_or(ptr::null_mut(), || {
        if !out_err.is_null() {
            unsafe { ptr::write(out_err, RftError::InternalPanic) };
        }

        if root.is_null() || name.is_null() {
            if !out_err.is_null() {
                unsafe { ptr::write(out_err, RftError::NullPointer) };
            }
            return ptr::null_mut();
        }

        let (root, name) = match (
            unsafe { CStr::from_ptr(root) }.to_str(),
            unsafe { CStr::from_ptr(name) }.to_str(),
        ) {
            (Ok(r), Ok(n)) => (r, n),
            _ => {
                if !out_err.is_null() {
                    unsafe { ptr::write(out_err, RftError::InvalidUtf8) };
                }
                return ptr::null_mut();
            }
        };

        match resolve_confined(root, name) {
            Ok(path) => unsafe { open_database(&path, out_err) },
            Err(e) => {
                if !out_err.is_null() {
                    unsafe { ptr::write(out_err, e) };
                }
                ptr::null_mut()
            }
        }
    })
}

/// Shared tail of [`rft_open`] / [`rft_open_at`]: open the database at a
/// validated path and register the handle.
///
/// # Safety
///
/// `out_err` must be a valid pointer to an `RftError`, or null.
unsafe fn open_database(path: &Path, out_err: *mut RftError) -> *mut RaftDb {
    match Database::open(path) {
        Ok(db) => {
            if !out_err.is_null() {
                unsafe { ptr::write(out_err, RftError::Ok) };
            }
            let raw = Box::into_raw(Box::new(RaftDb::new(db)));
            registry::LIVE_DBS.register(raw);
            raw
        }
        Err(_) => {
            if !out_err.is_null() {
                unsafe { ptr::write(out_err, RftError::IoError) };
            }
            ptr::null_mut()
        }
    }
}

/// Close and free a database handle. Aborts any pending observer tasks,
/// then blocks until the observer runtime has fully shut down — after
/// this returns, no observer callback will ever fire again, so the
/// caller may safely free any `user_data` it passed to `rft_observe`.
///
/// # Safety
///
/// - `db` must be a handle returned by [`rft_open`], or null (no-op).
///   Passing an already-closed or foreign pointer is a safe no-op.
/// - No other thread may be using `db` concurrently with this call.
/// - Must not be called from inside an observer callback (deadlock).
/// - After this call, `db` is dangling and must not be used.
#[no_mangle]
pub unsafe extern "C" fn rft_close(db: *mut RaftDb) {
    guard_or((), || {
        // Unregister-wins: exactly one concurrent close proceeds; a
        // double-close or stale pointer is a safe no-op.
        if !db.is_null() && registry::LIVE_DBS.unregister(db) {
            let handle = unsafe { &*db };
            observe::abort_all_subscriptions(handle);
            // Blocks until all observer tasks have drained, then frees
            // the database. Prevents use-after-free between a late
            // callback and the dropped Database/user_data.
            unsafe { Box::from_raw(db) }.shutdown();
        }
    });
}

// ── Low-level KV ops (kept for backwards-compat) ───────────────────────
//
// These bypass the document layer and operate directly on the underlying
// engine via a small "raw" view exposed by `Database`. New code should
// prefer the collection / query / transaction APIs above.

/// Insert or update a key-value pair on the raw engine.
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
    guard(|| {
        let handle = match unsafe { live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        if (key.is_null() && key_len > 0) || (value.is_null() && value_len > 0) {
            return RftError::NullPointer;
        }

        let key_slice = unsafe { slice::from_raw_parts(key, key_len) };
        let value_slice = unsafe { slice::from_raw_parts(value, value_len) };

        match handle
            .database()
            .raw_put(key_slice.to_vec(), value_slice.to_vec())
        {
            Ok(()) => RftError::Ok,
            Err(_) => RftError::IoError,
        }
    })
}

/// Look up a key on the raw engine.
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
    guard(|| {
        let handle = match unsafe { live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        if (key.is_null() && key_len > 0) || out_len.is_null() {
            return RftError::NullPointer;
        }

        let key_slice = unsafe { slice::from_raw_parts(key, key_len) };

        let value = match handle.database().raw_get(key_slice) {
            Ok(Some(v)) => v,
            Ok(None) => return RftError::NotFound,
            Err(_) => return RftError::IoError,
        };

        unsafe { write_buffer(&value, out_value, out_len) }
    })
}

/// Delete a key on the raw engine.
///
/// Returns [`RftError::Ok`] on success. Deleting a non-existent key is
/// not an error (it writes a tombstone).
///
/// # Safety
///
/// - `db` must be a valid, non-null handle from [`rft_open`].
/// - `key` must point to at least `key_len` readable bytes.
#[no_mangle]
pub unsafe extern "C" fn rft_delete(db: *mut RaftDb, key: *const u8, key_len: usize) -> RftError {
    guard(|| {
        let handle = match unsafe { live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        if key.is_null() && key_len > 0 {
            return RftError::NullPointer;
        }

        let key_slice = unsafe { slice::from_raw_parts(key, key_len) };

        match handle.database().raw_delete(key_slice.to_vec()) {
            Ok(()) => RftError::Ok,
            Err(_) => RftError::IoError,
        }
    })
}

// ── Internal helpers ───────────────────────────────────────────────────

/// Run an FFI body, converting any Rust panic into
/// [`RftError::InternalPanic`] instead of unwinding across the C
/// boundary (which is undefined behaviour) or aborting the host app.
pub(crate) fn guard(f: impl FnOnce() -> RftError) -> RftError {
    guard_or(RftError::InternalPanic, f)
}

/// Like [`guard`] but for FFI functions that do not return [`RftError`]:
/// returns `fallback` if the body panics.
pub(crate) fn guard_or<T>(fallback: T, f: impl FnOnce() -> T) -> T {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(value) => value,
        Err(_) => {
            // Redaction (X5): the panic payload is deliberately not
            // logged — panic messages can embed user data.
            tracing::error!("panic caught at the FFI boundary");
            fallback
        }
    }
}

/// Resolve a database handle against the live registry.
///
/// Returns [`RftError::NullPointer`] for null and
/// [`RftError::InvalidHandle`] for a pointer that was never registered
/// or has already been closed — so a stale handle fails safely instead
/// of dereferencing freed memory.
///
/// # Safety
///
/// If `db` is live in the registry it was produced by [`rft_open`] and
/// not yet closed, so it points to a valid `RaftDb`.
pub(crate) unsafe fn live_db<'a>(db: *mut RaftDb) -> Result<&'a RaftDb, RftError> {
    if db.is_null() {
        return Err(RftError::NullPointer);
    }
    if !registry::LIVE_DBS.is_live(db) {
        return Err(RftError::InvalidHandle);
    }
    Ok(unsafe { &*db })
}

/// Resolve a transaction handle against the live registry. Same
/// contract as [`live_db`], but yields a mutable reference (transaction
/// handles are single-threaded by API contract).
///
/// # Safety
///
/// If `txn` is live it was produced by
/// [`rft_transaction_begin`](transaction::rft_transaction_begin) and not
/// yet finalised, so it points to a valid `RaftTransaction`. The caller
/// must not use the same transaction handle from multiple threads
/// concurrently.
pub(crate) unsafe fn live_txn<'a>(
    txn: *mut RaftTransaction,
) -> Result<&'a mut RaftTransaction, RftError> {
    if txn.is_null() {
        return Err(RftError::NullPointer);
    }
    if !registry::LIVE_TXNS.is_live(txn) {
        return Err(RftError::InvalidHandle);
    }
    Ok(unsafe { &mut *txn })
}

/// Resolve a query-result handle against the live registry. Same
/// contract as [`live_db`].
///
/// # Safety
///
/// If `result` is live it was produced by
/// [`rft_query_execute`](query::rft_query_execute) and not yet freed,
/// so it points to a valid `RaftQueryResult`.
pub(crate) unsafe fn live_query_result<'a>(
    result: *const RaftQueryResult,
) -> Result<&'a RaftQueryResult, RftError> {
    if result.is_null() {
        return Err(RftError::NullPointer);
    }
    if !registry::LIVE_QUERY_RESULTS.is_live(result) {
        return Err(RftError::InvalidHandle);
    }
    Ok(unsafe { &*result })
}

/// Standard "write `bytes` into caller buffer, fall back to size query"
/// pattern shared by all FFI functions that return variable-length data.
///
/// # Safety
///
/// - `out_len` must be a valid `*mut usize`.
/// - `out_buf` must be writable for at least the value of `*out_len`
///   bytes, or null.
pub(crate) unsafe fn write_buffer(bytes: &[u8], out_buf: *mut u8, out_len: *mut usize) -> RftError {
    let required = bytes.len();
    let capacity = unsafe { ptr::read(out_len) };

    if out_buf.is_null() || capacity < required {
        unsafe { ptr::write(out_len, required) };
        return RftError::BufferTooSmall;
    }

    unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr(), out_buf, required);
        ptr::write(out_len, required);
    }
    RftError::Ok
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join("raft_db_ffi_tests").join(name);
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
        let db = unsafe { rft_open(path.as_ptr(), &mut err) };
        assert!(!db.is_null(), "rft_open failed: {err:?}");
        assert_eq!(err, RftError::Ok);
        (db, dir)
    }

    #[test]
    fn guard_converts_panic_into_error_code() {
        let code = guard(|| panic!("injected panic"));
        assert_eq!(code, RftError::InternalPanic);
    }

    #[test]
    fn guard_or_returns_fallback_on_panic() {
        let count = guard_or(0usize, || panic!("injected panic"));
        assert_eq!(count, 0);
        let null = guard_or(ptr::null_mut::<RaftDb>(), || panic!("injected panic"));
        assert!(null.is_null());
    }

    #[test]
    fn close_blocks_until_observer_callbacks_drain() {
        use std::os::raw::c_void;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        static CLOSED: AtomicBool = AtomicBool::new(false);
        static FIRED_AFTER_CLOSE: AtomicBool = AtomicBool::new(false);
        static CALLS: AtomicUsize = AtomicUsize::new(0);

        unsafe extern "C" fn slow_callback(_event: *const c_char, _user_data: *mut c_void) {
            CALLS.fetch_add(1, Ordering::SeqCst);
            // Simulate a slow platform callback still running while the
            // host calls rft_close from another thread.
            std::thread::sleep(std::time::Duration::from_millis(60));
            if CLOSED.load(Ordering::SeqCst) {
                FIRED_AFTER_CLOSE.store(true, Ordering::SeqCst);
            }
        }

        unsafe {
            let (db, dir) = open_test_db("close_drains");
            let coll = std::ffi::CString::new("users").unwrap();

            let mut sub_id = 0u64;
            assert_eq!(
                rft_observe(
                    db,
                    coll.as_ptr(),
                    slow_callback,
                    ptr::null_mut(),
                    &mut sub_id
                ),
                RftError::Ok
            );

            // Trigger a mutation and give the observer task a moment to
            // enter the (slow) callback.
            let json = r#"{"id":1,"fields":{}}"#;
            rft_collection_put(db, coll.as_ptr(), json.as_ptr(), json.len());
            std::thread::sleep(std::time::Duration::from_millis(20));

            // Close must block until the in-flight callback completes.
            rft_close(db);
            CLOSED.store(true, Ordering::SeqCst);

            // Wait longer than the callback sleep; if close had returned
            // while the callback was still running, it would observe
            // CLOSED == true and set the violation flag.
            std::thread::sleep(std::time::Duration::from_millis(120));
            assert!(
                !FIRED_AFTER_CLOSE.load(Ordering::SeqCst),
                "observer callback outlived rft_close ({} calls)",
                CALLS.load(Ordering::SeqCst)
            );

            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn stale_and_double_freed_handles_fail_safely() {
        unsafe {
            let (db, dir) = open_test_db("stale_handles");
            let coll = CString::new("users").unwrap();

            // A pointer that was never registered is InvalidHandle, not UB.
            let fake_db = 0x1000 as *mut RaftDb; // aligned, never registered
            assert_eq!(
                rft_delete(fake_db, b"k".as_ptr(), 1),
                RftError::InvalidHandle
            );

            // Query result: double free is a safe no-op; use-after-free
            // returns InvalidHandle / 0.
            let q = r#"{"collection":"users"}"#;
            let mut result: *mut RaftQueryResult = ptr::null_mut();
            assert_eq!(
                rft_query_execute(db, q.as_ptr(), q.len(), &mut result),
                RftError::Ok
            );
            rft_query_result_free(result);
            rft_query_result_free(result); // double free: no-op
            assert_eq!(rft_query_result_count(result), 0);
            let mut len = 0usize;
            assert_eq!(
                rft_query_result_get(result, 0, ptr::null_mut(), &mut len),
                RftError::InvalidHandle
            );

            // Transaction: second finalise and ops after commit fail safely.
            let mut txn: *mut RaftTransaction = ptr::null_mut();
            assert_eq!(rft_transaction_begin(db, &mut txn), RftError::Ok);
            assert_eq!(rft_transaction_commit(txn), RftError::Ok);
            assert_eq!(rft_transaction_commit(txn), RftError::InvalidHandle);
            rft_transaction_rollback(txn); // safe no-op
            assert_eq!(
                rft_transaction_delete(txn, coll.as_ptr(), 1),
                RftError::InvalidHandle
            );

            // Database: double close is a safe no-op; ops on a closed
            // handle return InvalidHandle instead of crashing.
            rft_close(db);
            rft_close(db);
            let json = r#"{"id":1,"fields":{}}"#;
            assert_eq!(
                rft_collection_put(db, coll.as_ptr(), json.as_ptr(), json.len()),
                RftError::InvalidHandle
            );
            let mut count = 0usize;
            assert_eq!(
                rft_collection_count(db, coll.as_ptr(), &mut count),
                RftError::InvalidHandle
            );
            let mut txn2: *mut RaftTransaction = ptr::null_mut();
            assert_eq!(
                rft_transaction_begin(db, &mut txn2),
                RftError::InvalidHandle
            );

            std::fs::remove_dir_all(&dir).ok();
        }
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
    fn open_rejects_parent_dir_traversal() {
        unsafe {
            let dir = temp_dir("traversal_base");
            std::fs::create_dir_all(&dir).unwrap();
            let sneaky = format!("{}/../escaped_db", dir.to_str().unwrap());
            let path = CString::new(sneaky).unwrap();
            let mut err = RftError::Ok;
            let db = rft_open(path.as_ptr(), &mut err);
            assert!(db.is_null());
            assert_eq!(err, RftError::InvalidPath);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn open_rejects_empty_path() {
        unsafe {
            let path = CString::new("").unwrap();
            let mut err = RftError::Ok;
            let db = rft_open(path.as_ptr(), &mut err);
            assert!(db.is_null());
            assert_eq!(err, RftError::InvalidPath);
        }
    }

    #[test]
    fn open_at_confines_to_root() {
        unsafe {
            let root_dir = temp_dir("open_at_root");
            std::fs::create_dir_all(&root_dir).unwrap();
            let root = CString::new(root_dir.to_str().unwrap()).unwrap();
            let mut err = RftError::Ok;

            // Plain name inside the root works.
            let name = CString::new("mydb").unwrap();
            let db = rft_open_at(root.as_ptr(), name.as_ptr(), &mut err);
            assert!(!db.is_null(), "confined open failed: {err:?}");
            assert_eq!(err, RftError::Ok);
            rft_close(db);

            // `..` in the name is rejected.
            for bad in ["../outside", "a/../../outside", ".."] {
                let name = CString::new(bad).unwrap();
                let db = rft_open_at(root.as_ptr(), name.as_ptr(), &mut err);
                assert!(db.is_null(), "{bad} must be rejected");
                assert_eq!(err, RftError::InvalidPath, "{bad}");
            }

            // Absolute names are rejected.
            let abs = CString::new("/tmp/abs_db").unwrap();
            let db = rft_open_at(root.as_ptr(), abs.as_ptr(), &mut err);
            assert!(db.is_null());
            assert_eq!(err, RftError::InvalidPath);

            std::fs::remove_dir_all(&root_dir).ok();
        }
    }

    #[test]
    #[cfg(unix)]
    fn open_at_rejects_symlink_escape() {
        unsafe {
            let root_dir = temp_dir("open_at_symlink_root");
            let outside_dir = temp_dir("open_at_symlink_outside");
            std::fs::create_dir_all(&root_dir).unwrap();
            std::fs::create_dir_all(&outside_dir).unwrap();

            // Plant a symlink inside the sandbox pointing outside it.
            let link = root_dir.join("escape");
            std::os::unix::fs::symlink(&outside_dir, &link).unwrap();

            let root = CString::new(root_dir.to_str().unwrap()).unwrap();
            let name = CString::new("escape/db").unwrap();
            let mut err = RftError::Ok;
            let db = rft_open_at(root.as_ptr(), name.as_ptr(), &mut err);
            assert!(db.is_null(), "symlink escape must be rejected");
            assert_eq!(err, RftError::InvalidPath);

            std::fs::remove_dir_all(&root_dir).ok();
            std::fs::remove_dir_all(&outside_dir).ok();
        }
    }

    #[test]
    fn raw_put_and_get() {
        unsafe {
            let (db, dir) = open_test_db("raw_put_get");

            let key = b"hello";
            let value = b"world";

            assert_eq!(
                rft_put(db, key.as_ptr(), key.len(), value.as_ptr(), value.len()),
                RftError::Ok
            );

            let mut buf = [0u8; 64];
            let mut out_len = buf.len();
            assert_eq!(
                rft_get(db, key.as_ptr(), key.len(), buf.as_mut_ptr(), &mut out_len),
                RftError::Ok
            );
            assert_eq!(&buf[..out_len], b"world");

            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn collection_put_get_delete() {
        unsafe {
            let (db, dir) = open_test_db("coll_crud");
            let coll = CString::new("users").unwrap();

            let doc_json = r#"{"id":1,"fields":{"name":{"String":"Alice"}}}"#;
            assert_eq!(
                rft_collection_put(db, coll.as_ptr(), doc_json.as_ptr(), doc_json.len(),),
                RftError::Ok
            );

            let mut buf = vec![0u8; 256];
            let mut out_len = buf.len();
            assert_eq!(
                rft_collection_get(db, coll.as_ptr(), 1, buf.as_mut_ptr(), &mut out_len),
                RftError::Ok
            );
            let json = std::str::from_utf8(&buf[..out_len]).unwrap();
            assert!(json.contains("Alice"));

            assert_eq!(rft_collection_delete(db, coll.as_ptr(), 1), RftError::Ok);

            let mut out_len = buf.len();
            assert_eq!(
                rft_collection_get(db, coll.as_ptr(), 1, buf.as_mut_ptr(), &mut out_len),
                RftError::NotFound
            );

            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn collection_count_and_list_ids() {
        unsafe {
            let (db, dir) = open_test_db("coll_count");
            let coll = CString::new("u").unwrap();

            for i in 1u64..=3 {
                let json = format!(r#"{{"id":{i},"fields":{{}}}}"#);
                assert_eq!(
                    rft_collection_put(db, coll.as_ptr(), json.as_ptr(), json.len()),
                    RftError::Ok
                );
            }

            let mut count = 0usize;
            assert_eq!(
                rft_collection_count(db, coll.as_ptr(), &mut count),
                RftError::Ok
            );
            assert_eq!(count, 3);

            let mut ids = vec![0u64; 3];
            let mut len = ids.len();
            assert_eq!(
                rft_collection_list_ids(db, coll.as_ptr(), ids.as_mut_ptr(), &mut len),
                RftError::Ok
            );
            assert_eq!(len, 3);
            assert_eq!(ids, vec![1, 2, 3]);

            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn query_returns_filtered_docs() {
        unsafe {
            let (db, dir) = open_test_db("query_filter");
            let coll = CString::new("users").unwrap();

            for i in 1u64..=5 {
                let json = format!(
                    r#"{{"id":{i},"fields":{{"age":{{"Int":{}}}}}}}"#,
                    20 + i as i64 * 5,
                );
                rft_collection_put(db, coll.as_ptr(), json.as_ptr(), json.len());
            }

            // age >= 35 should yield 3 docs (35, 40, 45)
            let q = r#"{"collection":"users","filter":{"Condition":{"field":"age","predicate":"Gte","value":{"Int":35}}}}"#;
            let mut result: *mut RaftQueryResult = ptr::null_mut();
            assert_eq!(
                rft_query_execute(db, q.as_ptr(), q.len(), &mut result),
                RftError::Ok
            );
            assert!(!result.is_null());
            assert_eq!(rft_query_result_count(result), 3);

            // Read back doc 0
            let mut buf = vec![0u8; 256];
            let mut len = buf.len();
            assert_eq!(
                rft_query_result_get(result, 0, buf.as_mut_ptr(), &mut len),
                RftError::Ok
            );
            assert!(std::str::from_utf8(&buf[..len])
                .unwrap()
                .contains("\"Int\""));

            rft_query_result_free(result);
            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn transaction_commit_and_conflict() {
        unsafe {
            let (db, dir) = open_test_db("txn");
            let coll = CString::new("users").unwrap();

            // Seed a doc.
            let seed = r#"{"id":1,"fields":{"name":{"String":"Alice"}}}"#;
            rft_collection_put(db, coll.as_ptr(), seed.as_ptr(), seed.len());

            // Begin txn, read, modify, commit — succeeds.
            let mut txn: *mut RaftTransaction = ptr::null_mut();
            assert_eq!(rft_transaction_begin(db, &mut txn), RftError::Ok);
            let mut buf = vec![0u8; 256];
            let mut len = buf.len();
            assert_eq!(
                rft_transaction_get(txn, coll.as_ptr(), 1, buf.as_mut_ptr(), &mut len),
                RftError::Ok
            );
            let upd = r#"{"id":1,"fields":{"name":{"String":"Updated"}}}"#;
            assert_eq!(
                rft_transaction_put(txn, coll.as_ptr(), upd.as_ptr(), upd.len()),
                RftError::Ok
            );
            assert_eq!(rft_transaction_commit(txn), RftError::Ok);

            // Now a conflict scenario.
            let mut txn: *mut RaftTransaction = ptr::null_mut();
            assert_eq!(rft_transaction_begin(db, &mut txn), RftError::Ok);
            let mut len = buf.len();
            rft_transaction_get(txn, coll.as_ptr(), 1, buf.as_mut_ptr(), &mut len);

            // Concurrent write outside the txn.
            let outside = r#"{"id":1,"fields":{"name":{"String":"Concurrent"}}}"#;
            rft_collection_put(db, coll.as_ptr(), outside.as_ptr(), outside.len());

            let upd = r#"{"id":1,"fields":{"name":{"String":"FromTxn"}}}"#;
            rft_transaction_put(txn, coll.as_ptr(), upd.as_ptr(), upd.len());
            assert_eq!(rft_transaction_commit(txn), RftError::TransactionConflict);

            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn observe_fires_callback_on_mutation() {
        use std::os::raw::c_void;
        use std::sync::atomic::{AtomicUsize, Ordering};

        static COUNTER: AtomicUsize = AtomicUsize::new(0);

        unsafe extern "C" fn callback(_event: *const c_char, _user_data: *mut c_void) {
            COUNTER.fetch_add(1, Ordering::SeqCst);
        }

        unsafe {
            let (db, dir) = open_test_db("observe");
            let coll = CString::new("users").unwrap();

            let mut sub_id = 0u64;
            assert_eq!(
                rft_observe(db, coll.as_ptr(), callback, ptr::null_mut(), &mut sub_id,),
                RftError::Ok
            );
            assert!(sub_id > 0);

            // Generate a mutation.
            let json = r#"{"id":1,"fields":{}}"#;
            rft_collection_put(db, coll.as_ptr(), json.as_ptr(), json.len());

            // Give the observer task a moment to deliver.
            std::thread::sleep(std::time::Duration::from_millis(80));

            assert!(COUNTER.load(Ordering::SeqCst) >= 1);

            assert_eq!(rft_unobserve(db, sub_id), RftError::Ok);
            assert_eq!(rft_unobserve(db, sub_id), RftError::UnknownSubscription);

            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn observe_query_initial_snapshot_then_diffs() {
        use std::os::raw::c_void;
        use std::sync::Mutex;

        struct CapturedDiffs(Mutex<Vec<String>>);
        static DIFFS: std::sync::OnceLock<CapturedDiffs> = std::sync::OnceLock::new();

        fn diffs() -> &'static CapturedDiffs {
            DIFFS.get_or_init(|| CapturedDiffs(Mutex::new(Vec::new())))
        }

        unsafe extern "C" fn callback(json: *const c_char, _user_data: *mut c_void) {
            let s = unsafe { std::ffi::CStr::from_ptr(json) }
                .to_str()
                .unwrap()
                .to_string();
            diffs().0.lock().unwrap().push(s);
        }

        unsafe {
            let (db, dir) = open_test_db("observe_query");
            let coll = CString::new("users").unwrap();

            // Seed two docs before subscribing.
            for i in 1u64..=2 {
                let doc = format!(
                    r#"{{"id":{i},"fields":{{"age":{{"Int":{}}}}}}}"#,
                    20 + i as i64
                );
                rft_collection_put(db, coll.as_ptr(), doc.as_ptr(), doc.len());
            }

            // Subscribe to a query that matches all users.
            let q = r#"{"collection":"users"}"#;
            let mut sub_id = 0u64;
            assert_eq!(
                rft_observe_query(
                    db,
                    q.as_ptr(),
                    q.len(),
                    callback,
                    ptr::null_mut(),
                    &mut sub_id,
                ),
                RftError::Ok
            );

            // Initial snapshot should arrive synchronously.
            {
                let captured = diffs().0.lock().unwrap();
                assert_eq!(captured.len(), 1, "expected initial snapshot");
                let snapshot = &captured[0];
                assert!(
                    snapshot.contains("\"added\""),
                    "snapshot json should have added key: {snapshot}",
                );
                // Both seeded docs should be in the snapshot.
                assert!(snapshot.matches("\"id\":").count() == 2);
            }

            // Mutation: add a third doc → triggers a diff with one added.
            let doc3 = r#"{"id":3,"fields":{"age":{"Int":99}}}"#;
            rft_collection_put(db, coll.as_ptr(), doc3.as_ptr(), doc3.len());
            std::thread::sleep(std::time::Duration::from_millis(300));

            {
                let captured = diffs().0.lock().unwrap();
                assert!(
                    captured.len() >= 2,
                    "expected diff after mutation; got {} entries: {:?}",
                    captured.len(),
                    *captured
                );
                let last = &captured[captured.len() - 1];
                assert!(
                    last.contains("\"id\":3"),
                    "diff should include doc 3: {last}"
                );
            }

            assert_eq!(rft_unobserve(db, sub_id), RftError::Ok);
            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// Thread-affinity contract (F7e): mutation-observer callbacks fire
    /// on a runtime worker thread, never on the registering thread.
    #[test]
    fn observe_callbacks_fire_off_registering_thread() {
        use std::os::raw::c_void;
        use std::sync::Mutex;
        use std::thread::ThreadId;

        static THREADS: std::sync::OnceLock<Mutex<Vec<ThreadId>>> = std::sync::OnceLock::new();

        fn threads() -> &'static Mutex<Vec<ThreadId>> {
            THREADS.get_or_init(|| Mutex::new(Vec::new()))
        }

        unsafe extern "C" fn callback(_event: *const c_char, _user_data: *mut c_void) {
            threads().lock().unwrap().push(std::thread::current().id());
        }

        unsafe {
            let (db, dir) = open_test_db("observe_thread");
            let coll = CString::new("users").unwrap();
            let registering_thread = std::thread::current().id();

            let mut sub_id = 0u64;
            assert_eq!(
                rft_observe(db, coll.as_ptr(), callback, ptr::null_mut(), &mut sub_id),
                RftError::Ok
            );

            let json = r#"{"id":1,"fields":{}}"#;
            rft_collection_put(db, coll.as_ptr(), json.as_ptr(), json.len());
            std::thread::sleep(std::time::Duration::from_millis(150));

            {
                let captured = threads().lock().unwrap();
                assert!(!captured.is_empty(), "expected at least one event");
                for tid in captured.iter() {
                    assert_ne!(
                        *tid, registering_thread,
                        "observer callback must fire on a runtime worker \
                         thread, not the registering thread"
                    );
                }
            }

            assert_eq!(rft_unobserve(db, sub_id), RftError::Ok);
            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// Thread-affinity contract (F7e): the live-query initial snapshot
    /// fires synchronously on the registering thread; subsequent diffs
    /// fire on a runtime worker thread.
    #[test]
    fn observe_query_snapshot_sync_then_diffs_off_thread() {
        use std::os::raw::c_void;
        use std::sync::Mutex;
        use std::thread::ThreadId;

        static THREADS: std::sync::OnceLock<Mutex<Vec<ThreadId>>> = std::sync::OnceLock::new();

        fn threads() -> &'static Mutex<Vec<ThreadId>> {
            THREADS.get_or_init(|| Mutex::new(Vec::new()))
        }

        unsafe extern "C" fn callback(_json: *const c_char, _user_data: *mut c_void) {
            threads().lock().unwrap().push(std::thread::current().id());
        }

        unsafe {
            let (db, dir) = open_test_db("observe_query_thread");
            let coll = CString::new("users").unwrap();
            let registering_thread = std::thread::current().id();

            let doc = r#"{"id":1,"fields":{"age":{"Int":30}}}"#;
            rft_collection_put(db, coll.as_ptr(), doc.as_ptr(), doc.len());

            let q = r#"{"collection":"users"}"#;
            let mut sub_id = 0u64;
            assert_eq!(
                rft_observe_query(
                    db,
                    q.as_ptr(),
                    q.len(),
                    callback,
                    ptr::null_mut(),
                    &mut sub_id,
                ),
                RftError::Ok
            );

            // The initial snapshot is delivered synchronously on the
            // registering thread, before rft_observe_query returns.
            {
                let captured = threads().lock().unwrap();
                assert_eq!(captured.len(), 1, "expected exactly the snapshot");
                assert_eq!(
                    captured[0], registering_thread,
                    "initial snapshot must fire on the registering thread"
                );
            }

            // A mutation-triggered diff arrives on a worker thread.
            let doc2 = r#"{"id":2,"fields":{"age":{"Int":40}}}"#;
            rft_collection_put(db, coll.as_ptr(), doc2.as_ptr(), doc2.len());
            std::thread::sleep(std::time::Duration::from_millis(300));

            {
                let captured = threads().lock().unwrap();
                assert!(captured.len() >= 2, "expected a diff after the mutation");
                for tid in captured.iter().skip(1) {
                    assert_ne!(
                        *tid, registering_thread,
                        "live-query diffs must fire on a runtime worker thread"
                    );
                }
            }

            assert_eq!(rft_unobserve(db, sub_id), RftError::Ok);
            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// Single test covering the whole Dart-port surface so the global
    /// `POST_COBJECT` state is never raced by parallel test threads.
    #[test]
    fn dart_port_observers_post_copied_events() {
        use dart_port::test_support::{fake_post_cobject_addr, messages_for, reset_post_cobject};

        unsafe {
            let (db, dir) = open_test_db("dart_port");
            let coll = CString::new("users").unwrap();
            let mut sub_id = 0u64;

            // Uninitialized API → dedicated error, no subscription made.
            reset_post_cobject();
            assert_eq!(
                rft_observe_dart_port(db, coll.as_ptr(), 71, &mut sub_id),
                RftError::DartApiNotInitialized
            );

            // Null function pointer is rejected.
            assert_eq!(rft_dart_init(ptr::null_mut()), RftError::NullPointer);

            // Register the fake VM post function (copies during the call,
            // same lifetime contract as Dart_PostCObject_DL).
            assert_eq!(rft_dart_init(fake_post_cobject_addr()), RftError::Ok);

            // Collection observer → mutation events posted to port 71.
            assert_eq!(
                rft_observe_dart_port(db, coll.as_ptr(), 71, &mut sub_id),
                RftError::Ok
            );
            assert!(sub_id > 0);

            let doc = r#"{"id":1,"fields":{"age":{"Int":30}}}"#;
            rft_collection_put(db, coll.as_ptr(), doc.as_ptr(), doc.len());
            std::thread::sleep(std::time::Duration::from_millis(150));

            let events = messages_for(71);
            assert!(!events.is_empty(), "expected a posted mutation event");
            assert!(
                events[0].contains("\"collection\":\"users\"") && events[0].contains("Insert"),
                "unexpected event payload: {}",
                events[0]
            );
            assert_eq!(rft_unobserve(db, sub_id), RftError::Ok);

            // Live-query observer → initial snapshot posted synchronously
            // to port 72, then diffs on mutation.
            let query = br#"{"collection":"users"}"#;
            let mut q_sub = 0u64;
            assert_eq!(
                rft_observe_query_dart_port(db, query.as_ptr(), query.len(), 72, &mut q_sub),
                RftError::Ok
            );
            let snapshot = messages_for(72);
            assert_eq!(snapshot.len(), 1, "snapshot must be posted synchronously");
            assert!(
                snapshot[0].contains("\"id\":1"),
                "snapshot: {}",
                snapshot[0]
            );

            let doc2 = r#"{"id":2,"fields":{"age":{"Int":40}}}"#;
            rft_collection_put(db, coll.as_ptr(), doc2.as_ptr(), doc2.len());
            std::thread::sleep(std::time::Duration::from_millis(300));

            let all = messages_for(72);
            assert!(all.len() >= 2, "expected a diff after mutation: {all:?}");
            assert!(
                all.last().unwrap().contains("\"id\":2"),
                "diff should contain doc 2: {}",
                all.last().unwrap()
            );

            assert_eq!(rft_unobserve(db, q_sub), RftError::Ok);
            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }
}
