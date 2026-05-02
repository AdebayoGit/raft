//! C ABI layer — stable FFI interface for all platform bindings.
//!
//! The database is exposed as an opaque handle (`*mut RaftDb`) with
//! `rft_`-prefixed free functions. All errors are returned as a
//! [`RftError`] C enum.
//!
//! ## Surface
//!
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

mod collection;
mod error;
mod handle;
mod observe;
mod query;
mod transaction;

pub use collection::{
    rft_collection_count, rft_collection_delete, rft_collection_get,
    rft_collection_list_ids, rft_collection_put, rft_collection_put_auto,
};
pub use error::RftError;
pub use handle::RaftDb;
pub use observe::{rft_observe, rft_observe_query, rft_unobserve, RftObserveCallback};
pub use query::{
    rft_query_execute, rft_query_result_count, rft_query_result_free,
    rft_query_result_get, RaftQueryResult,
};
pub use transaction::{
    rft_transaction_begin, rft_transaction_commit, rft_transaction_delete,
    rft_transaction_get, rft_transaction_put, rft_transaction_rollback,
    RaftTransaction,
};

use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;
use std::slice;

use crate::database::Database;

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

    match Database::open(c_str) {
        Ok(db) => {
            if !out_err.is_null() {
                unsafe { ptr::write(out_err, RftError::Ok) };
            }
            Box::into_raw(Box::new(RaftDb::new(db)))
        }
        Err(_) => {
            if !out_err.is_null() {
                unsafe { ptr::write(out_err, RftError::IoError) };
            }
            ptr::null_mut()
        }
    }
}

/// Close and free a database handle. Aborts any pending observer tasks
/// before dropping the runtime.
///
/// # Safety
///
/// - `db` must be a handle returned by [`rft_open`], or null (no-op).
/// - After this call, `db` is dangling and must not be used.
#[no_mangle]
pub unsafe extern "C" fn rft_close(db: *mut RaftDb) {
    if !db.is_null() {
        let handle = unsafe { &*db };
        observe::abort_all_subscriptions(handle);
        drop(unsafe { Box::from_raw(db) });
    }
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
    let Some(handle) = (unsafe { db.as_ref() }) else {
        return RftError::NullPointer;
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
    let Some(handle) = (unsafe { db.as_ref() }) else {
        return RftError::NullPointer;
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
pub unsafe extern "C" fn rft_delete(
    db: *mut RaftDb,
    key: *const u8,
    key_len: usize,
) -> RftError {
    let Some(handle) = (unsafe { db.as_ref() }) else {
        return RftError::NullPointer;
    };
    if key.is_null() && key_len > 0 {
        return RftError::NullPointer;
    }

    let key_slice = unsafe { slice::from_raw_parts(key, key_len) };

    match handle.database().raw_delete(key_slice.to_vec()) {
        Ok(()) => RftError::Ok,
        Err(_) => RftError::IoError,
    }
}

// ── Internal helpers ───────────────────────────────────────────────────

/// Standard "write `bytes` into caller buffer, fall back to size query"
/// pattern shared by all FFI functions that return variable-length data.
///
/// # Safety
///
/// - `out_len` must be a valid `*mut usize`.
/// - `out_buf` must be writable for at least the value of `*out_len`
///   bytes, or null.
pub(crate) unsafe fn write_buffer(
    bytes: &[u8],
    out_buf: *mut u8,
    out_len: *mut usize,
) -> RftError {
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
                rft_collection_put(
                    db,
                    coll.as_ptr(),
                    doc_json.as_ptr(),
                    doc_json.len(),
                ),
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
            assert!(std::str::from_utf8(&buf[..len]).unwrap().contains("\"Int\""));

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
                rft_observe(
                    db,
                    coll.as_ptr(),
                    callback,
                    ptr::null_mut(),
                    &mut sub_id,
                ),
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
                let doc = format!(r#"{{"id":{i},"fields":{{"age":{{"Int":{}}}}}}}"#, 20 + i as i64);
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
                assert!(last.contains("\"id\":3"), "diff should include doc 3: {last}");
            }

            assert_eq!(rft_unobserve(db, sub_id), RftError::Ok);
            rft_close(db);
            std::fs::remove_dir_all(&dir).ok();
        }
    }
}
