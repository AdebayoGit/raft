//! Transaction FFI — `rft_transaction_*` functions.
//!
//! A transaction is an opaque, owned handle. The caller must end its
//! lifetime by calling either [`rft_transaction_commit`] or
//! [`rft_transaction_rollback`] — both consume the handle. Forgetting to
//! call either leaks the buffer (no rollback is implicit).
//!
//! Reads are tracked for optimistic concurrency control: a commit fails
//! with [`RftError::TransactionConflict`] if any document read during
//! the transaction has been modified by another writer in the meantime.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;
use std::slice;

use crate::database::DatabaseError;
use crate::index::DocId;
use crate::query::Document;
use crate::transaction::TransactionError;

use super::error::RftError;
use super::handle::RaftDb;
use super::write_buffer;

/// Opaque transaction handle.
pub struct RaftTransaction {
    inner: Option<crate::database::DbTransaction>,
}

/// Begin a new transaction. The caller takes ownership of the returned
/// handle and must end it with `commit` or `rollback`.
///
/// # Safety
///
/// - `db` must be a valid handle.
/// - `out_txn` must be a valid `*mut *mut RaftTransaction`.
#[no_mangle]
pub unsafe extern "C" fn rft_transaction_begin(
    db: *mut RaftDb,
    out_txn: *mut *mut RaftTransaction,
) -> RftError {
    super::guard(|| {
        let handle = match unsafe { super::live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        if out_txn.is_null() {
            return RftError::NullPointer;
        }

        let txn = handle.database().begin_transaction();
        let raw = Box::into_raw(Box::new(RaftTransaction { inner: Some(txn) }));
        super::registry::LIVE_TXNS.register(raw);
        unsafe { ptr::write(out_txn, raw) };
        RftError::Ok
    })
}

/// Read a document inside the transaction. The version is recorded for
/// conflict detection at commit time. JSON encoding is written to
/// `out_buf` using the same buffer-too-small protocol as
/// `rft_collection_get`.
///
/// Returns [`RftError::NotFound`] when the doc does not exist (the read
/// is still tracked, so insertion of the doc by another writer before
/// commit will be detected as a conflict).
///
/// # Safety
///
/// - `txn` must be a valid handle returned by
///   [`rft_transaction_begin`] and not yet committed/rolled back.
/// - Other arguments follow the standard FFI contract.
#[no_mangle]
pub unsafe extern "C" fn rft_transaction_get(
    txn: *mut RaftTransaction,
    collection: *const c_char,
    doc_id: u64,
    out_buf: *mut u8,
    out_len: *mut usize,
) -> RftError {
    super::guard(|| {
        let handle = match unsafe { super::live_txn(txn) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        let Some(t) = handle.inner.as_mut() else {
            return RftError::InvalidHandle;
        };
        if collection.is_null() || out_len.is_null() {
            return RftError::NullPointer;
        }

        let coll = match unsafe { CStr::from_ptr(collection) }.to_str() {
            Ok(s) => s,
            Err(_) => return RftError::InvalidUtf8,
        };

        match t.get(coll, DocId(doc_id)) {
            Ok(Some(doc)) => match serde_json::to_vec(&doc) {
                Ok(bytes) => unsafe { write_buffer(&bytes, out_buf, out_len) },
                Err(_) => RftError::InvalidJson,
            },
            Ok(None) => RftError::NotFound,
            Err(TransactionError::AlreadyFinalised) => RftError::InvalidHandle,
            Err(_) => RftError::IoError,
        }
    })
}

/// Buffer a write inside the transaction. Applied atomically on commit.
///
/// # Safety
///
/// - `txn` must be a valid handle returned by
///   [`rft_transaction_begin`] and not yet finalised.
/// - `doc_json` must be a valid UTF-8 buffer of `doc_json_len` bytes.
#[no_mangle]
pub unsafe extern "C" fn rft_transaction_put(
    txn: *mut RaftTransaction,
    collection: *const c_char,
    doc_json: *const u8,
    doc_json_len: usize,
) -> RftError {
    super::guard(|| {
        let handle = match unsafe { super::live_txn(txn) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        let Some(t) = handle.inner.as_mut() else {
            return RftError::InvalidHandle;
        };
        if collection.is_null() || (doc_json.is_null() && doc_json_len > 0) {
            return RftError::NullPointer;
        }

        let coll = match unsafe { CStr::from_ptr(collection) }.to_str() {
            Ok(s) => s,
            Err(_) => return RftError::InvalidUtf8,
        };
        let json = unsafe { slice::from_raw_parts(doc_json, doc_json_len) };
        let doc: Document = match super::document_from_json(json) {
            Ok(d) => d,
            Err(e) => return e,
        };

        match t.put(coll, doc) {
            Ok(()) => RftError::Ok,
            Err(_) => RftError::InvalidHandle,
        }
    })
}

/// Buffer a delete inside the transaction.
///
/// # Safety
///
/// - `txn` must be a valid, active handle.
/// - `collection` must be a valid null-terminated UTF-8 C string.
#[no_mangle]
pub unsafe extern "C" fn rft_transaction_delete(
    txn: *mut RaftTransaction,
    collection: *const c_char,
    doc_id: u64,
) -> RftError {
    super::guard(|| {
        let handle = match unsafe { super::live_txn(txn) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        let Some(t) = handle.inner.as_mut() else {
            return RftError::InvalidHandle;
        };
        if collection.is_null() {
            return RftError::NullPointer;
        }

        let coll = match unsafe { CStr::from_ptr(collection) }.to_str() {
            Ok(s) => s,
            Err(_) => return RftError::InvalidUtf8,
        };

        match t.delete(coll, DocId(doc_id)) {
            Ok(()) => RftError::Ok,
            Err(_) => RftError::InvalidHandle,
        }
    })
}

/// Validate the read set and atomically apply all buffered changes.
/// Consumes the handle — it is freed regardless of outcome and must not
/// be used again.
///
/// Returns:
/// - [`RftError::Ok`] on success.
/// - [`RftError::TransactionConflict`] if a tracked doc was modified
///   concurrently. No writes are applied.
///
/// # Safety
///
/// - `txn` must be a valid, active handle. After this call, `txn` is
///   freed and dangling.
#[no_mangle]
pub unsafe extern "C" fn rft_transaction_commit(txn: *mut RaftTransaction) -> RftError {
    super::guard(|| {
        if txn.is_null() {
            return RftError::NullPointer;
        }
        // Unregister-wins: exactly one of a concurrent commit/rollback
        // pair frees the handle; the loser gets InvalidHandle instead of
        // a double-free.
        if !super::registry::LIVE_TXNS.unregister(txn) {
            return RftError::InvalidHandle;
        }
        let mut boxed = unsafe { Box::from_raw(txn) };
        let Some(inner) = boxed.inner.take() else {
            return RftError::InvalidHandle;
        };
        match inner.commit() {
            Ok(()) => RftError::Ok,
            Err(DatabaseError::Transaction(TransactionError::Conflict { .. })) => {
                RftError::TransactionConflict
            }
            Err(_) => RftError::IoError,
        }
    })
}

/// Discard the transaction without applying any buffered changes.
/// Consumes the handle.
///
/// # Safety
///
/// - `txn` must be a valid handle returned by
///   [`rft_transaction_begin`], or null (no-op).
/// - After this call, `txn` is freed and dangling.
#[no_mangle]
pub unsafe extern "C" fn rft_transaction_rollback(txn: *mut RaftTransaction) {
    super::guard_or((), || {
        // Unregister-wins: rolling back an already-finalised or foreign
        // handle is a safe no-op.
        if txn.is_null() || !super::registry::LIVE_TXNS.unregister(txn) {
            return;
        }
        let mut boxed = unsafe { Box::from_raw(txn) };
        if let Some(inner) = boxed.inner.take() {
            inner.rollback();
        }
    });
}
