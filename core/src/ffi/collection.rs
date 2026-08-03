//! Collection CRUD FFI — `rft_collection_*` functions.
//!
//! Documents flow over the FFI boundary as JSON-encoded UTF-8 byte
//! strings. The wire format mirrors [`crate::query::Document`]:
//!
//! ```json
//! { "id": 1, "fields": { "name": { "String": "Alice" }, "age": { "Int": 30 } } }
//! ```
//!
//! All collection ops use the caller-provided buffer pattern from the KV
//! layer: pass `out_buf` + `*out_len`; on `BufferTooSmall`, `*out_len`
//! holds the required size.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;

use crate::index::DocId;
use crate::query::Document;

use super::error::RftError;
use super::handle::RaftDb;
use super::write_buffer;

/// Insert or update a document in `collection`. The document's `id` field
/// is honoured. The same id always maps to the same slot — repeated puts
/// overwrite (and bump the internal version).
///
/// # Safety
///
/// - `db` must be a valid handle returned by [`rft_open`](super::rft_open).
/// - `collection` must be a valid null-terminated UTF-8 C string.
/// - `doc_json` must be a valid UTF-8 buffer of `doc_json_len` bytes.
#[no_mangle]
pub unsafe extern "C" fn rft_collection_put(
    db: *mut RaftDb,
    collection: *const c_char,
    doc_json: *const u8,
    doc_json_len: usize,
) -> RftError {
    super::guard(|| {
        let handle = match unsafe { super::live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        if collection.is_null() {
            return RftError::NullPointer;
        }

        let coll = match unsafe { CStr::from_ptr(collection) }.to_str() {
            Ok(s) => s,
            Err(_) => return RftError::InvalidUtf8,
        };
        let json = match unsafe { super::input_slice(doc_json, doc_json_len) } {
            Ok(value) => value,
            Err(e) => return e,
        };
        let doc: Document = match super::document_from_json(json) {
            Ok(d) => d,
            Err(e) => return e,
        };

        match handle.database().put(coll, doc) {
            Ok(_) => RftError::Ok,
            Err(_) => RftError::IoError,
        }
    })
}

/// Insert a document, letting the database assign a fresh id. Writes the
/// assigned id to `*out_doc_id` on success.
///
/// # Safety
///
/// - All non-null pointers must point to valid memory of the appropriate
///   size; `out_doc_id` must be a writable `*mut u64`.
#[no_mangle]
pub unsafe extern "C" fn rft_collection_put_auto(
    db: *mut RaftDb,
    collection: *const c_char,
    doc_json: *const u8,
    doc_json_len: usize,
    out_doc_id: *mut u64,
) -> RftError {
    super::guard(|| {
        let handle = match unsafe { super::live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        if collection.is_null() || out_doc_id.is_null() {
            return RftError::NullPointer;
        }

        let coll = match unsafe { CStr::from_ptr(collection) }.to_str() {
            Ok(s) => s,
            Err(_) => return RftError::InvalidUtf8,
        };
        let json = match unsafe { super::input_slice(doc_json, doc_json_len) } {
            Ok(value) => value,
            Err(e) => return e,
        };
        let doc: Document = match super::document_from_json(json) {
            Ok(d) => d,
            Err(e) => return e,
        };

        match handle.database().put_auto(coll, doc) {
            Ok(id) => {
                unsafe { ptr::write(out_doc_id, id.0) };
                RftError::Ok
            }
            Err(_) => RftError::IoError,
        }
    })
}

/// Fetch a document by id, writing its JSON encoding to `out_buf`. On
/// `BufferTooSmall` the required size is written to `*out_len` and no
/// bytes are copied.
///
/// # Safety
///
/// - `db` must be a valid handle.
/// - `collection` must be a valid null-terminated UTF-8 C string.
/// - `out_buf` must be writable for `*out_len` bytes, or null to query
///   the required size.
/// - `out_len` must be a valid `*mut usize`.
#[no_mangle]
pub unsafe extern "C" fn rft_collection_get(
    db: *mut RaftDb,
    collection: *const c_char,
    doc_id: u64,
    out_buf: *mut u8,
    out_len: *mut usize,
) -> RftError {
    super::guard(|| {
        let handle = match unsafe { super::live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        if collection.is_null() || out_len.is_null() {
            return RftError::NullPointer;
        }

        let coll = match unsafe { CStr::from_ptr(collection) }.to_str() {
            Ok(s) => s,
            Err(_) => return RftError::InvalidUtf8,
        };

        let Some(doc) = handle.database().get(coll, DocId(doc_id)) else {
            return RftError::NotFound;
        };

        let bytes = match serde_json::to_vec(&doc) {
            Ok(b) => b,
            Err(_) => return RftError::InvalidJson,
        };

        unsafe { write_buffer(&bytes, out_buf, out_len) }
    })
}

/// Delete a document. Returns [`RftError::Ok`] whether the document
/// existed or not.
///
/// # Safety
///
/// - `db` must be a valid handle.
/// - `collection` must be a valid null-terminated UTF-8 C string.
#[no_mangle]
pub unsafe extern "C" fn rft_collection_delete(
    db: *mut RaftDb,
    collection: *const c_char,
    doc_id: u64,
) -> RftError {
    super::guard(|| {
        let handle = match unsafe { super::live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        if collection.is_null() {
            return RftError::NullPointer;
        }

        let coll = match unsafe { CStr::from_ptr(collection) }.to_str() {
            Ok(s) => s,
            Err(_) => return RftError::InvalidUtf8,
        };

        match handle.database().delete(coll, DocId(doc_id)) {
            Ok(_) => RftError::Ok,
            Err(_) => RftError::IoError,
        }
    })
}

/// Number of documents in `collection`. Writes the count to `*out_count`.
///
/// # Safety
///
/// - `db` must be a valid handle.
/// - `collection` must be a valid null-terminated UTF-8 C string.
/// - `out_count` must be a valid `*mut usize`.
#[no_mangle]
pub unsafe extern "C" fn rft_collection_count(
    db: *mut RaftDb,
    collection: *const c_char,
    out_count: *mut usize,
) -> RftError {
    super::guard(|| {
        let handle = match unsafe { super::live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        if collection.is_null() || out_count.is_null() {
            return RftError::NullPointer;
        }

        let coll = match unsafe { CStr::from_ptr(collection) }.to_str() {
            Ok(s) => s,
            Err(_) => return RftError::InvalidUtf8,
        };

        unsafe { ptr::write(out_count, handle.database().count(coll)) };
        RftError::Ok
    })
}

/// List all document ids in `collection`, sorted ascending.
///
/// Writes up to `*out_len` ids into `out_ids` and sets `*out_len` to
/// the number written. If `out_ids` is null or `*out_len` is smaller
/// than the total count, returns [`RftError::BufferTooSmall`] and
/// `*out_len` holds the required size.
///
/// # Safety
///
/// - `db` must be a valid handle.
/// - `collection` must be a valid null-terminated UTF-8 C string.
/// - `out_ids` must be writable for `*out_len * 8` bytes, or null.
/// - `out_len` must be a valid `*mut usize`.
#[no_mangle]
pub unsafe extern "C" fn rft_collection_list_ids(
    db: *mut RaftDb,
    collection: *const c_char,
    out_ids: *mut u64,
    out_len: *mut usize,
) -> RftError {
    super::guard(|| {
        let handle = match unsafe { super::live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        if collection.is_null() || out_len.is_null() {
            return RftError::NullPointer;
        }

        let coll = match unsafe { CStr::from_ptr(collection) }.to_str() {
            Ok(s) => s,
            Err(_) => return RftError::InvalidUtf8,
        };

        let ids = handle.database().list_ids(coll);
        let required = ids.len();
        let capacity = unsafe { ptr::read(out_len) };

        if out_ids.is_null() || capacity < required {
            unsafe { ptr::write(out_len, required) };
            return RftError::BufferTooSmall;
        }

        for (i, id) in ids.iter().enumerate() {
            unsafe { ptr::write(out_ids.add(i), id.0) };
        }
        unsafe { ptr::write(out_len, required) };
        RftError::Ok
    })
}
