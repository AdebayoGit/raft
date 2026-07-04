//! Batch and scan FFI — the hot-path complement to the per-document
//! `rft_collection_*` calls.
//!
//! These functions exist because per-document FFI crossings and JSON
//! serialization dominate bulk workloads (profiled at ~67% of a 10k-doc
//! bulk write). All batch payloads use the compact binary codec in
//! [`super::codec`] — see that module for the wire format.
//!
//! Durability: `rft_collection_put_many` and `rft_collection_delete_many`
//! apply atomically through one transaction — a single WAL write and a
//! single fsync — the same contract as a caller-managed
//! `rft_transaction_begin` … `commit` loop, in one crossing.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;
use std::slice;

use crate::index::DocId;

use super::error::RftError;
use super::handle::RaftDb;
use super::write_buffer;
use crate::codec;

/// Opaque engine-owned byte buffer. Returned by [`rft_collection_scan`];
/// read it via [`rft_buf_data`] / [`rft_buf_len`] and release it with
/// [`rft_buf_free`]. One scan = one engine pass = four FFI crossings
/// total, regardless of document count.
pub struct RftBuf {
    data: Vec<u8>,
}

impl RftBuf {
    pub(super) fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
}

/// Pointer to the buffer's bytes. Valid until [`rft_buf_free`].
///
/// # Safety
///
/// `buf` must be a live handle from [`rft_collection_scan`] (or null,
/// which yields null).
#[no_mangle]
pub unsafe extern "C" fn rft_buf_data(buf: *const RftBuf) -> *const u8 {
    if buf.is_null() {
        return ptr::null();
    }
    unsafe { (*buf).data.as_ptr() }
}

/// Length in bytes of the buffer. Null yields 0.
///
/// # Safety
///
/// `buf` must be a live handle from [`rft_collection_scan`] or null.
#[no_mangle]
pub unsafe extern "C" fn rft_buf_len(buf: *const RftBuf) -> usize {
    if buf.is_null() {
        return 0;
    }
    unsafe { (*buf).data.len() }
}

/// Free a buffer handle. Null is a no-op. After this call the handle and
/// any pointer previously returned by [`rft_buf_data`] are dangling.
///
/// # Safety
///
/// `buf` must be a handle from [`rft_collection_scan`] not yet freed, or
/// null.
#[no_mangle]
pub unsafe extern "C" fn rft_buf_free(buf: *mut RftBuf) {
    if !buf.is_null() {
        drop(unsafe { Box::from_raw(buf) });
    }
}

/// Insert or update every document in `batch` (binary batch encoding)
/// atomically: one transaction, one WAL write, one fsync. Document ids
/// are honoured; repeated ids within the batch apply in order (last one
/// wins).
///
/// The whole batch is validated before anything is written — a malformed
/// document rejects the entire call with no partial effects.
///
/// # Safety
///
/// - `db` must be a valid handle from [`rft_open`](super::rft_open).
/// - `collection` must be a valid null-terminated UTF-8 C string.
/// - `batch` must point to `batch_len` readable bytes (null only if
///   `batch_len == 0`).
#[no_mangle]
pub unsafe extern "C" fn rft_collection_put_many(
    db: *mut RaftDb,
    collection: *const c_char,
    batch: *const u8,
    batch_len: usize,
) -> RftError {
    super::guard(|| {
        let handle = match unsafe { super::live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        if collection.is_null() || (batch.is_null() && batch_len > 0) {
            return RftError::NullPointer;
        }
        let coll = match unsafe { CStr::from_ptr(collection) }.to_str() {
            Ok(s) => s,
            Err(_) => return RftError::InvalidUtf8,
        };
        let bytes = if batch_len == 0 {
            &[][..]
        } else {
            unsafe { slice::from_raw_parts(batch, batch_len) }
        };
        // Spans let the engine persist the caller's bytes verbatim — decode
        // once for the in-memory state, never re-encode for disk.
        let spans = match codec::decode_batch_spans(bytes) {
            Ok(d) => d,
            Err(_) => return RftError::InvalidJson,
        };
        let entries: Vec<_> = spans
            .into_iter()
            .map(|(doc, range)| (doc, &bytes[range]))
            .collect();
        match handle.database().put_batch_encoded(coll, entries) {
            Ok(()) => RftError::Ok,
            Err(_) => RftError::IoError,
        }
    })
}

/// Delete every id in `ids` atomically: one transaction, one WAL write,
/// one fsync. Missing ids are not an error (tombstones are written).
///
/// # Safety
///
/// - `db` must be a valid handle; `collection` a valid null-terminated
///   UTF-8 C string.
/// - `ids` must point to `count` readable `u64`s (null only if
///   `count == 0`).
#[no_mangle]
pub unsafe extern "C" fn rft_collection_delete_many(
    db: *mut RaftDb,
    collection: *const c_char,
    ids: *const u64,
    count: usize,
) -> RftError {
    super::guard(|| {
        let handle = match unsafe { super::live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        if collection.is_null() || (ids.is_null() && count > 0) {
            return RftError::NullPointer;
        }
        let coll = match unsafe { CStr::from_ptr(collection) }.to_str() {
            Ok(s) => s,
            Err(_) => return RftError::InvalidUtf8,
        };
        if count == 0 {
            return RftError::Ok;
        }
        let id_slice = unsafe { slice::from_raw_parts(ids, count) };
        let doc_ids: Vec<DocId> = id_slice.iter().map(|&id| DocId(id)).collect();
        match handle.database().delete_batch(coll, &doc_ids) {
            Ok(()) => RftError::Ok,
            Err(_) => RftError::IoError,
        }
    })
}

/// Read every document in `collection` in one call. On success writes a
/// new buffer handle (binary batch encoding, ids ascending) to
/// `*out_buf`; the caller owns it and must release it with
/// [`rft_buf_free`].
///
/// # Safety
///
/// - `db` must be a valid handle; `collection` a valid null-terminated
///   UTF-8 C string; `out_buf` a writable `*mut *mut RftBuf`.
#[no_mangle]
pub unsafe extern "C" fn rft_collection_scan(
    db: *mut RaftDb,
    collection: *const c_char,
    out_buf: *mut *mut RftBuf,
) -> RftError {
    super::guard(|| {
        let handle = match unsafe { super::live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        if collection.is_null() || out_buf.is_null() {
            return RftError::NullPointer;
        }
        let coll = match unsafe { CStr::from_ptr(collection) }.to_str() {
            Ok(s) => s,
            Err(_) => return RftError::InvalidUtf8,
        };

        // Stream-encode directly out of the collection's in-memory state:
        // one pass, no per-document clones, no intermediate Vec<Document>.
        let mut enc = codec::BatchEncoder::new();
        handle.database().for_each_doc(coll, |doc| enc.push(doc));
        let raw = Box::into_raw(Box::new(RftBuf { data: enc.finish() }));
        unsafe { ptr::write(out_buf, raw) };
        RftError::Ok
    })
}

/// Fetch one document by id as binary codec bytes — the single-lookup
/// replacement for the two-phase JSON `rft_collection_get`. `*out_len`
/// carries the buffer capacity in and the written (or required) size
/// out; on `BufferTooSmall` nothing is copied and the caller retries
/// with a larger buffer (rare when reusing a sensibly-sized one).
///
/// # Safety
///
/// - `db` must be a valid handle; `collection` a valid null-terminated
///   UTF-8 C string.
/// - `out_len` must be a valid `*mut usize`; `out_buf` must point to
///   `*out_len` writable bytes (or be null to only query the size).
#[no_mangle]
pub unsafe extern "C" fn rft_collection_get_buf(
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

        let encoded = handle.database().with_doc(coll, DocId(doc_id), |doc| {
            let mut bytes = Vec::new();
            codec::encode_doc(doc, &mut bytes);
            bytes
        });
        let Some(bytes) = encoded else {
            return RftError::NotFound;
        };
        unsafe { write_buffer(&bytes, out_buf, out_len) }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::super::RftError;
    use super::*;
    use crate::codec::{decode_batch, decode_doc, encode_batch};
    use crate::query::{Document, Value};

    fn doc(id: u64, score: i64) -> Document {
        let mut fields = HashMap::new();
        fields.insert("name".into(), Value::String(format!("user-{id}")));
        fields.insert("score".into(), Value::Int(score));
        Document {
            id: DocId(id),
            fields,
        }
    }

    unsafe fn open_test_db(name: &str) -> (*mut RaftDb, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("rft_bulk_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let c_path = std::ffi::CString::new(dir.to_str().unwrap()).unwrap();
        let mut err = RftError::Ok;
        let db = unsafe { super::super::rft_open(c_path.as_ptr(), &mut err) };
        assert!(!db.is_null(), "open failed: {err:?}");
        (db, dir)
    }

    unsafe fn close_and_clean(db: *mut RaftDb, dir: std::path::PathBuf) {
        unsafe { super::super::rft_close(db) };
        let _ = std::fs::remove_dir_all(&dir);
    }

    const COLL: &[u8] = b"bench\0";

    #[test]
    fn put_many_then_scan_roundtrip() {
        unsafe {
            let (db, dir) = open_test_db("roundtrip");
            let docs: Vec<_> = (1..=50).map(|i| doc(i, i as i64)).collect();
            let batch = encode_batch(&docs);

            let rc = rft_collection_put_many(db, COLL.as_ptr().cast(), batch.as_ptr(), batch.len());
            assert_eq!(rc, RftError::Ok);

            let mut buf: *mut RftBuf = ptr::null_mut();
            let rc = rft_collection_scan(db, COLL.as_ptr().cast(), &mut buf);
            assert_eq!(rc, RftError::Ok);
            assert!(!buf.is_null());
            let bytes = slice::from_raw_parts(rft_buf_data(buf), rft_buf_len(buf));
            let scanned = decode_batch(bytes).unwrap();
            assert_eq!(scanned.len(), 50);
            assert_eq!(scanned[0].id, DocId(1));
            assert_eq!(scanned[49].id, DocId(50));
            rft_buf_free(buf);

            close_and_clean(db, dir);
        }
    }

    #[test]
    fn put_many_persists_across_reopen() {
        unsafe {
            let dir = std::env::temp_dir().join(format!("rft_bulk_reopen_{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            let c_path = std::ffi::CString::new(dir.to_str().unwrap()).unwrap();

            let mut err = RftError::Ok;
            let db = super::super::rft_open(c_path.as_ptr(), &mut err);
            assert!(!db.is_null());
            let docs: Vec<_> = (1..=30).map(|i| doc(i, i as i64 * 10)).collect();
            let batch = encode_batch(&docs);
            assert_eq!(
                rft_collection_put_many(db, COLL.as_ptr().cast(), batch.as_ptr(), batch.len()),
                RftError::Ok
            );
            super::super::rft_close(db);

            // Reopen: the verbatim-persisted binary docs must rehydrate.
            let db2 = super::super::rft_open(c_path.as_ptr(), &mut err);
            assert!(!db2.is_null());
            let mut count = 0usize;
            super::super::rft_collection_count(db2, COLL.as_ptr().cast(), &mut count);
            assert_eq!(count, 30);
            let mut buf = vec![0u8; 4096];
            let mut len = buf.len();
            assert_eq!(
                rft_collection_get_buf(db2, COLL.as_ptr().cast(), 17, buf.as_mut_ptr(), &mut len),
                RftError::Ok
            );
            let d = decode_doc(&buf[..len]).unwrap();
            assert_eq!(d.fields.get("score"), Some(&Value::Int(170)));
            super::super::rft_close(db2);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    #[test]
    fn put_many_is_atomic_on_malformed_batch() {
        unsafe {
            let (db, dir) = open_test_db("atomic");
            // Valid batch followed by garbage -> whole call rejected.
            let docs: Vec<_> = (1..=5).map(|i| doc(i, 0)).collect();
            let mut batch = encode_batch(&docs);
            batch.push(0xAB); // trailing garbage

            let rc = rft_collection_put_many(db, COLL.as_ptr().cast(), batch.as_ptr(), batch.len());
            assert_eq!(rc, RftError::InvalidJson);

            // Nothing was written.
            let mut count = 0usize;
            let rc = super::super::rft_collection_count(db, COLL.as_ptr().cast(), &mut count);
            assert_eq!(rc, RftError::Ok);
            assert_eq!(count, 0);

            close_and_clean(db, dir);
        }
    }

    #[test]
    fn delete_many_removes_all() {
        unsafe {
            let (db, dir) = open_test_db("delmany");
            let docs: Vec<_> = (1..=20).map(|i| doc(i, 0)).collect();
            let batch = encode_batch(&docs);
            assert_eq!(
                rft_collection_put_many(db, COLL.as_ptr().cast(), batch.as_ptr(), batch.len()),
                RftError::Ok
            );

            let ids: Vec<u64> = (1..=20).collect();
            assert_eq!(
                rft_collection_delete_many(db, COLL.as_ptr().cast(), ids.as_ptr(), ids.len()),
                RftError::Ok
            );

            let mut count = usize::MAX;
            super::super::rft_collection_count(db, COLL.as_ptr().cast(), &mut count);
            assert_eq!(count, 0);

            close_and_clean(db, dir);
        }
    }

    #[test]
    fn get_buf_single_phase_and_not_found() {
        unsafe {
            let (db, dir) = open_test_db("getbuf");
            let batch = encode_batch(&[doc(7, 99)]);
            rft_collection_put_many(db, COLL.as_ptr().cast(), batch.as_ptr(), batch.len());

            // Reused caller buffer, single call.
            let mut buf = vec![0u8; 4096];
            let mut len = buf.len();
            let rc =
                rft_collection_get_buf(db, COLL.as_ptr().cast(), 7, buf.as_mut_ptr(), &mut len);
            assert_eq!(rc, RftError::Ok);
            let d = decode_doc(&buf[..len]).unwrap();
            assert_eq!(d.id, DocId(7));
            assert_eq!(d.fields.get("score"), Some(&Value::Int(99)));

            let mut len2 = buf.len();
            let rc =
                rft_collection_get_buf(db, COLL.as_ptr().cast(), 8, buf.as_mut_ptr(), &mut len2);
            assert_eq!(rc, RftError::NotFound);

            close_and_clean(db, dir);
        }
    }

    #[test]
    fn get_buf_reports_required_size_when_too_small() {
        unsafe {
            let (db, dir) = open_test_db("getbuf_small");
            let batch = encode_batch(&[doc(1, 1)]);
            rft_collection_put_many(db, COLL.as_ptr().cast(), batch.as_ptr(), batch.len());

            let mut tiny = [0u8; 2];
            let mut len = tiny.len();
            let rc =
                rft_collection_get_buf(db, COLL.as_ptr().cast(), 1, tiny.as_mut_ptr(), &mut len);
            assert_eq!(rc, RftError::BufferTooSmall);
            assert!(len > 2, "required size must be reported");

            close_and_clean(db, dir);
        }
    }

    #[test]
    fn empty_batch_and_empty_delete_are_ok() {
        unsafe {
            let (db, dir) = open_test_db("empty");
            let batch = encode_batch(&[]);
            assert_eq!(
                rft_collection_put_many(db, COLL.as_ptr().cast(), batch.as_ptr(), batch.len()),
                RftError::Ok
            );
            assert_eq!(
                rft_collection_delete_many(db, COLL.as_ptr().cast(), ptr::null(), 0),
                RftError::Ok
            );
            close_and_clean(db, dir);
        }
    }

    #[test]
    fn scan_on_empty_collection_yields_empty_batch() {
        unsafe {
            let (db, dir) = open_test_db("scan_empty");
            let mut buf: *mut RftBuf = ptr::null_mut();
            assert_eq!(
                rft_collection_scan(db, COLL.as_ptr().cast(), &mut buf),
                RftError::Ok
            );
            let bytes = slice::from_raw_parts(rft_buf_data(buf), rft_buf_len(buf));
            assert_eq!(decode_batch(bytes).unwrap().len(), 0);
            rft_buf_free(buf);
            close_and_clean(db, dir);
        }
    }

    #[test]
    fn null_arguments_are_rejected_not_ub() {
        unsafe {
            let (db, dir) = open_test_db("nulls");
            assert_eq!(
                rft_collection_put_many(db, ptr::null(), ptr::null(), 0),
                RftError::NullPointer
            );
            assert_eq!(
                rft_collection_put_many(db, COLL.as_ptr().cast(), ptr::null(), 9),
                RftError::NullPointer
            );
            assert_eq!(
                rft_collection_scan(db, COLL.as_ptr().cast(), ptr::null_mut()),
                RftError::NullPointer
            );
            let mut len = 0usize;
            assert_eq!(
                rft_collection_get_buf(db, ptr::null(), 1, ptr::null_mut(), &mut len),
                RftError::NullPointer
            );
            assert_eq!(rft_buf_len(ptr::null()), 0);
            assert!(rft_buf_data(ptr::null()).is_null());
            rft_buf_free(ptr::null_mut()); // no-op, no crash
            close_and_clean(db, dir);
        }
    }
}
