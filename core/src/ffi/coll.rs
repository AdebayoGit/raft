//! Prepared collection handles — the hot-read FFI surface.
//!
//! `rft_collection_get` and friends re-validate the collection name and
//! re-hash it into the collections map on every call. A prepared handle
//! does that work once at open:
//!
//! - [`rft_coll_get_buf`] — single-lookup point read, encoding into a
//!   thread-local scratch buffer (no per-read heap allocation).
//! - [`rft_coll_get_many`] — N reads in one crossing under one lock hold.
//! - [`rft_coll_generation`] — a stable pointer to the collection's
//!   mutation-generation counter. A binding-side cache validates entries
//!   with a plain memory load of this counter — **no FFI crossing** — and
//!   refetches through the calls above when it changes. This is what makes
//!   hot reads frame-loop-safe at high refresh rates.
//!
//! # Lifetime contract
//!
//! Close every collection handle (via [`rft_collection_close`]) before
//! closing its database — the same ordering rule as transactions. The
//! generation pointer is valid until the handle is closed.

use std::cell::RefCell;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;
#[cfg(test)]
use std::slice;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use crate::index::DocId;

use super::bulk::RftBuf;
use super::error::RftError;
use super::handle::RaftDb;
use super::registry;
use super::write_buffer;

/// Opaque prepared-collection handle.
pub struct RaftCollection {
    db: *mut RaftDb,
    name: String,
    generation: Arc<AtomicU64>,
}

thread_local! {
    /// Per-thread encode scratch: reads encode here and copy out, so the
    /// steady-state read path performs zero heap allocations.
    static SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

unsafe fn live_coll<'a>(coll: *mut RaftCollection) -> Result<&'a RaftCollection, RftError> {
    if coll.is_null() || !registry::LIVE_COLLS.is_live(coll) {
        return Err(RftError::InvalidHandle);
    }
    Ok(unsafe { &*coll })
}

/// Open a prepared handle for `collection` (creating the collection entry
/// if it does not exist yet). The handle caches the validated name and the
/// collection's generation counter.
///
/// # Safety
///
/// - `db` must be a valid handle from [`rft_open`](super::rft_open).
/// - `collection` must be a valid null-terminated UTF-8 C string.
/// - `out_coll` must be a writable `*mut *mut RaftCollection`.
/// - The returned handle must be closed with [`rft_collection_close`]
///   **before** `db` is closed.
#[no_mangle]
pub unsafe extern "C" fn rft_collection_open(
    db: *mut RaftDb,
    collection: *const c_char,
    out_coll: *mut *mut RaftCollection,
) -> RftError {
    super::guard(|| {
        let handle = match unsafe { super::live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        if collection.is_null() || out_coll.is_null() {
            return RftError::NullPointer;
        }
        let name = match unsafe { CStr::from_ptr(collection) }.to_str() {
            Ok(s) => s.to_owned(),
            Err(_) => return RftError::InvalidUtf8,
        };
        let generation = handle.database().collection_generation(&name);
        let raw = Box::into_raw(Box::new(RaftCollection {
            db,
            name,
            generation,
        }));
        registry::LIVE_COLLS.register(raw);
        unsafe { ptr::write(out_coll, raw) };
        RftError::Ok
    })
}

/// Close a prepared collection handle. Null or already-closed handles are
/// safe no-ops.
///
/// # Safety
///
/// - After this call the handle — and any generation pointer obtained from
///   it — is dangling and must not be used.
#[no_mangle]
pub unsafe extern "C" fn rft_collection_close(coll: *mut RaftCollection) {
    super::guard_or((), || {
        if !coll.is_null() && registry::LIVE_COLLS.unregister(coll) {
            drop(unsafe { Box::from_raw(coll) });
        }
    })
}

/// Stable pointer to the collection's mutation-generation counter. The
/// value increments on every document write or delete in the collection,
/// through any write path. Bindings read it with a plain (aligned 64-bit)
/// load; entries cached at generation G stay valid while it still reads G.
///
/// Returns null for an invalid handle.
///
/// # Safety
///
/// - `coll` must be a live handle; the pointer is valid until
///   [`rft_collection_close`].
#[no_mangle]
pub unsafe extern "C" fn rft_coll_generation(coll: *mut RaftCollection) -> *const u64 {
    match unsafe { live_coll(coll) } {
        Ok(c) => c.generation.as_ptr() as *const u64,
        Err(_) => ptr::null(),
    }
}

/// Point read through a prepared handle: one lookup, zero heap allocation
/// (thread-local scratch), binary-codec payload. Buffer semantics match
/// [`rft_collection_get_buf`](super::rft_collection_get_buf).
///
/// # Safety
///
/// - `coll` must be a live handle whose database is still open.
/// - `out_len` must be a valid `*mut usize`; `out_buf` must point to
///   `*out_len` writable bytes (or be null to query the size).
#[no_mangle]
pub unsafe extern "C" fn rft_coll_get_buf(
    coll: *mut RaftCollection,
    doc_id: u64,
    out_buf: *mut u8,
    out_len: *mut usize,
) -> RftError {
    super::guard(|| {
        let c = match unsafe { live_coll(coll) } {
            Ok(c) => c,
            Err(e) => return e,
        };
        if out_len.is_null() {
            return RftError::NullPointer;
        }
        let handle = match unsafe { super::live_db(c.db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        SCRATCH.with(|scratch| {
            let mut scratch = scratch.borrow_mut();
            scratch.clear();
            let found = handle
                .database()
                .with_doc(&c.name, DocId(doc_id), |doc| {
                    crate::codec::encode_doc(doc, &mut scratch);
                })
                .is_some();
            if !found {
                return RftError::NotFound;
            }
            unsafe { write_buffer(&scratch, out_buf, out_len) }
        })
    })
}

/// Batch point-read: fetch every id in `ids` that exists, in one crossing
/// and one lock hold. On success writes a buffer handle (binary batch
/// encoding, input order, misses skipped) to `*out_buf`; release it with
/// [`rft_buf_free`](super::rft_buf_free).
///
/// # Safety
///
/// - `coll` must be a live handle whose database is still open.
/// - `ids` must point to `count` readable `u64`s (null only if `count`
///   is 0); `out_buf` must be a writable `*mut *mut RftBuf`.
#[no_mangle]
pub unsafe extern "C" fn rft_coll_get_many(
    coll: *mut RaftCollection,
    ids: *const u64,
    count: usize,
    out_buf: *mut *mut RftBuf,
) -> RftError {
    super::guard(|| {
        let c = match unsafe { live_coll(coll) } {
            Ok(c) => c,
            Err(e) => return e,
        };
        if out_buf.is_null() {
            return RftError::NullPointer;
        }
        let handle = match unsafe { super::live_db(c.db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        let id_slice = match unsafe { super::input_slice(ids, count) } {
            Ok(value) => value,
            Err(e) => return e,
        };
        let doc_ids: Vec<DocId> = id_slice.iter().map(|&id| DocId(id)).collect();

        let mut enc = crate::codec::BatchEncoder::new();
        handle
            .database()
            .get_many_visit(&c.name, &doc_ids, |doc| enc.push(doc));
        let raw = Box::into_raw(Box::new(RftBuf::new(enc.finish())));
        unsafe { ptr::write(out_buf, raw) };
        RftError::Ok
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::super::bulk::rft_collection_put_many;
    use super::super::{rft_buf_data, rft_buf_free, rft_buf_len, RftError};
    use super::*;
    use crate::codec::{decode_batch, decode_doc, encode_batch};
    use crate::query::{Document, Value};

    fn doc(id: u64, score: i64) -> Document {
        let mut fields = crate::query::Fields::new();
        fields.insert("score".into(), Value::Int(score));
        Document {
            id: DocId(id),
            fields,
        }
    }

    unsafe fn open_db(name: &str) -> (*mut RaftDb, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("rft_coll_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let c_path = std::ffi::CString::new(dir.to_str().unwrap()).unwrap();
        let mut err = RftError::Ok;
        let db = unsafe { super::super::rft_open(c_path.as_ptr(), &mut err) };
        assert!(!db.is_null());
        (db, dir)
    }

    const COLL: &[u8] = b"bench\0";

    #[test]
    fn handle_get_buf_and_get_many_roundtrip() {
        unsafe {
            let (db, dir) = open_db("roundtrip");
            let batch = encode_batch(&(1..=20).map(|i| doc(i, i as i64)).collect::<Vec<_>>());
            rft_collection_put_many(db, COLL.as_ptr().cast(), batch.as_ptr(), batch.len());

            let mut coll: *mut RaftCollection = ptr::null_mut();
            assert_eq!(
                rft_collection_open(db, COLL.as_ptr().cast(), &mut coll),
                RftError::Ok
            );

            // Point read through the handle.
            let mut buf = vec![0u8; 1024];
            let mut len = buf.len();
            assert_eq!(
                rft_coll_get_buf(coll, 7, buf.as_mut_ptr(), &mut len),
                RftError::Ok
            );
            let d = decode_doc(&buf[..len]).unwrap();
            assert_eq!(d.fields.get("score"), Some(&Value::Int(7)));

            let mut len2 = buf.len();
            assert_eq!(
                rft_coll_get_buf(coll, 999, buf.as_mut_ptr(), &mut len2),
                RftError::NotFound
            );

            // Batch read: misses skipped, order preserved.
            let ids = [5u64, 999, 1, 20];
            let mut out: *mut RftBuf = ptr::null_mut();
            assert_eq!(
                rft_coll_get_many(coll, ids.as_ptr(), ids.len(), &mut out),
                RftError::Ok
            );
            let bytes = slice::from_raw_parts(rft_buf_data(out), rft_buf_len(out));
            let docs = decode_batch(bytes).unwrap();
            assert_eq!(
                docs.iter().map(|d| d.id.0).collect::<Vec<_>>(),
                vec![5, 1, 20]
            );
            rft_buf_free(out);

            rft_collection_close(coll);
            super::super::rft_close(db);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    #[test]
    fn generation_pointer_tracks_writes() {
        unsafe {
            let (db, dir) = open_db("generation");
            let mut coll: *mut RaftCollection = ptr::null_mut();
            rft_collection_open(db, COLL.as_ptr().cast(), &mut coll);

            let gen_ptr = rft_coll_generation(coll);
            assert!(!gen_ptr.is_null());
            let before = ptr::read_volatile(gen_ptr);

            let batch = encode_batch(&[doc(1, 1)]);
            rft_collection_put_many(db, COLL.as_ptr().cast(), batch.as_ptr(), batch.len());
            let after = ptr::read_volatile(gen_ptr);
            assert!(after > before, "write must bump the shared generation");

            // The Arc keeps the counter alive and consistent with the core.
            let arc_val = (*coll).generation.load(Ordering::Acquire);
            assert_eq!(arc_val, after);

            rft_collection_close(coll);
            super::super::rft_close(db);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    #[test]
    fn stale_and_null_handles_fail_safely() {
        unsafe {
            let (db, dir) = open_db("stale");
            let mut coll: *mut RaftCollection = ptr::null_mut();
            rft_collection_open(db, COLL.as_ptr().cast(), &mut coll);
            rft_collection_close(coll);

            // Closed handle: every entry point degrades to an error.
            let mut len = 0usize;
            assert_eq!(
                rft_coll_get_buf(coll, 1, ptr::null_mut(), &mut len),
                RftError::InvalidHandle
            );
            let mut out: *mut RftBuf = ptr::null_mut();
            assert_eq!(
                rft_coll_get_many(coll, ptr::null(), 0, &mut out),
                RftError::InvalidHandle
            );
            assert!(rft_coll_generation(coll).is_null());
            rft_collection_close(coll); // double close: no-op

            assert_eq!(
                rft_coll_get_buf(ptr::null_mut(), 1, ptr::null_mut(), &mut len),
                RftError::InvalidHandle
            );

            super::super::rft_close(db);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}
