//! Fuzz the FFI JSON envelopes through the public C ABI (X2).
//!
//! Feeds arbitrary bytes as document JSON (`rft_collection_put`),
//! query-spec JSON (`rft_query_execute`), and binary batch payloads
//! (`rft_collection_put_many`, exercising the binary document codec's
//! decode path). Malformed input must map to a clean `RftError` — never a
//! panic across the FFI boundary, UB, an over-allocation, or a leaked
//! handle.

#![no_main]

use std::ffi::CString;
use std::sync::OnceLock;

use libfuzzer_sys::fuzz_target;
use raftdb::ffi::{
    rft_buf_free, rft_collection_put, rft_collection_put_many, rft_collection_scan, rft_open,
    rft_query_execute, rft_query_result_count, rft_query_result_free, RaftDb, RftBuf, RftError,
};

/// One database per fuzz process, opened lazily. Stored as a usize
/// because raw pointers aren't `Sync`; the handle itself is thread-safe.
fn db() -> *mut RaftDb {
    static DB: OnceLock<usize> = OnceLock::new();
    *DB.get_or_init(|| {
        let dir = std::env::temp_dir()
            .join("raft_db_fuzz_ffi")
            .join(std::process::id().to_string());
        std::fs::create_dir_all(&dir).expect("create db dir");
        let c_path = CString::new(dir.to_str().expect("utf8 tmp path")).expect("no NUL");
        let mut err = RftError::Ok;
        let handle = unsafe { rft_open(c_path.as_ptr(), &mut err) };
        assert!(!handle.is_null(), "rft_open failed: {err:?}");
        handle as usize
    }) as *mut RaftDb
}

fuzz_target!(|data: &[u8]| {
    let Some((&selector, json)) = data.split_first() else {
        return;
    };
    let db = db();
    let collection = c"fuzz".as_ptr();

    match selector % 3 {
        0 => {
            // Document envelope: arbitrary bytes as document JSON.
            let _ = unsafe { rft_collection_put(db, collection, json.as_ptr(), json.len()) };
        }
        1 => {
            // Query envelope: arbitrary bytes as a query spec.
            let mut result = std::ptr::null_mut();
            let err = unsafe { rft_query_execute(db, json.as_ptr(), json.len(), &mut result) };
            if err == RftError::Ok {
                let _ = unsafe { rft_query_result_count(result) };
                unsafe { rft_query_result_free(result) };
            }
        }
        _ => {
            // Binary batch envelope: arbitrary bytes through the codec's
            // decode_batch_spans path. Must reject cleanly, never panic or
            // over-allocate on a lying count/length prefix. If it somehow
            // decodes, a scan must round-trip without panic.
            let _ =
                unsafe { rft_collection_put_many(db, collection, json.as_ptr(), json.len()) };
            let mut out: *mut RftBuf = std::ptr::null_mut();
            let err = unsafe { rft_collection_scan(db, collection, &mut out) };
            if err == RftError::Ok && !out.is_null() {
                unsafe { rft_buf_free(out) };
            }
        }
    }
});
