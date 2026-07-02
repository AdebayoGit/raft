//! Fuzz the FFI JSON envelopes through the public C ABI (X2).
//!
//! Feeds arbitrary bytes as document JSON (`rft_collection_put`) and
//! query-spec JSON (`rft_query_execute`). Malformed input must map to a
//! clean `RftError` — never a panic across the FFI boundary, UB, or a
//! leaked handle.

#![no_main]

use std::ffi::CString;
use std::sync::OnceLock;

use libfuzzer_sys::fuzz_target;
use raftdb::ffi::{
    rft_collection_put, rft_open, rft_query_execute, rft_query_result_count,
    rft_query_result_free, RaftDb, RftError,
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

    if selector % 2 == 0 {
        // Document envelope: arbitrary bytes as document JSON.
        let _ = unsafe { rft_collection_put(db, collection, json.as_ptr(), json.len()) };
    } else {
        // Query envelope: arbitrary bytes as a query spec.
        let mut result = std::ptr::null_mut();
        let err = unsafe { rft_query_execute(db, json.as_ptr(), json.len(), &mut result) };
        if err == RftError::Ok {
            let _ = unsafe { rft_query_result_count(result) };
            unsafe { rft_query_result_free(result) };
        }
    }
});
