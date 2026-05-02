//! Query execution FFI — `rft_query_*` functions.
//!
//! Queries are passed in as a JSON-encoded [`crate::query::Query`] (see
//! the serde representation of `Query`) and execute against the in-memory
//! collection. Results are returned through an opaque
//! [`RaftQueryResult`] handle that holds a snapshot of the matching
//! documents; callers read them out by index and free the handle when
//! done.
//!
//! Wire format (matches `Query` serde derive plus extras for fluent UX):
//!
//! ```json
//! {
//!   "collection": "users",
//!   "filter": { "Condition": { "field": "age", "predicate": "Gte", "value": { "Int": 18 } } },
//!   "sort": { "field": "created_at", "direction": "Descending" },
//!   "limit": 20,
//!   "offset": 40
//! }
//! ```

use std::ptr;
use std::slice;

use serde::Deserialize;

use crate::query::{Document, Filter, Query, Sort};

use super::error::RftError;
use super::handle::RaftDb;
use super::write_buffer;

/// Opaque query-result handle. Holds the snapshot of matching documents.
pub struct RaftQueryResult {
    docs: Vec<Document>,
}

/// JSON wire format for a query. Mirrors [`crate::query::Query`] but
/// without the private fields, so we can deserialize it directly.
#[derive(Debug, Deserialize)]
struct QuerySpec {
    collection: String,
    #[serde(default)]
    filter: Option<Filter>,
    #[serde(default)]
    sort: Option<Sort>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: Option<usize>,
}

impl From<QuerySpec> for Query {
    fn from(s: QuerySpec) -> Self {
        let mut q = Query::collection(s.collection);
        if let Some(f) = s.filter {
            q = q.filter(f);
        }
        if let Some(srt) = s.sort {
            q = q.sort(srt);
        }
        if let Some(lim) = s.limit {
            q = q.limit(lim);
        }
        if let Some(off) = s.offset {
            q = q.offset(off);
        }
        q
    }
}

/// Execute a query and return an opaque result handle. Caller must free
/// it with [`rft_query_result_free`].
///
/// # Safety
///
/// - `db` must be a valid handle.
/// - `query_json` must be a valid UTF-8 buffer of `query_json_len` bytes.
/// - `out_result` must be a valid `*mut *mut RaftQueryResult`.
#[no_mangle]
pub unsafe extern "C" fn rft_query_execute(
    db: *mut RaftDb,
    query_json: *const u8,
    query_json_len: usize,
    out_result: *mut *mut RaftQueryResult,
) -> RftError {
    let Some(handle) = (unsafe { db.as_ref() }) else {
        return RftError::NullPointer;
    };
    if out_result.is_null() || (query_json.is_null() && query_json_len > 0) {
        return RftError::NullPointer;
    }

    let json = unsafe { slice::from_raw_parts(query_json, query_json_len) };
    let spec: QuerySpec = match serde_json::from_slice(json) {
        Ok(s) => s,
        Err(_) => return RftError::InvalidJson,
    };

    let query: Query = spec.into();
    let docs = handle.database().query(&query);

    let result = Box::new(RaftQueryResult { docs });
    unsafe { ptr::write(out_result, Box::into_raw(result)) };
    RftError::Ok
}

/// Number of documents in a query result. Returns 0 for null handles.
///
/// # Safety
///
/// - `result` must be a handle returned by [`rft_query_execute`], or null.
#[no_mangle]
pub unsafe extern "C" fn rft_query_result_count(result: *const RaftQueryResult) -> usize {
    if result.is_null() {
        return 0;
    }
    unsafe { (*result).docs.len() }
}

/// Fetch the JSON encoding of the document at `index` in the result set.
/// On `BufferTooSmall` the required size is written to `*out_len`.
///
/// # Safety
///
/// - `result` must be a handle returned by [`rft_query_execute`].
/// - `out_buf` must be writable for `*out_len` bytes, or null to query
///   the required size.
/// - `out_len` must be a valid `*mut usize`.
#[no_mangle]
pub unsafe extern "C" fn rft_query_result_get(
    result: *const RaftQueryResult,
    index: usize,
    out_buf: *mut u8,
    out_len: *mut usize,
) -> RftError {
    if result.is_null() || out_len.is_null() {
        return RftError::NullPointer;
    }
    let docs = unsafe { &(*result).docs };
    let Some(doc) = docs.get(index) else {
        return RftError::NotFound;
    };

    let bytes = match serde_json::to_vec(doc) {
        Ok(b) => b,
        Err(_) => return RftError::InvalidJson,
    };

    unsafe { write_buffer(&bytes, out_buf, out_len) }
}

/// Free a query result handle. Safe to call with null (no-op).
///
/// # Safety
///
/// - `result` must be a handle returned by [`rft_query_execute`], or null.
/// - After this call, `result` is dangling and must not be reused.
#[no_mangle]
pub unsafe extern "C" fn rft_query_result_free(result: *mut RaftQueryResult) {
    if !result.is_null() {
        drop(unsafe { Box::from_raw(result) });
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

// Suppress an unused-import warning on test-only types.
#[cfg(test)]
#[allow(unused_imports)]
use super::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_query() {
        let json = r#"{"collection":"users"}"#;
        let spec: QuerySpec = serde_json::from_str(json).unwrap();
        let q: Query = spec.into();
        assert_eq!(q.collection_name(), "users");
        assert!(q.get_filter().is_none());
    }

    #[test]
    fn parse_query_with_filter_and_sort() {
        let json = r#"{
            "collection":"users",
            "filter":{"Condition":{"field":"age","predicate":"Gte","value":{"Int":18}}},
            "sort":{"field":"name","direction":"Ascending"},
            "limit":10,
            "offset":5
        }"#;
        let spec: QuerySpec = serde_json::from_str(json).unwrap();
        let q: Query = spec.into();
        assert_eq!(q.get_limit(), Some(10));
        assert_eq!(q.get_offset(), Some(5));
    }
}
