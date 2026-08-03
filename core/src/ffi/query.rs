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

use serde::Deserialize;

use crate::query::{Document, Filter, Query, Sort};

use super::error::RftError;
use super::handle::RaftDb;
use super::write_buffer;

/// Opaque query-result handle. Holds the snapshot of matching documents.
pub struct RaftQueryResult {
    docs: Vec<Document>,
}

/// Envelope schema versions this build understands.
const SUPPORTED_ENVELOPE_VERSION: u32 = 1;

/// JSON wire format for a query. Mirrors [`crate::query::Query`] but
/// without the private fields, so we can deserialize it directly.
///
/// The envelope is strict (unknown keys are rejected) and versioned: an
/// optional `"v"` field declares the schema version, defaulting to 1.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct QuerySpec {
    /// Envelope schema version. Absent means version 1.
    #[serde(default)]
    v: Option<u32>,
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

/// Parse a query JSON byte buffer into a [`Query`], enforcing the size
/// cap, strict (unknown-key-rejecting) parsing, and the envelope
/// version. Used by [`rft_query_execute`],
/// [`super::observe::rft_observe_query`], and
/// [`super::dart_port::rft_observe_query_dart_port`].
pub(super) fn query_from_json(bytes: &[u8]) -> Result<Query, RftError> {
    if bytes.len() > super::RFT_MAX_QUERY_JSON_LEN {
        return Err(RftError::PayloadTooLarge);
    }
    let spec: QuerySpec = serde_json::from_slice(bytes).map_err(|_| RftError::InvalidJson)?;
    if spec.v.unwrap_or(SUPPORTED_ENVELOPE_VERSION) != SUPPORTED_ENVELOPE_VERSION {
        return Err(RftError::UnsupportedVersion);
    }
    Ok(Query::from(spec))
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
    super::guard(|| {
        let handle = match unsafe { super::live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        if out_result.is_null() {
            return RftError::NullPointer;
        }

        let json = match unsafe { super::input_slice(query_json, query_json_len) } {
            Ok(value) => value,
            Err(e) => return e,
        };
        let query = match query_from_json(json) {
            Ok(q) => q,
            Err(e) => return e,
        };
        let docs = handle.database().query(&query);

        let raw = Box::into_raw(Box::new(RaftQueryResult { docs }));
        super::registry::LIVE_QUERY_RESULTS.register(raw);
        unsafe { ptr::write(out_result, raw) };
        RftError::Ok
    })
}

/// Number of documents in a query result. Returns 0 for null, freed, or
/// otherwise invalid handles.
///
/// # Safety
///
/// - `result` must be a handle returned by [`rft_query_execute`], or null.
#[no_mangle]
pub unsafe extern "C" fn rft_query_result_count(result: *const RaftQueryResult) -> usize {
    super::guard_or(0, || match unsafe { super::live_query_result(result) } {
        Ok(r) => r.docs.len(),
        Err(_) => 0,
    })
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
    super::guard(|| {
        let res = match unsafe { super::live_query_result(result) } {
            Ok(r) => r,
            Err(e) => return e,
        };
        if out_len.is_null() {
            return RftError::NullPointer;
        }
        let Some(doc) = res.docs.get(index) else {
            return RftError::NotFound;
        };

        let bytes = match serde_json::to_vec(doc) {
            Ok(b) => b,
            Err(_) => return RftError::InvalidJson,
        };

        unsafe { write_buffer(&bytes, out_buf, out_len) }
    })
}

/// Free a query result handle. Safe to call with null (no-op). Freeing
/// an already-freed or foreign pointer is also a safe no-op.
///
/// # Safety
///
/// - `result` must be a handle returned by [`rft_query_execute`], or null.
/// - After this call, `result` is dangling and must not be reused.
#[no_mangle]
pub unsafe extern "C" fn rft_query_result_free(result: *mut RaftQueryResult) {
    super::guard_or((), || {
        // Unregister-wins: exactly one concurrent free proceeds.
        if !result.is_null() && super::registry::LIVE_QUERY_RESULTS.unregister(result) {
            drop(unsafe { Box::from_raw(result) });
        }
    });
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
    fn parse_rejects_unknown_envelope_keys() {
        let json = br#"{"collection":"users","evil":true}"#;
        assert!(matches!(query_from_json(json), Err(RftError::InvalidJson)));
    }

    #[test]
    fn parse_accepts_current_envelope_version() {
        let json = br#"{"v":1,"collection":"users"}"#;
        let q = query_from_json(json).unwrap();
        assert_eq!(q.collection_name(), "users");
    }

    #[test]
    fn parse_rejects_future_envelope_version() {
        let json = br#"{"v":2,"collection":"users"}"#;
        assert!(matches!(
            query_from_json(json),
            Err(RftError::UnsupportedVersion)
        ));
    }

    #[test]
    fn parse_rejects_oversized_query_envelope() {
        let mut json = br#"{"collection":""#.to_vec();
        json.extend(std::iter::repeat_n(
            b'a',
            super::super::RFT_MAX_QUERY_JSON_LEN,
        ));
        json.extend_from_slice(br#""}"#);
        assert!(matches!(
            query_from_json(&json),
            Err(RftError::PayloadTooLarge)
        ));
    }

    #[test]
    fn document_envelope_enforces_size_cap() {
        let mut json = br#"{"id":1,"fields":{"blob":{"String":""#.to_vec();
        // Stay structurally valid but exceed the cap.
        json.resize(super::super::RFT_MAX_DOC_JSON_LEN + 16, b'a');
        json.extend_from_slice(br#""}}}"#);
        assert!(matches!(
            super::super::document_from_json(&json),
            Err(RftError::PayloadTooLarge)
        ));
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
