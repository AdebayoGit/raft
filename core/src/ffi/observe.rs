//! Live-query subscription FFI — `rft_observe` / `rft_unobserve`.
//!
//! The bridge between Rust's reactive layer and platform native callbacks.
//! On `rft_observe`, we spawn a tokio task that subscribes to the
//! database event bus, filters by collection, and invokes the C callback
//! with a JSON-encoded [`MutationEvent`] each time a relevant change
//! happens. The returned subscription id is used by `rft_unobserve` to
//! cancel the task.
//!
//! Wire format for the event delivered to the callback:
//!
//! ```json
//! {
//!   "collection": "users",
//!   "doc_id": 42,
//!   "mutation_type": "Insert",
//!   "origin": "Local"
//! }
//! ```
//!
//! Memory ownership: the JSON string passed to the callback is owned by
//! the FFI layer and only valid for the duration of the call. Callers
//! must copy it if they need to retain it.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;

use crate::query::Query;
use crate::reactive::QueryDiff;

use super::error::RftError;
use super::handle::RaftDb;
use super::query::query_from_json;

/// C-compatible callback signature.
///
/// `event_json` is a null-terminated UTF-8 string valid only for the
/// duration of the call. `user_data` is the opaque pointer passed to
/// [`rft_observe`].
pub type RftObserveCallback =
    unsafe extern "C" fn(event_json: *const c_char, user_data: *mut c_void);

/// `*mut c_void` is not `Send` and cannot cross await points. We pass
/// the pointer across the boundary as a `usize` instead, which is `Send`,
/// and reconstruct the pointer at the call site. The platform binding
/// is responsible for ensuring the user-data pointer remains valid
/// for the subscription lifetime.
#[derive(Clone, Copy)]
struct UserData(usize);

impl UserData {
    fn new(ptr: *mut c_void) -> Self {
        Self(ptr as usize)
    }

    fn as_ptr(self) -> *mut c_void {
        self.0 as *mut c_void
    }
}

/// Subscription registry. Maps `subscription_id → JoinHandle`. The handle
/// is aborted on `unobserve`, which causes the observer task to exit at
/// its next await point.
#[derive(Default)]
pub struct SubscriptionRegistry {
    next_id: u64,
    handles: HashMap<u64, JoinHandle<()>>,
}

static GLOBAL_OBSERVER_COUNT: AtomicU64 = AtomicU64::new(0);

/// Insert `join_handle` into the handle's subscription registry and
/// return the freshly assigned subscription id. Shared by the C-callback
/// observers here and the Dart-port observers in [`super::dart_port`].
pub(super) fn register_subscription(handle: &RaftDb, join_handle: JoinHandle<()>) -> u64 {
    let id = {
        let mut reg = handle
            .subscriptions
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        reg.next_id += 1;
        let id = reg.next_id;
        reg.handles.insert(id, join_handle);
        id
    };
    GLOBAL_OBSERVER_COUNT.fetch_add(1, Ordering::Relaxed);
    id
}

/// Register an observer callback for `collection`. The callback fires
/// whenever a document in that collection is inserted, updated, or
/// deleted.
///
/// Returns a non-zero subscription id via `out_sub_id` on success. Pass
/// it to [`rft_unobserve`] to cancel the subscription.
///
/// # Safety
///
/// - `db` must be a valid handle.
/// - `collection` must be a valid null-terminated UTF-8 C string.
/// - `callback` must be a valid C function pointer that remains valid
///   until [`rft_unobserve`] returns.
/// - `user_data` is opaque to Rust; the platform binding is responsible
///   for managing its lifetime so it remains valid for the subscription.
/// - `out_sub_id` must be a valid `*mut u64`.
#[no_mangle]
pub unsafe extern "C" fn rft_observe(
    db: *mut RaftDb,
    collection: *const c_char,
    callback: RftObserveCallback,
    user_data: *mut c_void,
    out_sub_id: *mut u64,
) -> RftError {
    super::guard(|| {
        let handle = match unsafe { super::live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        if collection.is_null() || out_sub_id.is_null() {
            return RftError::NullPointer;
        }

        let coll = match unsafe { CStr::from_ptr(collection) }.to_str() {
            Ok(s) => s.to_string(),
            Err(_) => return RftError::InvalidUtf8,
        };

        let mut rx = handle.database().subscribe();
        let wrapped_user_data = UserData::new(user_data);

        let join_handle = handle.runtime().spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        if event.collection != coll {
                            continue;
                        }
                        let json = match serde_json::to_string(&event) {
                            Ok(s) => s,
                            Err(_) => continue,
                        };
                        let cstring = match CString::new(json) {
                            Ok(c) => c,
                            Err(_) => continue,
                        };
                        // SAFETY: callback is a C function pointer the caller
                        // promised remains valid for the subscription
                        // lifetime; user_data is opaque and managed by the
                        // caller.
                        unsafe {
                            callback(cstring.as_ptr(), wrapped_user_data.as_ptr());
                        }
                    }
                    Err(RecvError::Lagged(_)) => {
                        // Skip silently — the subscriber missed events but
                        // can keep going. Platform bindings can re-query if
                        // they need a precise re-sync.
                        continue;
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });

        let sub_id = register_subscription(handle, join_handle);
        unsafe { ptr::write(out_sub_id, sub_id) };
        RftError::Ok
    })
}

/// Register a *live query* subscription for `query_json`. The callback
/// fires immediately with an initial-snapshot diff (every matching
/// document in `added`, others empty) and then again every time a
/// mutation in the queried collection causes the result set to change.
///
/// Each diff is delivered as JSON:
///
/// ```json
/// {
///   "added":   [<Document>, ...],
///   "removed": [<Document>, ...],
///   "updated": [<Document>, ...]
/// }
/// ```
///
/// # Safety
///
/// - `db` must be a valid handle.
/// - `query_json` must be a valid UTF-8 buffer of `query_json_len` bytes.
/// - `callback` must be a valid C function pointer that remains valid
///   until [`rft_unobserve`] returns.
/// - `user_data` is opaque to Rust; the platform binding owns its
///   lifetime and must keep it valid for the subscription.
/// - `out_sub_id` must be a valid `*mut u64`.
#[no_mangle]
pub unsafe extern "C" fn rft_observe_query(
    db: *mut RaftDb,
    query_json: *const u8,
    query_json_len: usize,
    callback: RftObserveCallback,
    user_data: *mut c_void,
    out_sub_id: *mut u64,
) -> RftError {
    super::guard(|| {
        let handle = match unsafe { super::live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        if out_sub_id.is_null() || (query_json.is_null() && query_json_len > 0) {
            return RftError::NullPointer;
        }

        let json = unsafe { slice::from_raw_parts(query_json, query_json_len) };
        let query: Query = match query_from_json(json) {
            Ok(q) => q,
            Err(e) => return e,
        };

        // Atomically (subscribe → snapshot) so we don't drop any mutations
        // between the snapshot and the first diff.
        let (initial, mut live) = handle.database().live_query(query);

        // Emit the initial snapshot synchronously so callers always see the
        // current state before the task starts. This matches the platform
        // bindings' `observe(...)` semantics on Kotlin/Swift/Dart.
        let snapshot = QueryDiff {
            added: initial,
            removed: Vec::new(),
            updated: Vec::new(),
        };
        let wrapped_user_data = UserData::new(user_data);
        if let Err(err) = fire_diff(&snapshot, callback, wrapped_user_data.as_ptr()) {
            return err;
        }

        let join_handle = handle.runtime().spawn(async move {
            while let Some(diff) = live.next_diff().await {
                let _ = fire_diff(&diff, callback, wrapped_user_data.as_ptr());
            }
        });

        let sub_id = register_subscription(handle, join_handle);
        unsafe { ptr::write(out_sub_id, sub_id) };
        RftError::Ok
    })
}

/// Encode `diff` as JSON and invoke `callback` with the resulting C string.
///
/// # Safety
///
/// - `callback` must remain valid for the duration of the call.
/// - `user_data` is passed back unchanged to the callback.
fn fire_diff(
    diff: &QueryDiff,
    callback: RftObserveCallback,
    user_data: *mut c_void,
) -> Result<(), RftError> {
    let json = serde_json::to_string(diff).map_err(|_| RftError::InvalidJson)?;
    let cstring = CString::new(json).map_err(|_| RftError::InvalidJson)?;
    // SAFETY: caller of rft_observe_query promised the callback pointer
    // remains valid for the subscription lifetime; user_data is opaque
    // and managed by the caller.
    unsafe {
        callback(cstring.as_ptr(), user_data);
    }
    Ok(())
}

/// Cancel a subscription previously created by [`rft_observe`] or
/// [`rft_observe_query`]. Aborts the background task and removes it
/// from the registry. Calling this with an unknown id returns
/// [`RftError::UnknownSubscription`].
///
/// # Safety
///
/// - `db` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn rft_unobserve(db: *mut RaftDb, sub_id: u64) -> RftError {
    super::guard(|| {
        let handle = match unsafe { super::live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };

        let join = {
            let mut reg = handle
                .subscriptions
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            reg.handles.remove(&sub_id)
        };

        match join {
            Some(j) => {
                j.abort();
                RftError::Ok
            }
            None => RftError::UnknownSubscription,
        }
    })
}

/// Used by [`rft_close`] to abort all pending subscriptions before
/// dropping the database. Without this, observer tasks would outlive
/// the database and panic on access.
pub(super) fn abort_all_subscriptions(db: &RaftDb) {
    let handles: Vec<JoinHandle<()>> = {
        let mut reg = db.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
        reg.handles.drain().map(|(_, j)| j).collect()
    };
    for j in handles {
        j.abort();
    }
}
