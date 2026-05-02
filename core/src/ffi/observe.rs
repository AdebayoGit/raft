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
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;

use super::error::RftError;
use super::handle::RaftDb;

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
    let Some(handle) = (unsafe { db.as_ref() }) else {
        return RftError::NullPointer;
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

    let sub_id = {
        let mut reg = handle.subscriptions.lock().expect("subscriptions poisoned");
        reg.next_id += 1;
        let id = reg.next_id;
        reg.handles.insert(id, join_handle);
        id
    };

    GLOBAL_OBSERVER_COUNT.fetch_add(1, Ordering::Relaxed);
    unsafe { ptr::write(out_sub_id, sub_id) };
    RftError::Ok
}

/// Cancel a subscription previously created by [`rft_observe`]. Aborts
/// the background task and removes it from the registry. Calling this
/// with an unknown id returns [`RftError::UnknownSubscription`].
///
/// # Safety
///
/// - `db` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn rft_unobserve(db: *mut RaftDb, sub_id: u64) -> RftError {
    let Some(handle) = (unsafe { db.as_ref() }) else {
        return RftError::NullPointer;
    };

    let join = {
        let mut reg = handle.subscriptions.lock().expect("subscriptions poisoned");
        reg.handles.remove(&sub_id)
    };

    match join {
        Some(j) => {
            j.abort();
            RftError::Ok
        }
        None => RftError::UnknownSubscription,
    }
}

/// Used by [`rft_close`] to abort all pending subscriptions before
/// dropping the database. Without this, observer tasks would outlive
/// the database and panic on access.
pub(super) fn abort_all_subscriptions(db: &RaftDb) {
    let handles: Vec<JoinHandle<()>> = {
        let mut reg = db.subscriptions.lock().expect("subscriptions poisoned");
        reg.handles.drain().map(|(_, j)| j).collect()
    };
    for j in handles {
        j.abort();
    }
}
