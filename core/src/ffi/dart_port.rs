//! Dart-port event delivery — `rft_dart_init` / `rft_observe_dart_port` /
//! `rft_observe_query_dart_port`.
//!
//! The C-callback observers in [`super::observe`] hand the platform a
//! `*const c_char` that is only valid for the duration of the call. That
//! contract cannot be satisfied by Dart's `NativeCallable.listener`, which
//! dispatches asynchronously — by the time the Dart closure runs, the
//! CString is long dead.
//!
//! This module solves the lifetime problem with the `Dart_PostCObject_DL`
//! route: Dart hands us the address of the VM's `Dart_PostCObject_DL`
//! function once (via [`rft_dart_init`]), and observers post each event as
//! a `Dart_CObject` of type *string* to a Dart `SendPort`. The VM
//! **serializes (copies) the message into the port's queue before the
//! post call returns**, so the Rust-owned CString only has to outlive the
//! call — exactly the guarantee we can provide. The event arrives in Dart
//! as a plain `String` on a `ReceivePort`.
//!
//! The wire payloads are identical to the C-callback observers: a
//! [`MutationEvent`](crate::reactive::MutationEvent) JSON object per
//! mutation for collection observers, and a
//! [`QueryDiff`](crate::reactive::QueryDiff) JSON object per result-set
//! change for live-query observers.
//!
//! No Dart SDK headers or crates are required — the only ABI surface we
//! rely on is the layout of `Dart_CObject` for the *string* variant
//! (a 32-bit type tag followed by a union whose first word is the
//! `char*`), which is a stable part of the Dart embedding API.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::broadcast::error::RecvError;

use crate::query::Query;
use crate::reactive::{MutationEvent, QueryDiff};

use super::error::RftError;
use super::handle::RaftDb;
use super::observe::register_subscription;
use super::query::query_from_json;

/// `Dart_CObject_Type` tag for a null-terminated C string
/// (`Dart_CObject_kString` in `dart_native_api.h`).
const DART_COBJECT_STRING: i32 = 5;

/// Minimal mirror of the Dart embedding API's `Dart_CObject` for the
/// variants this module posts. The real union has more (and larger)
/// members; the VM only reads the member selected by `ty`, and the
/// padding below keeps this struct at least as large as the real one so
/// the VM never reads past our allocation.
#[repr(C)]
struct DartCObject {
    ty: i32,
    value: DartCObjectValue,
}

#[repr(C)]
union DartCObjectValue {
    as_string: *const c_char,
    /// Pads the union to the size of the largest member of the real
    /// `Dart_CObject` union (`as_external_typed_data`, five words).
    _pad: [usize; 5],
}

/// Signature of `Dart_PostCObject_DL` from the Dart embedding API.
type DartPostCObjectFn = unsafe extern "C" fn(port_id: i64, message: *mut DartCObject) -> bool;

/// Address of the VM's `Dart_PostCObject_DL`, stored as a `usize`
/// (0 = not initialized). Set once by [`rft_dart_init`].
static POST_COBJECT: AtomicUsize = AtomicUsize::new(0);

/// Register the Dart VM's `Dart_PostCObject_DL` function so observers
/// can deliver events to Dart `SendPort`s.
///
/// Dart callers pass `NativeApi.postCObject.address` (from `dart:ffi`).
/// Must be called once per process before any `rft_observe_*_dart_port`
/// call; calling it again is harmless.
///
/// # Safety
///
/// - `post_cobject_fn` must be the address of the Dart VM's
///   `Dart_PostCObject_DL` function (or a function with an identical
///   ABI), and must remain valid for the lifetime of the process.
#[no_mangle]
pub unsafe extern "C" fn rft_dart_init(post_cobject_fn: *mut c_void) -> RftError {
    super::guard(|| {
        if post_cobject_fn.is_null() {
            return RftError::NullPointer;
        }
        POST_COBJECT.store(post_cobject_fn as usize, Ordering::Release);
        RftError::Ok
    })
}

/// Post `json` to the Dart port `port` as a string `Dart_CObject`.
///
/// Returns `false` if the Dart API was never initialized, the payload
/// contains an interior NUL, or the VM rejected the post (e.g. the port
/// was already closed).
fn post_to_dart_port(port: i64, json: &str) -> bool {
    let fp = POST_COBJECT.load(Ordering::Acquire);
    if fp == 0 {
        return false;
    }
    let cstring = match CString::new(json) {
        Ok(c) => c,
        Err(_) => return false,
    };
    let mut message = DartCObject {
        ty: DART_COBJECT_STRING,
        value: DartCObjectValue {
            as_string: cstring.as_ptr(),
        },
    };
    // SAFETY: `fp` is non-zero, so it was stored by `rft_dart_init`, whose
    // caller promised it is `Dart_PostCObject_DL` (correct ABI, valid for
    // the process lifetime). The VM serializes the message into the port
    // queue before returning, so `cstring` outliving the call suffices.
    let post: DartPostCObjectFn = unsafe { std::mem::transmute::<usize, DartPostCObjectFn>(fp) };
    unsafe { post(port, &mut message) }
}

/// Encode `diff` as JSON and post it to `port`.
fn post_diff(diff: &QueryDiff, port: i64) -> Result<(), RftError> {
    let json = serde_json::to_string(diff).map_err(|_| RftError::InvalidJson)?;
    post_to_dart_port(port, &json);
    Ok(())
}

/// Register a collection observer that delivers each mutation event to
/// the Dart `SendPort` `port` as a JSON string (same payload as
/// [`rft_observe`](super::rft_observe)).
///
/// Requires a prior successful [`rft_dart_init`]; otherwise returns
/// [`RftError::DartApiNotInitialized`]. Cancel with
/// [`rft_unobserve`](super::rft_unobserve); closing the Dart
/// `ReceivePort` alone stops delivery but leaks the background task
/// until `rft_unobserve` or `rft_close`.
///
/// # Safety
///
/// - `db` must be a valid handle.
/// - `collection` must be a valid null-terminated UTF-8 C string.
/// - `port` must be a native port id obtained from a Dart `ReceivePort`.
/// - `out_sub_id` must be a valid `*mut u64`.
#[no_mangle]
pub unsafe extern "C" fn rft_observe_dart_port(
    db: *mut RaftDb,
    collection: *const c_char,
    port: i64,
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
        if POST_COBJECT.load(Ordering::Acquire) == 0 {
            return RftError::DartApiNotInitialized;
        }

        let coll = match unsafe { CStr::from_ptr(collection) }.to_str() {
            Ok(s) => s.to_string(),
            Err(_) => return RftError::InvalidUtf8,
        };

        let mut rx = handle.database().subscribe();
        let join_handle = handle.runtime().spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        if event.collection != coll {
                            continue;
                        }
                        if let Ok(json) = serde_json::to_string(&event) {
                            post_to_dart_port(port, &json);
                        }
                    }
                    Err(RecvError::Lagged(_)) => {
                        let event = MutationEvent::resync_required(&coll);
                        if let Ok(json) = serde_json::to_string(&event) {
                            post_to_dart_port(port, &json);
                        }
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

/// Register a live-query observer that delivers each
/// [`QueryDiff`](crate::reactive::QueryDiff) to the Dart `SendPort`
/// `port` as a JSON string (same payload and initial-snapshot semantics
/// as [`rft_observe_query`](super::rft_observe_query)).
///
/// Requires a prior successful [`rft_dart_init`]; otherwise returns
/// [`RftError::DartApiNotInitialized`].
///
/// # Safety
///
/// - `db` must be a valid handle.
/// - `query_json` must be a valid UTF-8 buffer of `query_json_len` bytes.
/// - `port` must be a native port id obtained from a Dart `ReceivePort`.
/// - `out_sub_id` must be a valid `*mut u64`.
#[no_mangle]
pub unsafe extern "C" fn rft_observe_query_dart_port(
    db: *mut RaftDb,
    query_json: *const u8,
    query_json_len: usize,
    port: i64,
    out_sub_id: *mut u64,
) -> RftError {
    super::guard(|| {
        let handle = match unsafe { super::live_db(db) } {
            Ok(h) => h,
            Err(e) => return e,
        };
        if out_sub_id.is_null() {
            return RftError::NullPointer;
        }
        if POST_COBJECT.load(Ordering::Acquire) == 0 {
            return RftError::DartApiNotInitialized;
        }

        let json = match unsafe { super::input_slice(query_json, query_json_len) } {
            Ok(value) => value,
            Err(e) => return e,
        };
        let query: Query = match query_from_json(json) {
            Ok(q) => q,
            Err(e) => return e,
        };

        // Atomically (subscribe → snapshot) so no mutation is dropped
        // between the snapshot and the first diff.
        let (initial, mut live) = handle.database().live_query(query);

        // Post the initial snapshot before the task starts, matching the
        // C-callback observer's semantics.
        let snapshot = QueryDiff {
            added: initial,
            removed: Vec::new(),
            updated: Vec::new(),
        };
        if let Err(err) = post_diff(&snapshot, port) {
            return err;
        }

        let join_handle = handle.runtime().spawn(async move {
            while let Some(diff) = live.next_diff().await {
                let _ = post_diff(&diff, port);
            }
        });

        let sub_id = register_subscription(handle, join_handle);
        unsafe { ptr::write(out_sub_id, sub_id) };
        RftError::Ok
    })
}

#[cfg(test)]
pub(super) mod test_support {
    //! A fake `Dart_PostCObject_DL` for tests: records every posted
    //! `(port, string)` pair, standing in for the Dart VM.

    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn posted() -> &'static Mutex<Vec<(i64, String)>> {
        static POSTED: OnceLock<Mutex<Vec<(i64, String)>>> = OnceLock::new();
        POSTED.get_or_init(|| Mutex::new(Vec::new()))
    }

    /// Fake `Dart_PostCObject_DL`: copies the string payload out of the
    /// message during the call — the same lifetime contract as the VM.
    unsafe extern "C" fn fake_post_cobject(port: i64, message: *mut DartCObject) -> bool {
        // SAFETY: called only via `post_to_dart_port`, which passes a
        // valid message of type string whose payload outlives the call.
        let s = unsafe {
            assert_eq!((*message).ty, DART_COBJECT_STRING);
            CStr::from_ptr((*message).value.as_string)
                .to_str()
                .unwrap()
                .to_string()
        };
        posted().lock().unwrap().push((port, s));
        true
    }

    /// Address of [`fake_post_cobject`] suitable for `rft_dart_init`.
    pub fn fake_post_cobject_addr() -> *mut c_void {
        fake_post_cobject as *const () as usize as *mut c_void
    }

    /// Messages recorded for `port`, in delivery order.
    pub fn messages_for(port: i64) -> Vec<String> {
        posted()
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| *p == port)
            .map(|(_, s)| s.clone())
            .collect()
    }

    /// Reset the initialized-API state so a test can assert the
    /// uninitialized error path. Tests using this must serialize
    /// themselves against other dart-port tests.
    pub fn reset_post_cobject() {
        POST_COBJECT.store(0, Ordering::Release);
    }
}
