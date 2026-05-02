//! Opaque database handle for FFI.
//!
//! To C callers this is `*mut RaftDb` — a pointer to an opaque type.
//! Internally it wraps the high-level [`Database`] runtime, which itself
//! sits on top of the [`StorageEngine`]. The runtime owns a tokio runtime
//! used by observer callbacks (see [`super::observe`]).

use std::sync::Mutex;

use crate::database::Database;

/// Opaque handle wrapping a [`Database`] and its async runtime.
///
/// Allocated on the heap by [`rft_open`](super::rft_open) and freed by
/// [`rft_close`](super::rft_close). C callers treat it as `*mut c_void`.
pub struct RaftDb {
    db: Database,
    /// Tokio runtime that backs observer subscription tasks.
    rt: tokio::runtime::Runtime,
    /// Subscription registry: `subscription_id → AbortHandle`. Used by
    /// [`super::observe::rft_unobserve`] to cancel a live observer.
    pub(super) subscriptions: Mutex<crate::ffi::observe::SubscriptionRegistry>,
}

impl RaftDb {
    pub(super) fn new(db: Database) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("failed to start observer runtime");
        Self {
            db,
            rt,
            subscriptions: Mutex::new(Default::default()),
        }
    }

    pub(super) fn database(&self) -> &Database {
        &self.db
    }

    pub(super) fn runtime(&self) -> &tokio::runtime::Runtime {
        &self.rt
    }
}
