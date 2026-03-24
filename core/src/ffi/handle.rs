//! Opaque database handle for FFI.
//!
//! To C callers this is `*mut RaftDb` — a pointer to an opaque type.
//! Internally it wraps the Rust [`StorageEngine`].

use crate::StorageEngine;

/// Opaque handle wrapping a [`StorageEngine`].
///
/// Allocated on the heap by [`rft_open`](super::rft_open) and freed by
/// [`rft_close`](super::rft_close). C callers treat it as `*mut c_void`.
pub struct RaftDb {
    engine: StorageEngine,
}

impl RaftDb {
    pub(super) fn new(engine: StorageEngine) -> Self {
        Self { engine }
    }

    pub(super) fn engine(&self) -> &StorageEngine {
        &self.engine
    }

    pub(super) fn engine_mut(&mut self) -> &mut StorageEngine {
        &mut self.engine
    }
}
