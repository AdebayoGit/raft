//! Live-handle registry — stale-handle and double-free protection.
//!
//! Every heap-allocated FFI handle (`RaftDb`, `RaftTransaction`,
//! `RaftQueryResult`) is registered here on creation and unregistered on
//! free. Entry points validate membership *before* dereferencing, so a
//! freed or garbage pointer produces [`RftError::InvalidHandle`] instead
//! of undefined behaviour, and concurrent double-free races are resolved
//! by the registry mutex (exactly one caller wins the unregister).
//!
//! Address reuse (a new handle allocated at a freed handle's address) can
//! in principle defeat the check, but in that case the pointer refers to
//! a *valid* handle of the same type, so no memory unsafety results.

use std::collections::BTreeSet;
use std::sync::Mutex;

/// A set of live handle addresses for one handle type.
pub(super) struct LiveSet(Mutex<BTreeSet<usize>>);

impl LiveSet {
    pub(super) const fn new() -> Self {
        Self(Mutex::new(BTreeSet::new()))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeSet<usize>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Record a freshly allocated handle as live.
    pub(super) fn register<T>(&self, ptr: *const T) {
        self.lock().insert(ptr as usize);
    }

    /// Returns `true` if the pointer refers to a live handle.
    pub(super) fn is_live<T>(&self, ptr: *const T) -> bool {
        self.lock().contains(&(ptr as usize))
    }

    /// Remove the handle, returning `true` if it was live. Exactly one
    /// concurrent caller wins, which makes free/close idempotent.
    pub(super) fn unregister<T>(&self, ptr: *const T) -> bool {
        self.lock().remove(&(ptr as usize))
    }
}

/// Live database handles.
pub(super) static LIVE_DBS: LiveSet = LiveSet::new();
/// Live transaction handles.
pub(super) static LIVE_TXNS: LiveSet = LiveSet::new();
/// Live query-result handles.
pub(super) static LIVE_QUERY_RESULTS: LiveSet = LiveSet::new();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_is_live_unregister_roundtrip() {
        let set = LiveSet::new();
        let x = 42u8;
        let p = &x as *const u8;

        assert!(!set.is_live(p));
        set.register(p);
        assert!(set.is_live(p));
        assert!(set.unregister(p));
        assert!(!set.is_live(p));
        // Second unregister loses the race.
        assert!(!set.unregister(p));
    }
}
