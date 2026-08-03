//! Live-handle registry — stale-handle and double-free protection.
//!
//! Every FFI handle is registered here on creation and unregistered on
//! free. Transaction handles are monotonic opaque tokens; the other handles
//! are heap allocated. Entry points validate membership *before* dereferencing, so a
//! freed or garbage pointer produces [`RftError::InvalidHandle`] instead
//! of undefined behaviour, and concurrent double-free races are resolved
//! by the registry mutex (exactly one caller wins the unregister).
//!
//! Address reuse (a new handle allocated at a freed handle's address) can
//! in principle defeat the check, but in that case the pointer refers to
//! a *valid* handle of the same type, so no memory unsafety results.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use crate::database::DbTransaction;

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
/// Live query-result handles.
pub(super) static LIVE_QUERY_RESULTS: LiveSet = LiveSet::new();
/// Live collection handles.
pub(super) static LIVE_COLLS: LiveSet = LiveSet::new();

pub(super) type SharedTransaction = Arc<Mutex<Option<DbTransaction>>>;

/// Transaction handles are tokens only. The registry owns the state so
/// operations never construct references from a pointer that another thread
/// can concurrently free.
pub(super) struct TransactionRegistry(Mutex<BTreeMap<usize, SharedTransaction>>);

impl TransactionRegistry {
    pub(super) const fn new() -> Self {
        Self(Mutex::new(BTreeMap::new()))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<usize, SharedTransaction>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(super) fn register<T>(&self, ptr: *const T, transaction: DbTransaction) {
        self.lock()
            .insert(ptr as usize, Arc::new(Mutex::new(Some(transaction))));
    }

    pub(super) fn get<T>(&self, ptr: *const T) -> Option<SharedTransaction> {
        self.lock().get(&(ptr as usize)).cloned()
    }

    /// Atomically prevents new operations from acquiring this transaction.
    pub(super) fn remove<T>(&self, ptr: *const T) -> Option<SharedTransaction> {
        self.lock().remove(&(ptr as usize))
    }
}

pub(super) static LIVE_TXNS: TransactionRegistry = TransactionRegistry::new();

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
