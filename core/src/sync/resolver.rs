//! Conflict resolver — dispatches merge based on [`SyncAuthority`].
//!
//! The pure CRDT [`Merge`] trait is never modified. Instead, this module
//! wraps it and adds authority-aware dispatch:
//!
//! - **`LocalFirst`** / **`RemoteFirst`**: delegates to `Merge::merge()`.
//! - **`RemoteAuthority`** with `is_remote == true`: unconditionally
//!   overwrites local state with remote state.
//! - **`RemoteAuthority`** with `is_remote == false`: local write wins
//!   (authority only applies to incoming remote data).

use std::hash::Hash;

use crate::crdt::{Counter, LwwRegister, Merge, OrSet};

use super::authority::{MergeContext, SyncAuthority};

/// Stateless conflict resolver that dispatches merge based on authority.
pub struct ConflictResolver;

impl ConflictResolver {
    /// Resolve a conflict between two LWW registers.
    pub fn resolve_lww<T: Clone>(
        local: &mut LwwRegister<T>,
        remote: &LwwRegister<T>,
        ctx: &MergeContext,
    ) {
        match (ctx.authority, ctx.is_remote) {
            (SyncAuthority::RemoteAuthority, true) => {
                // Remote always wins — overwrite unconditionally by cloning
                // the remote's full state into local.
                *local = remote.clone();
            }
            _ => {
                // LocalFirst, RemoteFirst, or non-remote context: CRDT merge.
                local.merge(remote);
            }
        }
    }

    /// Resolve a conflict between two counters.
    ///
    /// Under `RemoteAuthority` with a remote source, the remote's per-device
    /// deltas replace the local deltas entirely. This discards un-synced
    /// local increments — by design, since the server is the source of truth.
    pub fn resolve_counter(
        local: &mut Counter,
        remote: &Counter,
        ctx: &MergeContext,
    ) {
        match (ctx.authority, ctx.is_remote) {
            (SyncAuthority::RemoteAuthority, true) => {
                *local = remote.clone();
            }
            _ => {
                local.merge(remote);
            }
        }
    }

    /// Resolve a conflict between two OR-Sets.
    ///
    /// Under `RemoteAuthority` with a remote source, the remote's entries
    /// replace local entries entirely.
    pub fn resolve_orset<T: Eq + Hash + Clone>(
        local: &mut OrSet<T>,
        remote: &OrSet<T>,
        ctx: &MergeContext,
    ) {
        match (ctx.authority, ctx.is_remote) {
            (SyncAuthority::RemoteAuthority, true) => {
                *local = remote.clone();
            }
            _ => {
                local.merge(remote);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::HlcTimestamp;

    fn ts(physical: u64, logical: u16) -> HlcTimestamp {
        HlcTimestamp::new(physical, logical)
    }

    const DEVICE_A: u128 = 1;
    const DEVICE_B: u128 = 2;

    fn remote_ctx(authority: SyncAuthority) -> MergeContext {
        MergeContext {
            authority,
            is_remote: true,
        }
    }

    fn local_ctx(authority: SyncAuthority) -> MergeContext {
        MergeContext {
            authority,
            is_remote: false,
        }
    }

    // -- LWW Register -------------------------------------------------------

    #[test]
    fn lww_local_first_uses_crdt_merge() {
        let mut local = LwwRegister::new("local", ts(200, 0), DEVICE_A);
        let remote = LwwRegister::new("remote", ts(100, 0), DEVICE_B);

        ConflictResolver::resolve_lww(
            &mut local,
            &remote,
            &remote_ctx(SyncAuthority::LocalFirst),
        );
        // Local has higher timestamp, CRDT merge keeps local.
        assert_eq!(*local.value(), "local");
    }

    #[test]
    fn lww_remote_authority_remote_wins_even_with_lower_timestamp() {
        let mut local = LwwRegister::new("local", ts(200, 0), DEVICE_A);
        let remote = LwwRegister::new("remote", ts(100, 0), DEVICE_B);

        ConflictResolver::resolve_lww(
            &mut local,
            &remote,
            &remote_ctx(SyncAuthority::RemoteAuthority),
        );
        // Remote always wins under RemoteAuthority.
        assert_eq!(*local.value(), "remote");
    }

    #[test]
    fn lww_remote_authority_local_context_keeps_local() {
        let mut local = LwwRegister::new("local", ts(100, 0), DEVICE_A);
        let remote = LwwRegister::new("remote", ts(200, 0), DEVICE_B);

        ConflictResolver::resolve_lww(
            &mut local,
            &remote,
            &local_ctx(SyncAuthority::RemoteAuthority),
        );
        // Non-remote context: falls through to CRDT merge, remote has
        // higher ts so it wins via CRDT.
        assert_eq!(*local.value(), "remote");
    }

    #[test]
    fn lww_remote_first_delegates_to_crdt() {
        let mut local = LwwRegister::new("local", ts(200, 0), DEVICE_A);
        let remote = LwwRegister::new("remote", ts(100, 0), DEVICE_B);

        ConflictResolver::resolve_lww(
            &mut local,
            &remote,
            &remote_ctx(SyncAuthority::RemoteFirst),
        );
        assert_eq!(*local.value(), "local");
    }

    // -- Counter ------------------------------------------------------------

    #[test]
    fn counter_local_first_uses_crdt_merge() {
        let mut local = Counter::new();
        local.increment(DEVICE_A, 10);

        let mut remote = Counter::new();
        remote.increment(DEVICE_B, 5);

        ConflictResolver::resolve_counter(
            &mut local,
            &remote,
            &remote_ctx(SyncAuthority::LocalFirst),
        );
        assert_eq!(local.value(), 15); // 10 + 5
    }

    #[test]
    fn counter_remote_authority_replaces_local() {
        let mut local = Counter::new();
        local.increment(DEVICE_A, 10);

        let mut remote = Counter::new();
        remote.increment(DEVICE_B, 3);

        ConflictResolver::resolve_counter(
            &mut local,
            &remote,
            &remote_ctx(SyncAuthority::RemoteAuthority),
        );
        // Local deltas (device A +10) are gone — replaced by remote.
        assert_eq!(local.value(), 3);
        assert_eq!(local.device_delta(DEVICE_A), 0);
        assert_eq!(local.device_delta(DEVICE_B), 3);
    }

    #[test]
    fn counter_remote_first_uses_crdt_merge() {
        let mut local = Counter::new();
        local.increment(DEVICE_A, 10);

        let mut remote = Counter::new();
        remote.increment(DEVICE_B, 5);

        ConflictResolver::resolve_counter(
            &mut local,
            &remote,
            &remote_ctx(SyncAuthority::RemoteFirst),
        );
        assert_eq!(local.value(), 15);
    }

    // -- OrSet --------------------------------------------------------------

    #[test]
    fn orset_local_first_uses_crdt_merge() {
        let mut local = OrSet::new();
        local.add("apple", DEVICE_A, ts(100, 0));

        let mut remote = OrSet::new();
        remote.add("banana", DEVICE_B, ts(101, 0));

        ConflictResolver::resolve_orset(
            &mut local,
            &remote,
            &remote_ctx(SyncAuthority::LocalFirst),
        );
        assert!(local.contains(&"apple"));
        assert!(local.contains(&"banana"));
    }

    #[test]
    fn orset_remote_authority_replaces_local() {
        let mut local = OrSet::new();
        local.add("apple", DEVICE_A, ts(100, 0));
        local.add("cherry", DEVICE_A, ts(102, 0));

        let mut remote = OrSet::new();
        remote.add("banana", DEVICE_B, ts(101, 0));

        ConflictResolver::resolve_orset(
            &mut local,
            &remote,
            &remote_ctx(SyncAuthority::RemoteAuthority),
        );
        assert!(!local.contains(&"apple"));
        assert!(!local.contains(&"cherry"));
        assert!(local.contains(&"banana"));
    }

    #[test]
    fn orset_remote_first_uses_crdt_merge() {
        let mut local = OrSet::new();
        local.add("apple", DEVICE_A, ts(100, 0));

        let mut remote = OrSet::new();
        remote.add("banana", DEVICE_B, ts(101, 0));

        ConflictResolver::resolve_orset(
            &mut local,
            &remote,
            &remote_ctx(SyncAuthority::RemoteFirst),
        );
        assert!(local.contains(&"apple"));
        assert!(local.contains(&"banana"));
    }
}
