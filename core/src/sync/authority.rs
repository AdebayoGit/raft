//! Sync authority configuration — controls per-collection conflict resolution
//! strategy during synchronisation.
//!
//! The authority mode determines *who wins* when a local write and a remote
//! write conflict. It is set per-collection at schema definition time and
//! flows through the merge path.

use serde::{Deserialize, Serialize};

/// Controls how a collection resolves conflicts during sync.
///
/// Set per-collection via [`SchemaBuilder::sync_authority`]. Defaults to
/// [`LocalFirst`](SyncAuthority::LocalFirst).
///
/// # Modes
///
/// | Mode | Merge behaviour | Read behaviour |
/// |---|---|---|
/// | `LocalFirst` | CRDT merge — deterministic, commutative | Local store |
/// | `RemoteAuthority` | Remote always overwrites local on conflict | Local store |
/// | `RemoteFirst` | CRDT merge (same as `LocalFirst`) | Network if connected, local fallback (future) |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum SyncAuthority {
    /// The local device is the authority. Conflicts are resolved by CRDT
    /// merge semantics (timestamp + device_id ordering). This is the default
    /// and preserves all mathematical CRDT guarantees (commutativity,
    /// associativity, idempotency).
    #[default]
    LocalFirst,

    /// The remote (server) is the authority. On sync, the remote state
    /// unconditionally overwrites local state for conflicting fields,
    /// regardless of timestamps.
    ///
    /// **Warning**: un-synced local writes to fields governed by this mode
    /// will be discarded when a remote update arrives. Use this for
    /// server-managed data such as feature flags, admin settings, or
    /// pricing that should never be overridden by client writes.
    RemoteAuthority,

    /// The remote is preferred for reads when connectivity is available.
    /// On sync, conflicts are still resolved via CRDT merge (same as
    /// `LocalFirst`). The difference is on the *read path*: if the device
    /// is online, it pulls the freshest value from the remote before
    /// returning.
    ///
    /// **Note**: the read-through behaviour requires the sync engine
    /// (Phase 5). Until then, `RemoteFirst` behaves identically to
    /// `LocalFirst` at the merge level.
    RemoteFirst,
}

impl std::fmt::Display for SyncAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncAuthority::LocalFirst => write!(f, "LocalFirst"),
            SyncAuthority::RemoteAuthority => write!(f, "RemoteAuthority"),
            SyncAuthority::RemoteFirst => write!(f, "RemoteFirst"),
        }
    }
}

/// Contextual information passed to the conflict resolver during a sync
/// merge operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeContext {
    /// The authority mode configured for this collection.
    pub authority: SyncAuthority,
    /// Whether the incoming value originated from a remote device.
    pub is_remote: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_local_first() {
        assert_eq!(SyncAuthority::default(), SyncAuthority::LocalFirst);
    }

    #[test]
    fn display_variants() {
        assert_eq!(SyncAuthority::LocalFirst.to_string(), "LocalFirst");
        assert_eq!(SyncAuthority::RemoteAuthority.to_string(), "RemoteAuthority");
        assert_eq!(SyncAuthority::RemoteFirst.to_string(), "RemoteFirst");
    }

    #[test]
    fn serde_round_trip() {
        for variant in [
            SyncAuthority::LocalFirst,
            SyncAuthority::RemoteAuthority,
            SyncAuthority::RemoteFirst,
        ] {
            let json = serde_json::to_string(&variant).expect("serialize");
            let decoded: SyncAuthority = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(variant, decoded);
        }
    }

    #[test]
    fn merge_context_construction() {
        let ctx = MergeContext {
            authority: SyncAuthority::RemoteAuthority,
            is_remote: true,
        };
        assert_eq!(ctx.authority, SyncAuthority::RemoteAuthority);
        assert!(ctx.is_remote);
    }
}
