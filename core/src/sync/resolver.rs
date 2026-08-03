//! Conflict resolver — dispatches merge based on [`SyncAuthority`] and
//! per-field [`ConflictStrategy`].
//!
//! There are two layers of API on [`ConflictResolver`]:
//!
//! 1. **CRDT-level static methods** (`resolve_lww`, `resolve_counter`,
//!    `resolve_orset`) — unchanged from v0.1.0. Operate on CRDT primitives
//!    using the per-collection [`SyncAuthority`].
//! 2. **Value-level instance methods** (`resolve_value`, `register`) —
//!    new in v0.1.1. Operate on dynamically-typed [`Value`]s using the
//!    per-field [`ConflictStrategy`]. The per-field strategy *overrides*
//!    the per-collection [`SyncAuthority`].
//!
//! The pure CRDT [`Merge`] trait is never modified.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use crate::crdt::{Counter, LwwRegister, Merge, OrSet};
use crate::query::Value;
use crate::schema::{ConflictStrategy, CustomResolverId};
use crate::wal::HlcTimestamp;

use super::authority::{MergeContext, SyncAuthority};

/// Outcome of resolving a value-level conflict.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolveOutcome {
    /// Conflict resolved automatically — apply this value.
    Resolved(Value),
    /// No automatic resolution available (e.g. a `Custom` strategy whose
    /// resolver is not registered). Both versions are returned so the
    /// caller can surface the conflict to the application.
    Conflicted { local: Value, remote: Value },
}

/// A function-like object that merges two field values into one.
///
/// Implemented for any `Fn(Value, Value) -> Value + Send + Sync` via the
/// blanket impl below, so callers can register a closure directly.
pub trait CustomResolverFn: Send + Sync {
    fn merge(&self, local: Value, remote: Value) -> Value;
}

impl<F> CustomResolverFn for F
where
    F: Fn(Value, Value) -> Value + Send + Sync,
{
    fn merge(&self, local: Value, remote: Value) -> Value {
        (self)(local, remote)
    }
}

/// Conflict resolver.
///
/// Stateless for the CRDT-level static methods; carries a registry of
/// custom merge functions for the value-level API.
#[derive(Default, Clone)]
pub struct ConflictResolver {
    custom_resolvers: HashMap<CustomResolverId, Arc<dyn CustomResolverFn>>,
}

impl std::fmt::Debug for ConflictResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConflictResolver")
            .field(
                "custom_resolvers",
                &self.custom_resolvers.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl ConflictResolver {
    /// Construct a resolver with no registered custom merge functions.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a custom merge function under the given id. Replaces any
    /// previously registered resolver with the same id.
    ///
    /// The id is the [`CustomResolverId`] referenced by
    /// [`ConflictStrategy::Custom`] in the schema. Resolvers can be
    /// registered after the schema is loaded — fields whose resolver is
    /// not yet registered surface as [`ResolveOutcome::Conflicted`].
    pub fn register<F>(&mut self, id: impl Into<CustomResolverId>, resolver: F)
    where
        F: Fn(Value, Value) -> Value + Send + Sync + 'static,
    {
        self.custom_resolvers.insert(id.into(), Arc::new(resolver));
    }

    /// Returns `true` if a resolver is registered under the given id.
    pub fn has_resolver(&self, id: &CustomResolverId) -> bool {
        self.custom_resolvers.contains_key(id)
    }

    /// Resolve a value-level conflict according to the given per-field
    /// [`ConflictStrategy`].
    ///
    /// - `LastWriteWins`: higher HLC wins; ties broken by higher
    ///   `device_id` for cross-device determinism.
    /// - `ServerAuthority`: remote always wins, regardless of HLC.
    /// - `Custom(id)`: invokes the registered merge function. If no
    ///   resolver is registered, returns [`ResolveOutcome::Conflicted`].
    /// - `Crdt(_)`: not handled at the value level — CRDT fields keep
    ///   their per-device metadata and merge through the typed methods
    ///   (`resolve_lww` / `resolve_counter` / `resolve_orset`). For a
    ///   `Crdt` strategy this method falls back to LWW-by-HLC semantics
    ///   so callers operating on a flattened `Value` still get a
    ///   sensible deterministic result.
    #[allow(clippy::too_many_arguments)]
    pub fn resolve_value(
        &self,
        strategy: &ConflictStrategy,
        local: Value,
        local_hlc: HlcTimestamp,
        local_device: u128,
        remote: Value,
        remote_hlc: HlcTimestamp,
        remote_device: u128,
    ) -> ResolveOutcome {
        match strategy {
            ConflictStrategy::LastWriteWins | ConflictStrategy::Crdt(_) => {
                ResolveOutcome::Resolved(lww_pick(
                    local,
                    local_hlc,
                    local_device,
                    remote,
                    remote_hlc,
                    remote_device,
                ))
            }
            ConflictStrategy::ServerAuthority => {
                // The server (remote) is authoritative — its value wins
                // regardless of timestamps.
                let _ = (local, local_hlc, local_device, remote_hlc, remote_device);
                ResolveOutcome::Resolved(remote)
            }
            ConflictStrategy::Custom(id) => match self.custom_resolvers.get(id) {
                Some(resolver) => ResolveOutcome::Resolved(resolver.merge(local, remote)),
                None => ResolveOutcome::Conflicted { local, remote },
            },
        }
    }

    /// Resolve a field-level conflict, combining the per-collection
    /// [`SyncAuthority`] (via [`MergeContext`]) with the field's
    /// [`ConflictStrategy`].
    ///
    /// Precedence: an *explicit* per-field strategy (`LastWriteWins`,
    /// `ServerAuthority`, `Custom`) always overrides the collection
    /// authority. The default `Crdt(_)` strategy defers to the collection
    /// authority, which is where `RemoteAuthority` takes effect.
    ///
    /// # Interaction matrix
    ///
    /// | Authority × Strategy | `Crdt(_)` (default) | `LastWriteWins` | `ServerAuthority` | `Custom` |
    /// |---|---|---|---|---|
    /// | `LocalFirst` | CRDT merge (LWW at value level) | higher HLC wins | remote wins | resolver fn, else `Conflicted` |
    /// | `RemoteAuthority`, remote origin | **remote wins** | higher HLC wins | remote wins | resolver fn, else `Conflicted` |
    /// | `RemoteAuthority`, local origin | CRDT merge | higher HLC wins | remote wins | resolver fn, else `Conflicted` |
    /// | `RemoteFirst` | CRDT merge | higher HLC wins | remote wins | resolver fn, else `Conflicted` |
    ///
    /// Notes:
    /// - `RemoteAuthority` only overrides the default `Crdt` strategy and
    ///   only for values that actually originated from the remote
    ///   (`ctx.is_remote`). Local-origin merges keep CRDT semantics so
    ///   offline writes still converge deterministically.
    /// - `RemoteFirst` differs from `LocalFirst` on the *read path* only
    ///   (see [`SyncAuthority::RemoteFirst`]); its merge behaviour is
    ///   identical.
    /// - HLC ties break on higher `device_id` so all replicas converge.
    #[allow(clippy::too_many_arguments)]
    pub fn resolve_field(
        &self,
        ctx: &MergeContext,
        strategy: &ConflictStrategy,
        local: Value,
        local_hlc: HlcTimestamp,
        local_device: u128,
        remote: Value,
        remote_hlc: HlcTimestamp,
        remote_device: u128,
    ) -> ResolveOutcome {
        if matches!(strategy, ConflictStrategy::Crdt(_))
            && ctx.authority == SyncAuthority::RemoteAuthority
            && ctx.is_remote
        {
            // Default-strategy field in a remote-authoritative collection:
            // the remote value is enforced regardless of timestamps.
            return ResolveOutcome::Resolved(remote);
        }
        self.resolve_value(
            strategy,
            local,
            local_hlc,
            local_device,
            remote,
            remote_hlc,
            remote_device,
        )
    }

    // -- existing CRDT-level dispatch (v0.1.0 API, unchanged) -----------

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
    pub fn resolve_counter(local: &mut Counter, remote: &Counter, ctx: &MergeContext) {
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

/// Pick between two values by HLC, breaking ties on device id so all
/// replicas converge to the same winner.
fn lww_pick(
    local: Value,
    local_hlc: HlcTimestamp,
    local_device: u128,
    remote: Value,
    remote_hlc: HlcTimestamp,
    remote_device: u128,
) -> Value {
    use std::cmp::Ordering;
    match remote_hlc.cmp(&local_hlc) {
        Ordering::Greater => remote,
        Ordering::Less => local,
        Ordering::Equal => {
            if remote_device > local_device {
                remote
            } else {
                local
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

        ConflictResolver::resolve_lww(&mut local, &remote, &remote_ctx(SyncAuthority::LocalFirst));
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

        ConflictResolver::resolve_lww(&mut local, &remote, &remote_ctx(SyncAuthority::RemoteFirst));
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
        assert_eq!(local.value(), Ok(15)); // 10 + 5
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
        assert_eq!(local.value(), Ok(3));
        assert_eq!(local.device_delta(DEVICE_A), Ok(0));
        assert_eq!(local.device_delta(DEVICE_B), Ok(3));
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
        assert_eq!(local.value(), Ok(15));
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

    // -- Value-level resolver (per-field ConflictStrategy) ---------------

    use crate::query::Value;
    use crate::schema::{ConflictStrategy, CrdtKind, CustomResolverId};

    #[test]
    fn value_lww_picks_higher_hlc() {
        let r = ConflictResolver::new();
        let out = r.resolve_value(
            &ConflictStrategy::LastWriteWins,
            Value::Int(1),
            ts(100, 0),
            DEVICE_A,
            Value::Int(2),
            ts(200, 0),
            DEVICE_B,
        );
        assert_eq!(out, ResolveOutcome::Resolved(Value::Int(2)));
    }

    #[test]
    fn value_lww_tie_breaks_on_higher_device_id() {
        let r = ConflictResolver::new();
        let out = r.resolve_value(
            &ConflictStrategy::LastWriteWins,
            Value::Int(1),
            ts(100, 0),
            DEVICE_A,
            Value::Int(2),
            ts(100, 0),
            DEVICE_B,
        );
        assert_eq!(out, ResolveOutcome::Resolved(Value::Int(2)));
    }

    #[test]
    fn value_server_authority_returns_remote() {
        let r = ConflictResolver::new();
        let out = r.resolve_value(
            &ConflictStrategy::ServerAuthority,
            Value::Int(99),
            ts(9999, 0), // local has higher HLC, doesn't matter
            DEVICE_A,
            Value::Int(1),
            ts(1, 0),
            DEVICE_B,
        );
        assert_eq!(out, ResolveOutcome::Resolved(Value::Int(1)));
    }

    #[test]
    fn value_custom_resolver_invoked_when_registered() {
        let mut r = ConflictResolver::new();
        r.register("max_int", |a, b| match (&a, &b) {
            (Value::Int(x), Value::Int(y)) => Value::Int((*x).max(*y)),
            _ => b,
        });
        let strategy = ConflictStrategy::Custom(CustomResolverId::new("max_int"));
        let out = r.resolve_value(
            &strategy,
            Value::Int(7),
            ts(100, 0),
            DEVICE_A,
            Value::Int(3),
            ts(200, 0),
            DEVICE_B,
        );
        assert_eq!(out, ResolveOutcome::Resolved(Value::Int(7)));
    }

    #[test]
    fn value_custom_resolver_missing_yields_conflicted() {
        let r = ConflictResolver::new();
        let strategy = ConflictStrategy::Custom(CustomResolverId::new("missing"));
        let out = r.resolve_value(
            &strategy,
            Value::String("a".into()),
            ts(100, 0),
            DEVICE_A,
            Value::String("b".into()),
            ts(200, 0),
            DEVICE_B,
        );
        assert_eq!(
            out,
            ResolveOutcome::Conflicted {
                local: Value::String("a".into()),
                remote: Value::String("b".into()),
            }
        );
    }

    #[test]
    fn value_crdt_strategy_falls_back_to_lww_at_value_level() {
        // Crdt fields normally merge through the typed CRDT methods.
        // When called via resolve_value (e.g. on a flattened document
        // representation) we still need a deterministic answer — pick
        // by HLC.
        let r = ConflictResolver::new();
        let out = r.resolve_value(
            &ConflictStrategy::Crdt(CrdtKind::LwwRegister),
            Value::String("local".into()),
            ts(100, 0),
            DEVICE_A,
            Value::String("remote".into()),
            ts(200, 0),
            DEVICE_B,
        );
        assert_eq!(
            out,
            ResolveOutcome::Resolved(Value::String("remote".into()))
        );
    }

    #[test]
    fn register_replaces_previous_resolver() {
        let mut r = ConflictResolver::new();
        r.register("k", |_, _| Value::Int(1));
        r.register("k", |_, _| Value::Int(2));
        assert!(r.has_resolver(&CustomResolverId::new("k")));
        let out = r.resolve_value(
            &ConflictStrategy::Custom(CustomResolverId::new("k")),
            Value::Int(0),
            ts(0, 0),
            DEVICE_A,
            Value::Int(0),
            ts(0, 0),
            DEVICE_B,
        );
        assert_eq!(out, ResolveOutcome::Resolved(Value::Int(2)));
    }

    // -- Authority × Strategy interaction matrix --------------------------

    /// Table-driven coverage of every (SyncAuthority × ConflictStrategy)
    /// combination, per the matrix documented on `resolve_field`.
    ///
    /// Fixture: local = Int(1) at ts 200 (device A, *newer*),
    ///          remote = Int(2) at ts 100 (device B, *older*).
    /// A newer local timestamp makes it observable whether a strategy
    /// respects HLC ordering (local wins) or enforces the remote.
    #[test]
    fn authority_strategy_interaction_matrix() {
        #[derive(Debug, Clone, Copy, PartialEq)]
        enum Expect {
            LocalWins,  // HLC-ordered merge — newer local survives
            RemoteWins, // remote enforced regardless of HLC
            Custom,     // registered resolver output (max = local's 7)
            Conflicted, // unregistered custom resolver
        }

        let mut resolver = ConflictResolver::new();
        resolver.register("max_int", |a, b| match (&a, &b) {
            (Value::Int(x), Value::Int(y)) => Value::Int((*x).max(*y)),
            _ => b,
        });

        let crdt = ConflictStrategy::Crdt(CrdtKind::LwwRegister);
        let lww = ConflictStrategy::LastWriteWins;
        let server = ConflictStrategy::ServerAuthority;
        let custom = ConflictStrategy::Custom(CustomResolverId::new("max_int"));
        let missing = ConflictStrategy::Custom(CustomResolverId::new("missing"));

        use SyncAuthority::*;
        // (authority, is_remote, strategy, expected)
        let cases: &[(SyncAuthority, bool, &ConflictStrategy, Expect)] = &[
            // LocalFirst — pure CRDT semantics; explicit strategies apply.
            (LocalFirst, true, &crdt, Expect::LocalWins),
            (LocalFirst, true, &lww, Expect::LocalWins),
            (LocalFirst, true, &server, Expect::RemoteWins),
            (LocalFirst, true, &custom, Expect::Custom),
            (LocalFirst, true, &missing, Expect::Conflicted),
            // RemoteAuthority, remote origin — overrides the default Crdt
            // strategy only; explicit per-field strategies win.
            (RemoteAuthority, true, &crdt, Expect::RemoteWins),
            (RemoteAuthority, true, &lww, Expect::LocalWins),
            (RemoteAuthority, true, &server, Expect::RemoteWins),
            (RemoteAuthority, true, &custom, Expect::Custom),
            (RemoteAuthority, true, &missing, Expect::Conflicted),
            // RemoteAuthority, local origin — no remote involved, CRDT
            // semantics preserved so offline writes converge.
            (RemoteAuthority, false, &crdt, Expect::LocalWins),
            (RemoteAuthority, false, &lww, Expect::LocalWins),
            (RemoteAuthority, false, &server, Expect::RemoteWins),
            (RemoteAuthority, false, &custom, Expect::Custom),
            (RemoteAuthority, false, &missing, Expect::Conflicted),
            // RemoteFirst — merge behaviour identical to LocalFirst.
            (RemoteFirst, true, &crdt, Expect::LocalWins),
            (RemoteFirst, true, &lww, Expect::LocalWins),
            (RemoteFirst, true, &server, Expect::RemoteWins),
            (RemoteFirst, true, &custom, Expect::Custom),
            (RemoteFirst, true, &missing, Expect::Conflicted),
        ];

        for (authority, is_remote, strategy, expected) in cases {
            let ctx = MergeContext {
                authority: *authority,
                is_remote: *is_remote,
            };
            let out = resolver.resolve_field(
                &ctx,
                strategy,
                Value::Int(7), // local, newer
                ts(200, 0),
                DEVICE_A,
                Value::Int(2), // remote, older
                ts(100, 0),
                DEVICE_B,
            );
            let actual = match &out {
                ResolveOutcome::Resolved(Value::Int(7)) => {
                    // max_int(7, 2) == 7 == local value; disambiguate via
                    // strategy: Custom means the resolver ran.
                    if matches!(strategy, ConflictStrategy::Custom(_)) {
                        Expect::Custom
                    } else {
                        Expect::LocalWins
                    }
                }
                ResolveOutcome::Resolved(Value::Int(2)) => Expect::RemoteWins,
                ResolveOutcome::Conflicted { .. } => Expect::Conflicted,
                other => panic!("unexpected outcome {other:?}"),
            };
            assert_eq!(
                actual, *expected,
                "authority={authority:?} is_remote={is_remote} strategy={strategy:?} → {out:?}"
            );
        }
    }

    #[test]
    fn matrix_hlc_tie_breaks_on_device_id_across_authorities() {
        // Equal HLCs: the higher device id must win under every authority
        // for LWW-ordered strategies, so replicas converge.
        let resolver = ConflictResolver::new();
        for authority in [
            SyncAuthority::LocalFirst,
            SyncAuthority::RemoteAuthority,
            SyncAuthority::RemoteFirst,
        ] {
            let ctx = MergeContext {
                authority,
                is_remote: false,
            };
            let out = resolver.resolve_field(
                &ctx,
                &ConflictStrategy::LastWriteWins,
                Value::Int(1),
                ts(100, 0),
                DEVICE_A,
                Value::Int(2),
                ts(100, 0),
                DEVICE_B, // higher device id
            );
            assert_eq!(
                out,
                ResolveOutcome::Resolved(Value::Int(2)),
                "tie-break failed under {authority:?}"
            );
        }
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
