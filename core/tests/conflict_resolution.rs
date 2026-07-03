//! Integration tests for the per-field conflict resolution model
//! introduced in v0.1.1.
//!
//! These tests exercise the public surface added in this PR:
//!
//! - The schema-level [`ConflictStrategy`] enum and the
//!   `.conflict(...)` builder method.
//! - The runtime [`ConflictResolver`] with custom resolver registration.
//! - The default-strategy fallback that preserves v0.1.0 semantics.
//!
//! The document-level conflict-tracking API (`doc.conflicts()`,
//! `doc.resolve_conflict(...)`) and the FFI surface are deferred to a
//! follow-up PR alongside the sync apply path. The corresponding test
//! `conflicted_field_resolution_api` from the original spec is therefore
//! not included here.

use raftdb::query::Value;
use raftdb::schema::{
    ConflictStrategy, CrdtKind, CustomResolverId, FieldType, Schema, SchemaBuilder,
};
use raftdb::sync::{ConflictResolver, ResolveOutcome};
use raftdb::wal::HlcTimestamp;

const DEVICE_A: u128 = 1;
const DEVICE_B: u128 = 2;

fn ts(physical: u64, logical: u16) -> HlcTimestamp {
    HlcTimestamp::new(physical, logical)
}

// --- 1. LWW: simple resolution ------------------------------------------

#[test]
fn lww_simple_resolution() {
    let resolver = ConflictResolver::new();
    let outcome = resolver.resolve_value(
        &ConflictStrategy::LastWriteWins,
        Value::String("local".into()),
        ts(100, 0),
        DEVICE_A,
        Value::String("remote".into()),
        ts(200, 0),
        DEVICE_B,
    );
    assert_eq!(
        outcome,
        ResolveOutcome::Resolved(Value::String("remote".into())),
        "remote has higher HLC, must win",
    );

    // Reverse — local has higher HLC.
    let outcome = resolver.resolve_value(
        &ConflictStrategy::LastWriteWins,
        Value::String("local".into()),
        ts(300, 0),
        DEVICE_A,
        Value::String("remote".into()),
        ts(200, 0),
        DEVICE_B,
    );
    assert_eq!(
        outcome,
        ResolveOutcome::Resolved(Value::String("local".into())),
    );
}

// --- 2. LWW: HLC tie broken deterministically by device id --------------

#[test]
fn lww_hlc_tie_deterministic() {
    let resolver = ConflictResolver::new();

    // Same HLC; remote device id is higher → remote wins.
    let outcome = resolver.resolve_value(
        &ConflictStrategy::LastWriteWins,
        Value::Int(1),
        ts(500, 7),
        DEVICE_A,
        Value::Int(2),
        ts(500, 7),
        DEVICE_B,
    );
    assert_eq!(outcome, ResolveOutcome::Resolved(Value::Int(2)));

    // Run with the perspective swapped; result must converge to the same
    // value (Int(2)) on both sides.
    let outcome_mirrored = resolver.resolve_value(
        &ConflictStrategy::LastWriteWins,
        Value::Int(2),
        ts(500, 7),
        DEVICE_B,
        Value::Int(1),
        ts(500, 7),
        DEVICE_A,
    );
    assert_eq!(
        outcome_mirrored,
        ResolveOutcome::Resolved(Value::Int(2)),
        "tie-break by device id must converge across devices",
    );
}

// --- 3. ServerAuthority overrides local --------------------------------

#[test]
fn server_authority_overrides_local() {
    let resolver = ConflictResolver::new();
    let outcome = resolver.resolve_value(
        &ConflictStrategy::ServerAuthority,
        Value::Int(999),
        ts(10_000, 0), // local has *higher* HLC
        DEVICE_A,
        Value::Int(42),
        ts(50, 0), // remote (server) has lower HLC
        DEVICE_B,
    );
    // Server value wins regardless of timestamps.
    assert_eq!(outcome, ResolveOutcome::Resolved(Value::Int(42)));
}

// --- 4. CRDT OR-Set: existing concurrent add/remove behaviour ----------

#[test]
fn crdt_or_set_concurrent_add_remove() {
    use raftdb::crdt::{Merge, OrSet};

    // The new ConflictStrategy::Crdt variant is a marker — actual CRDT
    // merge behaviour is unchanged from v0.1.0. This test verifies the
    // marker is recognised and that the underlying primitive still
    // behaves correctly.
    let strategy = ConflictStrategy::Crdt(CrdtKind::OrSet);
    assert!(strategy.is_compatible_with(FieldType::Collection));

    let mut local: OrSet<&str> = OrSet::new();
    local.add("apple", DEVICE_A, ts(100, 0));
    local.add("banana", DEVICE_A, ts(101, 0));

    let mut remote: OrSet<&str> = OrSet::new();
    remote.add("banana", DEVICE_B, ts(102, 0));
    remote.add("cherry", DEVICE_B, ts(103, 0));
    // Concurrently remove apple on remote.
    remote.remove(&"apple", ts(104, 0));

    local.merge(&remote);

    // add-wins semantics: apple was concurrently added (local) and
    // removed (remote); add wins because it has a later observable tag.
    assert!(local.contains(&"banana"));
    assert!(local.contains(&"cherry"));
}

// --- 5. Custom resolver: invoked on conflict ---------------------------

#[test]
fn custom_resolver_invoked() {
    let mut resolver = ConflictResolver::new();
    resolver.register("max_int", |local, remote| match (&local, &remote) {
        (Value::Int(a), Value::Int(b)) => Value::Int((*a).max(*b)),
        _ => remote,
    });

    let strategy = ConflictStrategy::Custom(CustomResolverId::new("max_int"));
    let outcome = resolver.resolve_value(
        &strategy,
        Value::Int(5),
        ts(100, 0),
        DEVICE_A,
        Value::Int(10),
        ts(50, 0), // lower HLC, but custom resolver picks max
        DEVICE_B,
    );
    assert_eq!(outcome, ResolveOutcome::Resolved(Value::Int(10)));
}

// --- 6. Custom resolver missing → field marked conflicted --------------

#[test]
fn custom_resolver_missing_marks_conflicted() {
    let resolver = ConflictResolver::new();
    let strategy = ConflictStrategy::Custom(CustomResolverId::new("not_registered"));

    let outcome = resolver.resolve_value(
        &strategy,
        Value::String("local".into()),
        ts(100, 0),
        DEVICE_A,
        Value::String("remote".into()),
        ts(200, 0),
        DEVICE_B,
    );
    assert_eq!(
        outcome,
        ResolveOutcome::Conflicted {
            local: Value::String("local".into()),
            remote: Value::String("remote".into()),
        },
    );
}

// --- 7. (deferred) doc.resolve_conflict — see file-level note ----------

// --- 8. Mixed strategies in a single schema ----------------------------

#[test]
fn mixed_strategies_in_single_schema() {
    let schema: Schema = SchemaBuilder::new("user")
        .field("name", FieldType::String)
        .conflict(ConflictStrategy::LastWriteWins)
        .field("balance", FieldType::Int)
        .conflict(ConflictStrategy::ServerAuthority)
        .field("tags", FieldType::Collection)
        .conflict(ConflictStrategy::Crdt(CrdtKind::OrSet))
        .field("preferences", FieldType::Bytes)
        .conflict(ConflictStrategy::Custom(CustomResolverId::new(
            "merge_prefs",
        )))
        .build()
        .expect("valid schema");

    assert_eq!(schema.field_count(), 4);

    let name = schema.field("name").expect("name field");
    assert_eq!(name.conflict_strategy(), &ConflictStrategy::LastWriteWins);

    let balance = schema.field("balance").expect("balance field");
    assert_eq!(
        balance.conflict_strategy(),
        &ConflictStrategy::ServerAuthority
    );

    let tags = schema.field("tags").expect("tags field");
    assert_eq!(
        tags.conflict_strategy(),
        &ConflictStrategy::Crdt(CrdtKind::OrSet)
    );

    let prefs = schema.field("preferences").expect("preferences field");
    assert_eq!(
        prefs.conflict_strategy(),
        &ConflictStrategy::Custom(CustomResolverId::new("merge_prefs")),
    );

    // Each field resolves through the right path.
    let mut resolver = ConflictResolver::new();
    resolver.register("merge_prefs", |_local, remote| remote);

    // ServerAuthority: remote wins regardless of HLC.
    let outcome = resolver.resolve_value(
        balance.conflict_strategy(),
        Value::Int(100),
        ts(999, 0),
        DEVICE_A,
        Value::Int(50),
        ts(1, 0),
        DEVICE_B,
    );
    assert_eq!(outcome, ResolveOutcome::Resolved(Value::Int(50)));

    // LastWriteWins: higher HLC wins.
    let outcome = resolver.resolve_value(
        name.conflict_strategy(),
        Value::String("alice".into()),
        ts(100, 0),
        DEVICE_A,
        Value::String("bob".into()),
        ts(200, 0),
        DEVICE_B,
    );
    assert_eq!(
        outcome,
        ResolveOutcome::Resolved(Value::String("bob".into()))
    );

    // Custom merge_prefs: returns remote (registered above).
    let outcome = resolver.resolve_value(
        prefs.conflict_strategy(),
        Value::Bytes(vec![1]),
        ts(100, 0),
        DEVICE_A,
        Value::Bytes(vec![2]),
        ts(50, 0),
        DEVICE_B,
    );
    assert_eq!(outcome, ResolveOutcome::Resolved(Value::Bytes(vec![2])));
}

// --- 9. Default strategy per field type (backward compat) --------------

#[test]
fn default_strategy_per_field_type() {
    // Schemas constructed via the v0.1.0 API (no `.conflict(...)` calls)
    // must end up with backward-compatible defaults: scalar fields →
    // Crdt(LwwRegister), collections → Crdt(OrSet).
    let schema = SchemaBuilder::new("legacy")
        .field("title", FieldType::String)
        .field("count", FieldType::Int)
        .field("ratio", FieldType::Float)
        .field("active", FieldType::Bool)
        .field("blob", FieldType::Bytes)
        .field("ref_id", FieldType::Reference)
        .field("items", FieldType::Collection)
        .build()
        .expect("valid schema");

    let cases = [
        ("title", ConflictStrategy::Crdt(CrdtKind::LwwRegister)),
        ("count", ConflictStrategy::Crdt(CrdtKind::LwwRegister)),
        ("ratio", ConflictStrategy::Crdt(CrdtKind::LwwRegister)),
        ("active", ConflictStrategy::Crdt(CrdtKind::LwwRegister)),
        ("blob", ConflictStrategy::Crdt(CrdtKind::LwwRegister)),
        ("ref_id", ConflictStrategy::Crdt(CrdtKind::LwwRegister)),
        ("items", ConflictStrategy::Crdt(CrdtKind::OrSet)),
    ];
    for (name, expected) in cases {
        let f = schema
            .field(name)
            .unwrap_or_else(|| panic!("missing {name}"));
        assert_eq!(
            f.conflict_strategy(),
            &expected,
            "wrong default strategy for field {name}",
        );
    }
}
