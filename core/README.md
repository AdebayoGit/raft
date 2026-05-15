# raft-db

The Rust core of Raft — a mobile-native, offline-first embedded database. See the [repository README](../README.md) for the project overview.

This document covers Rust-API details that don't need to live in the project README.

## Conflict Resolution

Raft fields can resolve concurrent writes in one of four ways. The strategy is declared per field at schema definition time. The default for every field — used when `.conflict(...)` is omitted — preserves the v0.1.0 behaviour, so existing schemas keep working without changes.

### Strategies

| Strategy          | Storage cost | Data safety            | When to use                                          |
| ----------------- | ------------ | ---------------------- | ---------------------------------------------------- |
| `Crdt(...)`       | High         | No data loss           | Counters, sets, registers needing concurrent merging |
| `LastWriteWins`   | Low          | Loses concurrent write | Scalar fields where one-of-two is acceptable         |
| `ServerAuthority` | Low          | Discards local write   | Server-managed values (balances, prices, flags)      |
| `Custom(id)`      | Medium       | Developer-controlled   | Domain-specific resolution                           |

- **`Crdt(CrdtKind)`** keeps full per-device metadata so concurrent writes converge mathematically. Uses the existing CRDT primitives (`LwwRegister`, `OrSet`, `Counter`).
- **`LastWriteWins`** is a metadata-free shortcut: pick the value with the higher HLC timestamp; ties break on the higher `device_id` so all replicas converge.
- **`ServerAuthority`** treats the remote (server) value as authoritative — local writes that conflict with an incoming server value are discarded. Use this for fields whose source of truth is the server.
- **`Custom(id)`** invokes a developer-registered merge function. If no resolver is registered for `id` the field surfaces as `ResolveOutcome::Conflicted` so the application can resolve it.

### Schema example

```rust
use raftdb::schema::{
    Schema, FieldType, ConflictStrategy, CrdtKind, CustomResolverId,
};

let schema = Schema::builder("user")
    .field("name", FieldType::String)
        .conflict(ConflictStrategy::LastWriteWins)
    .field("balance", FieldType::Int)
        .conflict(ConflictStrategy::ServerAuthority)
    .field("tags", FieldType::Collection)
        .conflict(ConflictStrategy::Crdt(CrdtKind::OrSet))
    .field("preferences", FieldType::Bytes)
        .conflict(ConflictStrategy::Custom(CustomResolverId::new("merge_prefs")))
    .build()
    .expect("valid");
```

### Custom resolvers

```rust
use raftdb::query::Value;
use raftdb::sync::{ConflictResolver, ResolveOutcome};

let mut resolver = ConflictResolver::new();
resolver.register("max_int", |local, remote| match (&local, &remote) {
    (Value::Int(a), Value::Int(b)) => Value::Int((*a).max(*b)),
    _ => remote,
});
```

A resolver may be registered after the schema is loaded. Fields whose resolver is not yet registered surface as `ResolveOutcome::Conflicted { local, remote }`.

### Per-field vs per-collection

The per-field `ConflictStrategy` overrides the per-collection [`SyncAuthority`](src/sync/authority.rs). `SyncAuthority` continues to work for collections that don't need finer-grained control; for collections that do, set strategies on individual fields and they take precedence.

### Migration from v0.1.0

No code changes are required. Schemas constructed without calling `.conflict(...)` deserialise and behave exactly as before:

| Field type   | Default strategy        |
| ------------ | ----------------------- |
| Scalar types | `Crdt(LwwRegister)`     |
| `Collection` | `Crdt(OrSet)`           |

Fields can opt in to the new strategies one at a time.

### What's deferred

Two pieces of the v0.1.1 conflict-resolution roadmap are deferred to a follow-up PR alongside the sync apply path:

- Document-level conflict tracking (`Document::conflicts()`, `Document::resolve_conflict(...)`) — surfaces unresolved `Custom` conflicts to the application.
- FFI surface (`rft_schema_field_set_conflict`, `rft_register_resolver`) — exposes the strategies to platform bindings.

Until those land, the C ABI keeps the v0.1.0 surface and platform bindings continue to use the per-collection `SyncAuthority`.

## Tests

```bash
cd core
cargo test                 # unit + integration
cargo test --features ffi  # full surface including the C ABI
```
