# Raft — Mobile-Native Embedded Database

## What This Is

Raft is an embedded, offline-first database built in Rust, designed from the ground up for mobile clients. It is not an adaptation of a server-side database. Every architectural decision assumes the device is the primary compute, connectivity is unreliable, and sync is a feature — not a requirement.

By default, the local database is the source of truth. The network is a sync channel. Developers can configure the authority model per collection via `SyncAuthority` — see [Key Design Decisions](#key-design-decisions).

---

## Project Identity

| Key | Value |
|---|---|
| Crate name | `raft-db` |
| C header prefix | `rft_` |
| Flutter package | `raft_db` |
| Android artifact | `com.raftdb` |
| Swift package | `RaftDB` |
| Language | Rust |
| Target platforms | iOS, Android, Linux, macOS, Windows |
| FFI targets | Dart (dart:ffi), Kotlin (JNI), Swift (UniFFI or manual bridge) |

---

## Architecture Overview

```
┌─────────────────────────────────────────┐
│           Application Layer             │
│     (Dart / Kotlin / Swift bindings)    │
├─────────────────────────────────────────┤
│            Query Engine                 │
│   (typed, reactive, live observers)     │
├─────────────────────────────────────────┤
│           Document Store                │
│  (typed objects, indexed, schema-aware) │
├─────────────────────────────────────────┤
│         Mutation Log (append-only)      │
│      (source of truth — CRDTs)          │
├─────────────────────────────────────────┤
│           Storage Engine                │
│   (LSM-tree, optimised for mobile I/O)  │
├─────────────────────────────────────────┤
│          Sync Layer (optional)          │
│  (protocol spec, pluggable backend)     │
└─────────────────────────────────────────┘
```

The mutation log is the foundation. The document store is a projection of it. Sync is log replication. Conflict resolution is CRDT merge logic on the log.

---

## Module Structure

```
src/
├── lib.rs
├── wal/          # Write-Ahead Log — append-only, HLC timestamps, crc32 checksums
├── memtable/     # In-memory sorted buffer before SSTable flush
├── sstable/      # Immutable sorted string table files
├── compaction/   # Idle-aware, battery-conscious compaction scheduler
├── manifest/     # Tracks SSTable versions and DB state
├── crdt/         # CRDT primitives — LWW register, OR-set, counter
├── schema/       # Schema DSL parser and runtime type registry
├── index/        # Secondary indexes (B-tree + hash)
├── query/        # Typed predicate query engine and planner
├── reactive/     # Pub/sub engine, live query subscriptions, diff emission
├── transaction/  # Optimistic concurrency, batch writes, rollback
├── ffi/          # C ABI layer — stable interface for all platform bindings
└── sync/         # Sync engine, protocol, connectivity-aware scheduler
```

---

## Implementation Phases

### Phase 1 — Storage Engine (Months 1–3)
- WAL: append-only log, HLC timestamps, crc32 checksums, binary encoding
- MemTable: in-memory sorted buffer (BTreeMap), size-bounded flush trigger
- SSTable: immutable file format, bloom filter, block index
- Compaction: levelled strategy, idle-aware scheduling
- Manifest: tracks live SSTables and DB version

### Phase 2 — Document Store + Query Engine (Months 3–5)
- Typed schema system — field types, CRDT type per field, additive migrations
- Secondary indexes — B-tree (range), hash (equality), composite
- Query engine — typed predicate API, index-aware planner, no SQL

### Phase 3 — Reactive Layer (Months 5–6)
- Internal pub/sub bus (tokio broadcast channels)
- Live query subscriptions — query result diffing, push on change
- Transaction model — optimistic concurrency, batch write, rollback

### Phase 4 — Cross-Platform Bindings (Months 6–8)
- C ABI via cbindgen — opaque handles, error propagation, memory ownership
- Dart/Flutter bindings — dart:ffi + generated type-safe API + Stream<T>
- Kotlin/Android — JNI bridge + Flow<T>
- Swift/iOS — UniFFI or manual bridge + AsyncSequence

### Phase 5 — Sync Protocol (Months 8–11)
- Open, backend-agnostic delta sync protocol (protobuf wire format)
- Client sync engine — connectivity-aware, per-collection opt-in
- Sync state as queryable fields: `isPendingSync`, `lastSynced`, `conflictedFields`
- Reference server in Rust (Docker-deployable, not a cloud product)

### Phase 6 — DX + Ecosystem (Months 11–14)
- CLI: schema management, migration, inspection
- DB browser / inspector GUI
- Public benchmark suite vs Realm, Isar, SQLite
- Docs site — mobile-first mental model throughout

---

## Core Constraints

- **no_std-compatible where possible** — smaller binary footprint for mobile
- **Minimal dependencies** — justify every crate added
  - Allowed without question: `serde`, `bytes`, `crc32fast`, `tokio`, `thiserror`
  - Requires justification: anything else
- **No external database crates** — we are building the database
- **Clean Rust API first** — C ABI comes in Phase 4, do not design for FFI prematurely
- **Every module has inline unit tests** — no exceptions
- **No SQL** — query API is typed predicates only

---

## CRDT Model

Every field in a document is backed by a CRDT type declared in the schema. This makes conflict resolution automatic and deterministic.

| CRDT Type | Use Case |
|---|---|
| LWW Register | Scalar fields (string, number, bool) |
| OR-Set | Collections where add/remove can conflict |
| Counter | Numeric fields that increment/decrement |
| Causal Tree | Ordered text / rich content (future) |

All mutations are stamped with a **Hybrid Logical Clock (HLC)** and a **device ID**. Two devices making conflicting writes will always converge to the same state without manual resolution.

---

## Key Design Decisions

| Decision | Choice | Reason |
|---|---|---|
| Storage format | LSM-tree | Lower write amplification than B-tree; better for mobile I/O patterns |
| Timestamp | HLC (Hybrid Logical Clock) | Causality-aware without requiring clock sync |
| Conflict resolution | Configurable per-collection | `LocalFirst` (CRDT merge), `RemoteAuthority` (server wins), `RemoteFirst` (read-through) |
| Query API | Typed predicates | No SQL parser, no string injection, better DX |
| Sync | Optional, per-collection | Not every collection needs to sync |
| FFI | C ABI + cbindgen | Maximum portability across language runtimes |

---

## What Raft Is Not

- Not a server database adapted for mobile
- Not a cloud product or vendor lock-in
- Not a SQL database
- Not a key-value store (though one powers it internally)
- Not a replacement for your backend — it's the client layer

---

## Naming Rationale

**Raft** — you are always on the raft (local, offline). The ocean (sync) is optional. The name evokes resilience, mobility, and the idea that the local node is complete and self-sufficient without a connection.

The name intentionally echoes (but is distinct from) the Raft consensus algorithm — a nod to distributed systems thinking without claiming to implement it.
