# Raft documentation

Cross-platform reference for Raft. Each surface is documented once and shown in all four supported languages: **Swift**, **Kotlin**, **Dart (Flutter)**, and **TypeScript (React Native)**.

If you're learning a single platform, the per-platform READMEs are tighter starting points:

- [`flutter/README.md`](../flutter/README.md)
- [`android/README.md`](../android/README.md)
- [`swift/README.md`](../swift/README.md)
- [`rn/README.md`](../rn/README.md)

These cross-platform docs go deeper on each individual surface, with more examples and edge-case coverage.

## Surfaces

| Surface | Doc | What it covers |
|---|---|---|
| Raw key-value | [raw-kv.md](raw-kv.md) | Byte-addressed get/put/delete on the LSM-tree |
| Typed collections | [collections.md](collections.md) | Document store with engine-assigned ids, count, listing |
| Queries | [queries.md](queries.md) | Predicate query execution and result decoding |
| Transactions | [transactions.md](transactions.md) | Optimistic concurrency with read-set tracking |
| Observation | [observation.md](observation.md) | Live mutation events and query diffs |
| Errors | [errors.md](errors.md) | The unified error model and per-platform mappings |
| Memory ownership | [memory-ownership.md](memory-ownership.md) | Who allocates and frees each FFI handle, per binding |
| Thread affinity | [threading.md](threading.md) | Which thread callbacks fire on and how bindings marshal them |
| Sync (merge surface) | [sync.md](sync.md) | HLCs, CRDTs, `SyncAuthority`, `ConflictStrategy`, peer integration |

## Conventions used in these docs

- **`db`** — an opened `RaftDB` / `RaftDb` / `RaftDB` handle (the casing follows each platform's idiom).
- **Example types** — `User { id: Long/UInt64/int/number, name: String, age: Int/Int/int/number }` appears throughout. The `id` field is the storage document id when using the typed-collection surface.
- **Tabs** — each example block lists Swift → Kotlin → Dart → TypeScript in that order. Pick the one you read.
- **JSON shapes** — every cross-FFI value goes as JSON; the deserialized shape on the JS / Dart / Kotlin / Swift side is the same JSON object, just decoded with the platform's native JSON library.

## Glossary

- **HLC** — Hybrid Logical Clock. Every write is stamped with a `(physical_ms, logical)` pair so concurrent writes have a deterministic total order. See [sync.md](sync.md#hybrid-logical-clocks).
- **CRDT** — Conflict-free Replicated Data Type. Data type whose operations commute, so any device that observes the same set of writes ends up with the same state. See [sync.md](sync.md#crdts).
- **Authority** — per-collection declaration of who wins a conflict. `LocalFirst` (default), `RemoteAuthority`, or `RemoteFirst`. See [sync.md](sync.md#syncauthority).
- **Strategy** — per-field declaration of how a conflict resolves. `Crdt(kind)`, `LastWriteWins`, `ServerAuthority`, `Custom(id)`. See [sync.md](sync.md#conflictstrategy).
- **Peer** — any network endpoint that ingests local writes and emits remote ones. Could be your own backend or Relay. Out of scope for raft-db itself.

## Versioning

These docs match raft-db v0.1.0. API stability follows semver — minor versions are additive, major versions are allowed to break the public surface.
