# Merge-surface contract

[← Back to docs index](README.md)

This document is the **integrator contract** for anything that acts as a
network peer to raft-db — Relay, or a developer's own backend adapter.
Where [sync.md](sync.md) explains the merge surface conceptually, this
doc pins down exactly *what is stable*, *what is versioned*, and *what
semver promises apply*. If you build against the surfaces below, a
raft-db upgrade within the declared compatibility window will not break
you.

## Contents

- [Scope](#scope)
- [The stable trait surface](#the-stable-trait-surface)
- [Versioned formats](#versioned-formats)
- [Semver policy](#semver-policy)
- [Compatibility matrix](#compatibility-matrix)
- [What is explicitly unstable](#what-is-explicitly-unstable)

---

## Scope

A peer integrates with raft-db through exactly three surfaces:

1. **Ingest** — apply remote writes locally (`Database::put` /
   `collectionPut` from bindings). The merge surface resolves conflicts.
2. **Egress** — tail local mutations (`observeCollection`) and push them
   upstream.
3. **Configuration** — schema-declared `SyncAuthority` (per collection)
   and `ConflictStrategy` (per field) determine merge behaviour.

Everything else — the wire protocol, transport, retry logic, server
storage — is the peer's business. raft-db never dictates it.

## The stable trait surface

These types are the contract. They are re-exported from stable paths
and governed by the [semver policy](#semver-policy) below.

### `crdt::Merge` (frozen)

```rust
pub trait Merge {
    fn merge(&mut self, other: &Self);
}
```

The pure CRDT merge trait. **Frozen**: no new required methods will be
added in any 0.x or 1.x release. Implementations shipped by raft-db
(`LwwRegister`, `OrSet`, `Counter`) guarantee commutativity,
associativity, and idempotency — the properties peers rely on to apply
writes in any order, any number of times.

### `sync::SyncAuthority` (stable, may grow)

```rust
#[non_exhaustive-equivalent: matching must include a wildcard arm]
pub enum SyncAuthority { LocalFirst, RemoteAuthority, RemoteFirst }
```

Per-collection merge mode, serialized into the schema (serde). Existing
variants and their semantics never change; new variants may be added in
minor releases. Peers must treat unknown authority values as
`LocalFirst` (the safe default: CRDT merge, no data loss).

### `schema::ConflictStrategy` (stable, may grow)

```rust
pub enum ConflictStrategy {
    Crdt(CrdtKind),        // LwwRegister | OrSet | Counter
    LastWriteWins,         // HLC order, device-id tiebreak
    ServerAuthority,       // remote wins unconditionally
    Custom(CustomResolverId),
}
```

Per-field override, serialized into the schema. Defaults derive from
the field type via `ConflictStrategy::default_for` — those defaults are
part of the contract and will not change for existing field types.

### `sync::ConflictResolver` / `ResolveOutcome` (stable)

- `resolve_lww` / `resolve_counter` / `resolve_orset` — CRDT-level
  resolution under a given `SyncAuthority` + `MergeContext`.
- `resolve_value` — value-level resolution under a per-field
  `ConflictStrategy`; returns `ResolveOutcome::Resolved(Value)` or
  `ResolveOutcome::Conflicted { local, remote }`.
- `register(id, fn)` — attach a `Custom` merge function. Unregistered
  custom strategies surface as `Conflicted`, never panic, never drop
  data.

### `wal::HlcTimestamp` (frozen)

```rust
pub struct HlcTimestamp { pub physical: u64, pub logical: u16 }
```

Total order: `(physical, logical)` lexicographic, then `device_id`
(`u128`) as the final tiebreaker. This ordering rule is **frozen** — it
is what makes merge deterministic across every device and every peer,
and it will not change in any release, ever. Encoded size is fixed at
10 bytes big-endian (`physical` u64 BE, then `logical` u16 BE).

## Versioned formats

Every persisted or exchanged byte format carries an explicit version
marker. A reader that encounters an unknown version must refuse loudly
(and raft-db's own readers do).

| Format | Marker | Current | Where |
|---|---|---|---|
| WAL entry | structural (fixed header, crc32 trailer) | v1 | `[hlc 10B][device_id u128 BE][payload_len u32 BE][payload][crc32 BE]`, payload ≤ 16 MiB |
| SSTable | magic `RFST` in footer | v1 | immutable table files |
| Backup snapshot | magic `RFTBKUP1` (digit = version) | v1 | `export_backup` / `restore_backup` files |
| Query envelope (FFI) | explicit `v` field | 1 | JSON query spec across the C ABI |
| Schema document | serde-versioned via strategy docs | v0.1.1 surface | collection schemas incl. `SyncAuthority`, `ConflictStrategy` |

Versioning rules:

- **New format version ⇒ new magic/version value.** `RFTBKUP2` would be
  a new snapshot format; readers of v1 reject it with a typed error
  rather than misparsing.
- **Old readers never silently misread new data** — every format is
  checksummed (crc32) and length-bounded, so version skew fails closed.
- **New readers keep reading old versions** for at least one minor
  release after a format bump, providing an upgrade path.

### What a peer actually puts on the wire

raft-db deliberately does not define your wire protocol. The contract
is narrower: whatever you transmit, the bytes you hand back into
`collectionPut` must round-trip the document JSON you observed, and if
you carry HLC/device metadata (recommended), carry it opaquely and
unmodified. Peers must never invent, reorder, or renumber HLC
timestamps — doing so breaks causality and voids the convergence
guarantee.

## Semver policy

raft-db is pre-1.0 (`0.y.z`). The declared policy, which is stricter
than Cargo's default pre-1.0 semantics:

- **Patch (`0.y.z → 0.y.z+1`)** — bug fixes only. No public API
  changes, no format changes. Always safe.
- **Minor (`0.y → 0.y+1`)** — may add API (new methods, new enum
  variants on `SyncAuthority`/`ConflictStrategy`, new format *readers*).
  May **not** remove or change the semantics of anything listed in
  [the stable trait surface](#the-stable-trait-surface), and may not
  drop the ability to read the previous on-disk format version.
- **Frozen items** — the `Merge` trait shape, the HLC ordering rule,
  and the deterministic-convergence guarantee are exempt from even
  major-version change. They are the product.
- **1.0** — at 1.0 the entire stable surface above graduates to
  standard semver: breaking changes only at 2.0, with a documented
  migration for every persisted format.

Deprecation flow: mark `#[deprecated]` in minor `N`, remove no earlier
than minor `N+2`. Every deprecation names its replacement.

## Compatibility matrix

| Integrator uses | Guaranteed compatible with |
|---|---|
| `Merge`, HLC ordering | every future release (frozen) |
| `SyncAuthority` / `ConflictStrategy` semantics | all releases in the same minor series, plus additive minors |
| Backup snapshot v1 (`RFTBKUP1`) | readable until at least one minor after a v2 format ships |
| Query envelope `v:1` | supported until at least one minor after `v:2` ships |
| Anything under [unstable](#what-is-explicitly-unstable) | the exact version pinned in your lockfile — nothing else |

## What is explicitly unstable

Not covered by any promise; may change in any release:

- Internal module layout (`engine`, `memtable`, `sstable`, `compaction`
  internals) — depend on the re-exports, not the modules.
- The byte layout of CRDT metadata inside stored documents (only its
  *semantics* are stable, via the resolver API).
- The FFI C ABI beyond the query envelope's `v` field — bindings are
  versioned with the crate, not independently.
- Benchmarks, test helpers, fuzz targets.

If you need something promoted from this list into the contract, open
an issue — widening the contract is a minor release; narrowing it is
not allowed.

## Related

- [Sync — the merge surface](sync.md) — concepts, CRDTs, worked examples
- [Observation](observation.md) — the egress surface
- [Errors](errors.md) — typed failures peers must handle
