# RaftDB — Swift

> Mobile-native embedded database for iOS / macOS. Idiomatic Swift over the Raft Rust core.

[![Swift](https://img.shields.io/badge/swift-5.9%2B-orange.svg)](https://swift.org)
[![Platforms](https://img.shields.io/badge/platforms-iOS%2014%20%7C%20macOS%2012-lightgrey.svg)]()

Offline-first key-value and document storage with `async`/`await`, `AsyncStream` observers, optimistic transactions, and a static `xcframework` linked from the Raft Rust core. Raft is **local-first**: your local database is the source of truth, and any network sync is an optional layer on top.

---

## Contents

- [Install](#install)
- [Quickstart](#quickstart)
- [Concepts](#concepts)
- [API Reference](#api-reference)
  - [Lifecycle](#lifecycle)
  - [Raw key-value](#raw-key-value)
  - [Typed collections](#typed-collections)
  - [Queries](#queries)
  - [Transactions](#transactions)
  - [Observation](#observation)
  - [Errors](#errors)
- [Sync — local merge and peer integration](#sync--local-merge-and-peer-integration)
  - [The mental model](#the-mental-model)
  - [Hybrid Logical Clocks](#hybrid-logical-clocks)
  - [CRDTs explained](#crdts-explained)
  - [`SyncAuthority` modes](#syncauthority-modes)
  - [Per-field `ConflictStrategy`](#per-field-conflictstrategy)
  - [Plugging in a peer](#plugging-in-a-peer)
  - [Worked examples](#worked-examples)
  - [Current limits and what needs a peer](#current-limits-and-what-needs-a-peer)
- [Use case recipes](#use-case-recipes)
- [Troubleshooting](#troubleshooting)

---

## Install

Swift Package Manager — `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/yourusername/raft-db", from: "0.1.0"),
]
```

Or via Xcode: **File → Add Packages…** and paste the repository URL.

The package vendors a precompiled `RaftDB.xcframework` containing iOS and macOS slices — no extra build steps required.

---

## Quickstart

```swift
import RaftDB

struct User: Codable, Sendable {
    var id: UInt64 = 0
    let name: String
    let age: Int
}

func example() async throws {
    let url = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
        .appendingPathComponent("raft.db")
    let db = try await RaftDB.open(path: url.path)

    let users = RaftCollection<User>(db: db, name: "users")

    let id = try await users.putAuto(User(name: "Alice", age: 30))
    let alice = try await users.get(docId: id)        // User?
    try await users.delete(docId: id)

    db.close()
}
```

All blocking native calls are dispatched off the calling actor via `withCheckedThrowingContinuation` onto `DispatchQueue.global(qos: .userInitiated)`. Safe to call from any concurrency context.

---

## Concepts

Raft has **two complementary storage surfaces**:

1. **Raw key-value** — the underlying LSM-tree store. Keys and values are arbitrary `Data`. Use when you control the id space (UUIDs, slugs, content hashes).
2. **Typed collections** — documents addressed by `UInt64`. The engine assigns ids on `collectionPutAuto`, or you provide one via the document's `id` field. Typed collections enable indexed queries, change notifications, and the merge surface described in [Sync](#sync--local-merge-and-peer-integration).

`RaftCollection<T>` wraps either surface and handles serialization via `Codable`. Use raw-KV when you want to control the key shape yourself, typed collections when you want the engine to do the bookkeeping.

The two surfaces address **different storage namespaces**. Putting a document via one and reading via the other will not work. Pick one per logical collection.

---

## API Reference

### Lifecycle

```swift
// Open or create a database on disk
let db = try await RaftDB.open(path: "/path/to/db")

// Synchronous variant for tests or non-async contexts
let db2 = try RaftDB.openSync(path: path)

// Close releases the native handle. Safe to call multiple times.
db.close()
```

`RaftDB.open` does no network handshake — it's a pure local file open. The database is fully usable with zero network configuration.

### Raw key-value

```swift
// Write
try await db.put(key: Data("user:1".utf8), value: Data(#"{"name":"Alice"}"#.utf8))

// Read — nil for missing keys
let raw: Data? = try await db.get(key: Data("user:1".utf8))

// Delete (no-op if absent — a tombstone is written)
try await db.delete(key: Data("user:1".utf8))
```

A two-phase read protocol queries the value size first, then reads into an exact-size buffer — no buffer-size limits to tune.

### Typed collections

```swift
// Auto-assigned id
let id = try await db.collectionPutAuto("users",
    document: try JSONEncoder().encode(User(name: "Alice", age: 30)))

// Caller-chosen id (the JSON's `id` field is honoured)
try await db.collectionPut("users",
    document: try JSONEncoder().encode(User(id: 42, name: "Bob", age: 25)))

// Read
let raw: Data? = try await db.collectionGet("users", id: id)

// Delete (no-op if id missing)
try await db.collectionDelete("users", id: id)

// Cardinality
let count = try await db.collectionCount("users")

// All ids (sorted ascending)
let ids: [UInt64] = try await db.collectionListIds("users")
```

For typed `Codable` access, use `RaftCollection<T>`:

```swift
let users = RaftCollection<User>(db: db, name: "users")

let aliceId = try await users.putAuto(User(name: "Alice", age: 30))
let alice: User? = try await users.get(docId: aliceId)
let count = try await users.count()
let ids = try await users.listIds()
```

`RaftCollection<T>` also exposes a legacy String-id raw-KV surface (`put(id: String, document:)`, `get(id: String)`, `delete(id: String)`) for callers that want their own key namespace. These address a different storage namespace from the typed methods.

### Queries

```swift
let query = try JSONEncoder().encode([
    "collection": "users",
    "where": ["field": "age", "op": "gte", "value": 18],
    "limit": 100,
] as [String: Any])

let results: [Data] = try await db.executeQuery(query)
for docData in results {
    let user = try JSONDecoder().decode(User.self, from: docData)
}
```

Query JSON shape is documented in the Rust core's `query` module. A typed builder will land alongside the schema work.

### Transactions

```swift
try await db.withTransaction { txn in
    guard let raw = try txn.get(collection: "users", id: 42) else { return }
    var user = try JSONDecoder().decode(User.self, from: raw)
    user = User(id: user.id, name: user.name, age: user.age + 1)
    try txn.put(collection: "users", document: try JSONEncoder().encode(user))
}
// Committed if the closure returns; rolled back if it throws.
```

Or manually:

```swift
let txn = try db.beginTransaction()
do {
    try txn.put(collection: "users", document: encoded)
    try txn.commit()       // throws RaftError.transactionConflict on conflict
} catch {
    txn.rollback()
    throw error
}
```

The transaction handle is **consumed** by `commit` or `rollback`. Calling `commit` twice throws `RaftError.invalidHandle`; rolling back after commit is a no-op.

### Observation

`RaftDB.observe(collection:)` returns an `AsyncStream<MutationEvent>` backed by a real native callback (synchronous `@convention(c)` dispatch from the Rust tokio thread). Cancelling the consuming task unregisters the native subscription.

```swift
let task = Task {
    for await event in db.observe(collection: "users") {
        switch event.mutationType {
        case .insert: print("inserted \(event.docId)")
        case .update: print("updated \(event.docId)")
        case .delete: print("deleted \(event.docId)")
        }
        // event.origin: .local or .remote
    }
}
// later
task.cancel()  // unobserves the native subscription
```

Live query observers fire immediately with the initial snapshot, then again on every result-set change:

```swift
let queryData = try JSONEncoder().encode(query)
for await diff in db.observeQuery(queryData) {
    for addedJson in diff.added {
        let user = try JSONDecoder().decode(User.self, from: addedJson)
    }
    for removedJson in diff.removed { /* ... */ }
    for updatedJson in diff.updated { /* ... */ }
}
```

`RaftCollection<T>` exposes the same as `users.observe() -> AsyncStream<MutationEvent>` and `users.liveQuery(_:) -> AsyncStream<QueryDiff>`.

### Errors

All native failures throw a typed `RaftError`:

```swift
public enum RaftError: Error, Equatable {
    case nullPointer            // 1
    case invalidUtf8            // 2
    case ioError                // 3
    case notFound               // 4 (mapped to nil returns, never thrown)
    case bufferTooSmall         // 5 (internal, retried automatically)
    case invalidJson            // 6
    case transactionConflict    // 7
    case invalidHandle          // 8
    case unknownSubscription    // 9
    case unknown(UInt32)
}
```

```swift
do {
    try await db.withTransaction { txn in /* ... */ }
} catch RaftError.transactionConflict {
    // re-read and retry
}
```

---

## Sync — local merge and peer integration

**This is the most important conceptual section of this README.** Raft is designed so the merge primitives — what makes concurrent writes converge to the same result on every device — live inside the database. The network layer that actually moves bytes between devices is intentionally **outside** raft-db. You build it (your own backend), or you plug in [Relay](https://github.com/raft-db/relay) (a separate product), or you simply never wire one up — your single-device app works correctly without any of this.

### The mental model

```
┌─────────────────────────────────────────┐
│              Your iOS app               │
├─────────────────────────────────────────┤
│                RaftDB                   │
├─────────────────────────────────────────┤
│         libraftdb (Rust core)           │
│   ┌─────────────────────────────────┐   │
│   │  Mutation log (HLC-stamped)     │   │
│   │  Document store + indexes       │   │
│   │  ConflictStrategy + Authority   │← the "merge surface"
│   └─────────────────────────────────┘   │
└────────────▲────────────────────────────┘
             │
             │  optional, external:
             │
  ┌──────────┴──────────────────┐
  │  Your backend  /  Relay     │ ← the network layer is NOT
  └─────────────────────────────┘    part of raft-db
```

Every write in Raft is stamped with:

1. A **Hybrid Logical Clock (HLC)** timestamp — causality-aware time
2. A **device ID** — a stable 128-bit identifier for the writing device

These two values are what make Raft's merge rules deterministic. Any two devices that observe the same set of writes — in any order — will compute the same final state. That is the property a CRDT provides, and it's what lets you build distributed systems without distributed locks.

Crucially, **Raft works perfectly well without any of this leaving the device**. The HLC and device-id machinery exists primarily so that *when* a peer eventually arrives, the merge rules are already defined.

### Hybrid Logical Clocks

Wall-clock timestamps are unreliable: clocks drift, NTP corrections jump backwards, devices skew across time zones. Logical clocks (Lamport timestamps) preserve causality but lose physical time. HLCs combine both:

```
HlcTimestamp = (physical_ms: u64, logical: u16)
```

- **physical_ms** — best-effort wall-clock millis (monotonic where the OS allows)
- **logical** — a counter that increments when a logical event happens at the same physical millisecond

The combined ordering is total and stable: comparing two HLCs is a tuple comparison, and ties on physical time break by logical counter. When devices exchange writes, each device's HLC is bumped to be strictly greater than the max it's seen, preserving "happens-before" relationships across the network.

You don't interact with HLCs directly in the Swift API today — they're an implementation detail of the merge surface. They become observable when you implement a peer integration (see [Plugging in a peer](#plugging-in-a-peer)).

### CRDTs explained

A **Conflict-free Replicated Data Type** is a data type whose operations commute. Concretely: if two devices apply the same set of operations in any order, they end up with the same value.

Raft has three primitives:

#### LWW Register (`Crdt(LwwRegister)`)

For scalar values where "the most recent write wins". The value is paired with the HLC timestamp + device id of the writer:

- Two writes compared → the higher HLC wins
- HLC tie → higher device id wins (deterministic tiebreaker)

This is the **default** for `String`, `Int`, `Float`, `Bool`, `Bytes`, `Reference` fields. It's cheap, simple, and correct when "one of two concurrent writes is acceptable".

#### OR-Set (`Crdt(OrSet)`)

For unordered collections where add/remove can conflict. Each `add` records a tag `(device_id, hlc)`; `remove` records that the matching tag is no longer live. An element is present iff at least one of its add-tags has not been removed.

The convergence rule: if device A removes an element and device B concurrently adds it, the element ends up **present** (because B's add tag is later than A's remove). This matches user intent in practice — explicit additions beat removals at the same logical moment.

This is the **default** for `Collection` fields. Use it for sets of permissions, tags, group members, etc.

#### Counter (`Crdt(Counter)`)

For monotonically incrementing or decrementing values. Each device tracks its own per-device count; the total is the sum across devices. Concurrent increments from multiple devices are preserved (none are lost).

Use it for likes, view counts, inventory deltas — anywhere you need "all the +1s and -1s should count, regardless of when they were applied".

### `SyncAuthority` modes

`SyncAuthority` is a per-**collection** declaration that tells the merge surface how to resolve conflicts when a peer's write meets a local write. The three modes:

| Mode | Local write semantics | Conflict resolution when a remote write arrives |
|---|---|---|
| `LocalFirst` (default) | Always applied; HLC stamped | CRDT merge — deterministic, no data loss |
| `RemoteAuthority` | Provisional; can be overwritten | Remote unconditionally overwrites local |
| `RemoteFirst` | Always applied | CRDT merge same as `LocalFirst`; reads prefer remote when connected |

When **no peer is wired**, `LocalFirst` and `RemoteFirst` behave identically (no remote to consult). `RemoteAuthority` does nothing useful without a peer — there's no remote to be the authority.

The default of `LocalFirst` is intentional: Raft is local-first; the developer opts in to remote authority where it makes sense (feature flags, server-managed counters, etc.).

Today these enums are configured on the Rust side at schema creation time. A typed Swift configurator API ships alongside the schema builder work tracked separately.

### Per-field `ConflictStrategy`

While `SyncAuthority` is per-collection, `ConflictStrategy` is per-**field** and gives finer control. The variants:

| Strategy | Storage cost | Data safety | Typical use |
|---|---|---|---|
| `Crdt(LwwRegister)` | Low | No data loss for scalar overwrites | Names, descriptions |
| `Crdt(OrSet)` | Medium (per-device tags) | No data loss for set ops | Tags, group members |
| `Crdt(Counter)` | Low (per-device count) | No data loss for +/- ops | Likes, view counts |
| `LastWriteWins` | Lowest | Silently drops concurrent writes | Settings that don't matter much |
| `ServerAuthority` | Lowest | Discards conflicting local writes | Inventory, balances |
| `Custom(id)` | Medium | Developer-controlled | Domain-specific merges |

Each field's default strategy is derived from its declared type and CRDT hint. You override per-field when the default isn't what you want — e.g. a `Bool` field for "user has seen onboarding" might be `LastWriteWins` because no merge nuance is needed.

### Plugging in a peer

A peer is anything that ingests local writes and emits remote ones. Two common shapes:

#### Your own backend

You already have a server. You want to:
1. Send local writes to the server after they happen
2. Receive server-side updates and apply them locally

```swift
// 1. Tail the mutation stream and push local writes
Task {
    for await event in db.observe(collection: "users") {
        guard event.origin == .local else { continue }
        if let doc = try await db.collectionGet("users", id: event.docId) {
            try await yourBackend.upsert(collection: "users", id: event.docId, doc: doc)
        }
    }
}

// 2. Apply incoming server writes back into Raft
Task {
    for await change in yourBackend.subscribeChanges() {
        try await db.collectionPut("users", document: change.documentJson)
    }
}
```

The engine handles HLC ordering / CRDT merge if you stamp the incoming write with the server's HLC. For a `RemoteAuthority` collection, the value you pass overwrites local. For `LocalFirst`, the CRDT merge runs.

#### Relay

[Relay](https://github.com/raft-db/relay) is a separate product that gives you a Redis-style network sync layer with no backend code. Drop it in, point your devices at it, and the merge surface is wired for you. Relay is **not** part of raft-db — it ships independently and consumes raft-db's merge primitives.

### Worked examples

#### A user profile (mixed strategies per field)

```
collection: users
  field name        — Crdt(LwwRegister), SyncAuthority::LocalFirst
  field email       — Crdt(LwwRegister), SyncAuthority::LocalFirst
  field tags        — Crdt(OrSet),       SyncAuthority::LocalFirst
  field balance     — Crdt(Counter),     SyncAuthority::RemoteAuthority  ← server controls this
  field last_seen   — LastWriteWins,     SyncAuthority::LocalFirst       ← we don't care about losing one
```

When two devices edit the same user:
- Both name edits converge to the later HLC (LWW)
- Both tag mutations preserve adds/removes correctly (OR-Set)
- Balance only honours the server's value (RemoteAuthority)
- last_seen drops one of the two writes deterministically

#### A feature-flag collection

```
collection: feature_flags
  SyncAuthority::RemoteAuthority
  all fields: LastWriteWins
```

The local device caches the flags for offline reads; any local mutation is provisional and gets overwritten by the next server push. This is the "Don't let the client meaningfully edit this" pattern.

#### A counter (likes)

```
collection: posts
  field likes — Crdt(Counter), SyncAuthority::LocalFirst
```

A user double-taps to like a post on the train (offline). Another user double-taps on a different device, also offline. Both reconnect: the like count is the sum of both increments, regardless of who synced first.

### Current limits and what needs a peer

What Raft does **today, with zero network code**:
- Local-only embedded database — open, read, write, query, transaction, observe all work
- HLC timestamps stamped on every write
- CRDT merge logic active for any concurrent local writes
- Schema-declared `SyncAuthority` and `ConflictStrategy` validated and applied
- Mutation log preserved as the source of truth

What needs a peer to be useful:
- `SyncAuthority::RemoteAuthority` / `RemoteFirst` semantics — without a peer, these degrade to local-only writes
- The `RemoteFirst` read-through-network behaviour
- Any actual cross-device data movement

What is **out of scope** for raft-db:
- The wire protocol for sending writes to a peer
- A sync engine that pushes/pulls deltas
- A reference sync server

Those live in Relay (or your own backend). raft-db gives you the merge primitives so whichever peer you choose can be a thin layer over `applyRemote`-style hooks.

---

## Use case recipes

### Repository pattern with `RaftCollection`

```swift
actor UserRepository {
    private let users: RaftCollection<User>

    init(db: RaftDB) { self.users = RaftCollection(db: db, name: "users") }

    func create(name: String, age: Int) async throws -> User {
        var u = User(name: name, age: age)
        let id = try await users.putAuto(u)
        u = User(id: id, name: u.name, age: u.age)
        return u
    }

    func update(_ user: User) async throws { try await users.put(docId: user.id, document: user) }
    func load(id: UInt64) async throws -> User? { try await users.get(docId: id) }
    func delete(id: UInt64) async throws { try await users.delete(docId: id) }
}
```

### Atomic counter increments via transaction

```swift
func increment(db: RaftDB, counterId: UInt64) async throws {
    for attempt in 0..<5 {
        do {
            try await db.withTransaction { txn in
                guard let raw = try txn.get(collection: "counters", id: counterId) else { return }
                var c = try JSONDecoder().decode(Counter.self, from: raw)
                c.value += 1
                try txn.put(collection: "counters",
                            document: try JSONEncoder().encode(c))
            }
            return
        } catch RaftError.transactionConflict {
            try await Task.sleep(nanoseconds: UInt64(50_000_000) << attempt)
        }
    }
}
```

### SwiftUI live-query view

```swift
@MainActor
final class UserListViewModel: ObservableObject {
    @Published private(set) var users: [User] = []
    private var observeTask: Task<Void, Never>?

    func bind(to db: RaftDB, query: Data) {
        observeTask = Task { [weak self] in
            for await diff in db.observeQuery(query) {
                guard let self else { return }
                // Apply diff.added / .removed / .updated to `users`
            }
        }
    }

    deinit { observeTask?.cancel() }
}
```

### Caching network responses with a TTL (raw-KV)

```swift
func cachedGet(_ url: URL) async throws -> Data {
    if let raw = try await db.get(key: Data("cache:\(url)".utf8)) {
        let entry = try JSONDecoder().decode(CacheEntry.self, from: raw)
        if entry.expiresAt > Date() { return entry.body }
    }
    let (body, _) = try await URLSession.shared.data(from: url)
    let entry = CacheEntry(body: body, expiresAt: Date().addingTimeInterval(60))
    try await db.put(key: Data("cache:\(url)".utf8),
                     value: try JSONEncoder().encode(entry))
    return body
}
```

---

## Troubleshooting

**Linker error: `Undefined symbol: _rft_open`** — the static library inside `RaftDB.xcframework` isn't being linked. In Xcode, **Build Phases → Link Binary With Libraries**, ensure `RaftDB.xcframework` is present. For Swift Package consumers this should be automatic; if you're building from source you may need a macOS slice (see `swift/RaftDB.xcframework`).

**`swift test` fails to link locally** — the shipped xcframework only includes iOS slices (`ios-arm64`, `ios-arm64_x86_64-simulator`). Add a macOS slice or run tests with `xcodebuild test -destination 'platform=iOS Simulator,name=iPhone 15'`.

**`RaftError.invalidJson`** — your `collectionPut` payload or query JSON didn't parse. The Rust side is strict; check `JSONEncoder()` configuration (`.useDefaultKeys` rather than camelCase if the schema expects snake_case).

**`RaftError.transactionConflict`** — another transaction modified a doc you read. Catch and retry, or restructure to read the smallest possible set of docs. Use exponential backoff for hot keys.

**`AsyncStream` task cancellation doesn't unsubscribe** — the `onTermination` handler runs synchronously; verify the consuming `Task` is actually being cancelled (not just paused).

---

## License

Dual-licensed under Apache-2.0 and MIT. See `LICENSE` and `LICENSE-MIT`.
