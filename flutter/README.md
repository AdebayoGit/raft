# raft_db — Flutter

> Mobile-native embedded database for Flutter, with a Dart API over the Raft Rust core.

[![pub](https://img.shields.io/pub/v/raft_db.svg)](https://pub.dev/packages/raft_db)

Offline-first key-value and document storage with reactive queries, optimistic transactions, and LSM-tree durability — all without a SQL parser, all on the device. Raft is **local-first**: your local database is the source of truth, and any network sync is an optional layer on top.

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

```yaml
dependencies:
  raft_db: ^0.1.0
```

```bash
flutter pub get
```

The plugin ships native binaries for Android (.so), iOS (.xcframework), macOS, Linux, and Windows. No extra Gradle / CocoaPods config required.

---

## Quickstart

```dart
import 'dart:convert';
import 'package:path_provider/path_provider.dart';
import 'package:raft_db/raft_db.dart';

Future<void> main() async {
  final dir = await getApplicationDocumentsDirectory();
  final db = await RaftDb.open('${dir.path}/raft');

  // Typed collection — engine assigns the doc id
  final aliceId = await db.collectionPutAuto(
    'users',
    Uint8List.fromList(utf8.encode(jsonEncode({'name': 'Alice', 'age': 30}))),
  );

  // Read by id
  final raw = await db.collectionGet('users', aliceId);
  final alice = jsonDecode(utf8.decode(raw!));
  print(alice['name']); // Alice

  await db.close();
}
```

All native calls dispatch through `Isolate.run` so the calling isolate never blocks on I/O.

---

## Concepts

Raft has **two complementary storage surfaces**:

1. **Raw key-value** — the underlying LSM-tree store. Keys and values are arbitrary bytes. Use when you control the id space (UUIDs, slugs, content hashes).
2. **Typed collections** — documents addressed by `int` (uint64 on the native side). The engine assigns ids on `collectionPutAuto`, or you provide one via the document's `id` field. Typed collections enable indexed queries, change notifications, and the merge surface described in [Sync](#sync--local-merge-and-peer-integration).

A `RaftCollection<T>` wrapper sits on top of either surface and handles serialization. Use raw-KV when you want to control the key shape yourself, typed collections when you want the engine to do the bookkeeping.

The two surfaces address **different storage namespaces**. Putting a document via one and reading via the other will not work. Pick one per logical collection.

---

## API Reference

### Lifecycle

```dart
// Open or create a database on disk.
final db = await RaftDb.open('/path/to/db');

// Close releases the native handle and aborts any in-flight observers.
// Safe to call multiple times.
await db.close();
```

`RaftDb.open` does no network handshake — it's a pure local file open. The database is fully usable with zero network configuration.

### Raw key-value

The byte-addressed surface. Use for content-addressed storage, custom indexing, or migrating data that already has its own key scheme.

```dart
// Write
await db.put(
  utf8.encode('user:1'),
  utf8.encode(jsonEncode({'name': 'Alice'})),
);

// Read — returns null for missing keys
final bytes = await db.get(utf8.encode('user:1'));
if (bytes != null) {
  final user = jsonDecode(utf8.decode(bytes));
}

// Delete — not an error if key is absent (a tombstone is written)
await db.delete(utf8.encode('user:1'));
```

A two-phase read protocol queries the value size first, then reads into an exact-size buffer — no buffer-size limits to tune.

### Typed collections

Document-style storage with engine-assigned ids and typed change notifications.

```dart
// Auto-assigned id
final id = await db.collectionPutAuto(
  'users',
  Uint8List.fromList(utf8.encode(jsonEncode({'name': 'Alice'}))),
);

// Caller-chosen id (the JSON's `id` field is honoured)
await db.collectionPut(
  'users',
  Uint8List.fromList(utf8.encode(jsonEncode({'id': 42, 'name': 'Bob'}))),
);

// Read
final raw = await db.collectionGet('users', id);

// Delete (no-op if id missing)
await db.collectionDelete('users', id);

// Cardinality
final count = await db.collectionCount('users');

// List all ids (sorted ascending)
final ids = await db.collectionListIds('users');
```

For typed serialization, use `RaftCollection<T>`:

```dart
class User {
  User({required this.name, this.id = 0});
  final int id;
  final String name;

  Map<String, dynamic> toJson() => {'id': id, 'name': name};
  factory User.fromJson(Map<String, dynamic> j) =>
      User(id: j['id'] as int, name: j['name'] as String);
}

final users = db.collection<User>(
  name: 'users',
  serialize: (u) => Uint8List.fromList(utf8.encode(jsonEncode(u.toJson()))),
  deserialize: (b) => User.fromJson(jsonDecode(utf8.decode(b))),
);

final aliceId = await users.putAuto(User(name: 'Alice'));
final alice = await users.getById(aliceId); // User?
```

`RaftCollection<T>` also exposes the legacy String-id raw-KV surface (`put(id: String, …)`, `get(id: String)`, `delete(id: String)`) for callers that want their own key namespace. These address a different storage namespace from the typed methods.

### Queries

Predicate queries are JSON-encoded and executed by the engine's planner (index-aware, no SQL).

```dart
final results = await db.executeQuery(
  Uint8List.fromList(utf8.encode(jsonEncode({
    'collection': 'users',
    'where': {'field': 'age', 'op': 'gte', 'value': 18},
    'limit': 100,
  }))),
);

for (final docJson in results) {
  final doc = jsonDecode(utf8.decode(docJson));
  print(doc);
}
```

Query JSON shape is documented in the Rust core's `query` module. Future versions will provide a typed builder.

### Transactions

Optimistic concurrency with read-set tracking. Either commit succeeds atomically, or `TransactionConflict` is thrown and no writes are applied.

```dart
await db.withTransaction((txn) async {
  final raw = await txn.get('users', 42);
  final user = jsonDecode(utf8.decode(raw!));
  user['age'] = user['age'] + 1;
  await txn.put('users',
      Uint8List.fromList(utf8.encode(jsonEncode(user))));
});
// Committed if the block returns; rolled back if it throws.
```

Or manually:

```dart
final txn = await db.beginTransaction();
try {
  await txn.put('users', userJson);
  await txn.commit();          // RaftDbException(code 7) on conflict
} catch (e) {
  await txn.rollback();
  rethrow;
}
```

The transaction handle is **consumed** by commit or rollback. Calling commit twice throws; rolling back after commit is a no-op.

### Observation

Two observers are wired through the typed-FFI surface:

- **Per-collection observer** — fires on every insert/update/delete in a collection
- **Live query observer** — fires immediately with the initial snapshot, then again on every result-set change

> **Note (Dart):** observe wiring is currently deferred on Dart. The Rust core emits events with a stack-local `CString` that's freed when the synchronous callback returns. Swift/Kotlin process events synchronously on the Rust tokio thread; Dart's only safe cross-thread option (`NativeCallable.listener`) dispatches asynchronously, so the pointer is dangling by the time Dart reads it. Wiring through a `Dart_PostCObject_DL`-based adapter is tracked separately. For now, model change notifications via your own pub/sub layer.

When the Dart adapter ships, the API will be:

```dart
final sub = db.observeCollection('users').listen((event) {
  switch (event.mutationType) {
    case MutationKind.insert:
      // ...
    case MutationKind.update:
      // ...
    case MutationKind.delete:
      // ...
  }
});

// Live query: emits a snapshot diff immediately, then on every change
final qsub = db.observeQuery(queryJson).listen((diff) {
  for (final added in diff.added) {
    // newly matching documents (raw JSON bytes)
  }
});

await sub.cancel();
await qsub.cancel();
```

### Errors

All native failures throw `RaftDbException` with an integer `code` field:

| Code | Meaning |
|---|---|
| 1 | NullPointer — an internal argument was null |
| 2 | InvalidUtf8 — a string argument wasn't valid UTF-8 |
| 3 | IoError — storage engine I/O failure |
| 4 | NotFound — returned as `null` from `get`, never thrown |
| 5 | BufferTooSmall — internal, retried automatically |
| 6 | InvalidJson — a document or query JSON failed to parse |
| 7 | TransactionConflict — a tracked document was modified concurrently |
| 8 | InvalidHandle — a freed transaction/result/subscription was reused |
| 9 | UnknownSubscription — `unobserve` called with an unknown id |

```dart
try {
  await db.withTransaction((txn) async { /* ... */ });
} on RaftDbException catch (e) {
  if (e.code == 7) {
    // Conflict — re-read and retry
  } else {
    rethrow;
  }
}
```

---

## Sync — local merge and peer integration

**This is the most important conceptual section of this README.** Raft is designed so the merge primitives — what makes concurrent writes converge to the same result on every device — live inside the database. The network layer that actually moves bytes between devices is intentionally **outside** raft-db. You build it (your own backend), or you plug in [Relay](https://github.com/raft-db/relay) (a separate product), or you simply never wire one up — your single-device app works correctly without any of this.

### The mental model

```
┌─────────────────────────────────────────┐
│            Your Flutter app             │
├─────────────────────────────────────────┤
│              raft_db (Dart)             │
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

You don't interact with HLCs directly in the Dart API today — they're an implementation detail of the merge surface. They become observable when you implement a peer integration (see [Plugging in a peer](#plugging-in-a-peer)).

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

Today these enums are configured on the Rust side at schema creation time. A typed Dart configurator API ships alongside the schema builder work tracked separately.

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

The pattern:

```dart
// 1. Hook into the mutation stream (currently via the to-be-built observe
//    adapter — for now, emit your own pub/sub from app-level write helpers)
db.observeCollection('users').listen((event) async {
  if (event.origin == MutationOrigin.local) {
    final doc = await db.collectionGet('users', event.docId);
    await yourBackend.upsert('users', event.docId, doc);
  }
});

// 2. Apply incoming server writes back into Raft. The engine takes care
//    of HLC ordering / CRDT merge if you stamp the incoming write with
//    the server's HLC. For a `RemoteAuthority` collection, the value
//    you pass overwrites local. For `LocalFirst`, the CRDT merge runs.
yourBackend.subscribeChanges().listen((change) async {
  await db.collectionPut('users', change.documentJson);
});
```

The exact wire format and HLC propagation primitives are tracked separately — the Dart-side observe adapter and a typed `applyRemote` helper will land together.

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
- Local-only embedded database — open, read, write, query, transaction all work
- HLC timestamps stamped on every write
- CRDT merge logic active for any concurrent local writes (rare on a single device, but the same machinery would kick in for a peer)
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

### Offline-first user data

```dart
final dir = await getApplicationDocumentsDirectory();
final db = await RaftDb.open('${dir.path}/raft');
final users = db.collection<User>(
  name: 'users',
  serialize: (u) => Uint8List.fromList(utf8.encode(jsonEncode(u.toJson()))),
  deserialize: (b) => User.fromJson(jsonDecode(utf8.decode(b))),
);

// Writes always succeed locally — no spinner, no error if offline
await users.putAuto(User(name: 'Alice'));
```

### Caching network responses with a TTL

```dart
final raw = await db.get(utf8.encode('cache:/api/users'));
if (raw != null) {
  final entry = jsonDecode(utf8.decode(raw));
  if (DateTime.now().millisecondsSinceEpoch < entry['expires_at']) {
    return entry['body'];
  }
}

final body = await http.get('/api/users');
await db.put(
  utf8.encode('cache:/api/users'),
  utf8.encode(jsonEncode({
    'body': body,
    'expires_at': DateTime.now().millisecondsSinceEpoch + 60000,
  })),
);
```

### Atomic counter increments inside a transaction

```dart
await db.withTransaction((txn) async {
  final raw = await txn.get('counters', 1);
  final c = jsonDecode(utf8.decode(raw!));
  c['value'] = c['value'] + 1;
  await txn.put('counters',
      Uint8List.fromList(utf8.encode(jsonEncode(c))));
});
// If two isolates race, one of them gets a code-7 conflict and can retry.
```

### Per-user partitioning via raw-KV prefixes

```dart
String userKey(String userId, String field) => 'u:$userId:$field';

await db.put(utf8.encode(userKey('alice', 'theme')), utf8.encode('dark'));
final theme = utf8.decode((await db.get(utf8.encode(userKey('alice', 'theme'))))!);
```

---

## Troubleshooting

**"Database is closed" / `StateError`** — `RaftDb.close()` is one-shot. Open a fresh instance to keep using the same on-disk database.

**`code 6 InvalidJson`** — your `collectionPut` payload or query JSON didn't parse. The Rust side is strict; check for trailing commas, NaN values, or non-UTF-8 bytes.

**`code 7 TransactionConflict`** — another transaction modified a doc you read. Catch and retry, or restructure to read the smallest possible set of docs.

**Native binary not found on a custom platform** — `RaftDb.open` calls `DynamicLibrary.open('libraftdb.so')` on Linux/Android, `DynamicLibrary.process()` on iOS/macOS, and `raftdb.dll` on Windows. For exotic targets, file an issue with the platform details.

**Observation events not firing on Dart** — this is the known deferred case (see [Observation](#observation)). Use app-level pub/sub for now.

---

## Example app

See [`example/`](example/) for a runnable Flutter app that exercises the full API.

---

## License

Dual-licensed under Apache-2.0 and MIT. See `LICENSE` and `LICENSE-MIT`.
