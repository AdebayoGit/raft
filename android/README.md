# raftdb — Android (Kotlin)

> Mobile-native embedded database for Android. Idiomatic Kotlin over the Raft Rust core.

[![Maven Central](https://img.shields.io/maven-central/v/com.raftdb/raftdb.svg)](https://central.sonatype.com/artifact/com.raftdb/raftdb)

Offline-first storage with `suspend` operations, `Flow`-based observers, optimistic transactions, and a JNI bridge to the Raft core. Raft is **local-first**: your local database is the source of truth, and any network sync is an optional layer on top.

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

`build.gradle.kts`:

```kotlin
dependencies {
    implementation("com.raftdb:raftdb:0.0.1")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.0")
}
```

`min-sdk` 21. The artifact bundles `libraftdb.so` + `libraftdb-jni.so` (the JNI shim that exports `Java_com_raftdb_RaftDb_*` symbols) for `arm64-v8a`, `armeabi-v7a`, and `x86_64`. ABI splits are honoured.

---

## Quickstart

```kotlin
import com.raftdb.RaftDb
import com.raftdb.RaftCollection
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.encodeToString

@Serializable
data class User(val id: Long = 0, val name: String, val age: Int)

suspend fun example(context: Context) {
    val db = RaftDb.open("${context.filesDir}/raft")
    val users = RaftCollection<User>(
        db = db,
        name = "users",
        serialize = { Json.encodeToString(it).toByteArray() },
        deserialize = { Json.decodeFromString(String(it)) },
    )

    val id = users.putAuto(User(name = "Alice", age = 30))
    val alice = users.getById(id)        // User?
    users.deleteById(id)

    db.close()
}
```

All blocking JNI calls run on `Dispatchers.IO`; the suspending API is safe to call from any coroutine context.

---

## Concepts

Raft has **two complementary storage surfaces**:

1. **Raw key-value** — the underlying LSM-tree store. Keys and values are arbitrary `ByteArray`. Use when you control the id space (UUIDs, slugs, content hashes).
2. **Typed collections** — documents addressed by `Long` (uint64 on the native side). The engine assigns ids on `collectionPutAuto`, or you provide one via the document's `id` field. Typed collections enable indexed queries, change notifications, and the merge surface described in [Sync](#sync--local-merge-and-peer-integration).

`RaftCollection<T>` wraps either surface and handles serialization. Use raw-KV when you want to control the key shape yourself, typed collections when you want the engine to do the bookkeeping.

The two surfaces address **different storage namespaces**. Putting a document via one and reading via the other will not work. Pick one per logical collection.

---

## API Reference

### Lifecycle

```kotlin
// Open or create a database on disk
val db = RaftDb.open("/data/data/com.example/files/raft")

// Synchronous variant for non-coroutine contexts
val db2 = RaftDb.openBlocking(path)

// Close releases the native handle. Safe to call multiple times.
db.close()
```

`RaftDb.open` does no network handshake — it's a pure local file open. The database is fully usable with zero network configuration.

### Raw key-value

```kotlin
db.put("user:1".toByteArray(), """{"name":"Alice"}""".toByteArray())

val bytes: ByteArray? = db.get("user:1".toByteArray())
bytes?.let { val user = Json.decodeFromString<User>(String(it)) }

db.delete("user:1".toByteArray())  // no-op if absent
```

A two-phase read protocol queries the value size first, then reads into an exact-size buffer — no buffer-size limits to tune.

### Typed collections

```kotlin
// Auto-assigned id
val id: Long = db.collectionPutAuto(
    "users",
    Json.encodeToString(User(name = "Alice", age = 30)).toByteArray(),
)

// Caller-chosen id (the JSON's `id` field is honoured)
db.collectionPut(
    "users",
    Json.encodeToString(User(id = 42, name = "Bob", age = 25)).toByteArray(),
)

// Read
val raw: ByteArray? = db.collectionGet("users", id)

// Delete (no-op if id missing)
db.collectionDelete("users", id)

// Cardinality
val count: Long = db.collectionCount("users")

// All ids (sorted ascending)
val ids: LongArray = db.collectionListIds("users")
```

For typed serialization, use `RaftCollection<T>`:

```kotlin
val users = RaftCollection<User>(
    db = db,
    name = "users",
    serialize = { Json.encodeToString(it).toByteArray() },
    deserialize = { Json.decodeFromString(String(it)) },
)

val aliceId: Long = users.putAuto(User(name = "Alice", age = 30))
val alice: User? = users.getById(aliceId)
val count: Long = users.count()
val ids: LongArray = users.listIds()
```

`RaftCollection<T>` also exposes a legacy String-id raw-KV surface (`put(id: String, doc)`, `get(id: String)`, `delete(id: String)`) for callers who control their key namespace. These address a different storage namespace from the typed methods.

### Queries

```kotlin
val queryJson = """
{
  "collection": "users",
  "where": {"field": "age", "op": "gte", "value": 18},
  "limit": 100
}
""".trimIndent()

val results: List<ByteArray> = db.executeQuery(queryJson.toByteArray())
for (docBytes in results) {
    val user = Json.decodeFromString<User>(String(docBytes))
}
```

Query JSON shape is documented in the Rust core's `query` module. A typed builder will land alongside the schema work.

### Transactions

```kotlin
db.withTransaction { txn ->
    val raw = txn.get("users", 42L) ?: return@withTransaction
    val user = Json.decodeFromString<User>(String(raw))
    val updated = user.copy(age = user.age + 1)
    txn.put("users", Json.encodeToString(updated).toByteArray())
}
// Committed if the lambda returns; rolled back if it throws.
```

Or manually:

```kotlin
val txn = db.beginTransaction()
try {
    txn.put("users", Json.encodeToString(user).toByteArray())
    txn.commit()                  // throws RaftError.TransactionConflict on conflict
} catch (e: RaftError.TransactionConflict) {
    // retry
}
```

The transaction handle is **consumed** by `commit` or `rollback`. Calling `commit` twice throws `RaftError.InvalidHandle`; rolling back after commit is a no-op.

### Observation

`RaftDb.observeCollection(name)` returns a cold `Flow<MutationEvent>` backed by a real native callback (JNI trampoline with `AttachCurrentThread`). Collecting from the flow registers a native subscription; cancelling the collection unregisters it.

```kotlin
val job = launch {
    db.observeCollection("users").collect { event ->
        when (event.mutationType) {
            MutationKind.INSERT -> println("inserted ${event.docId}")
            MutationKind.UPDATE -> println("updated ${event.docId}")
            MutationKind.DELETE -> println("deleted ${event.docId}")
        }
        // event.origin: LOCAL or REMOTE
    }
}
// later
job.cancel()  // unobserves the native subscription
```

Live query observers fire immediately with the initial snapshot, then again on every result-set change:

```kotlin
launch {
    db.observeQuery(queryJson.toByteArray()).collect { diff ->
        for (addedBytes in diff.added) {
            val user = Json.decodeFromString<User>(String(addedBytes))
        }
        for (removedBytes in diff.removed) { /* ... */ }
        for (updatedBytes in diff.updated) { /* ... */ }
    }
}
```

`RaftCollection<T>` exposes the same as `users.observe(): Flow<MutationEvent>` and `users.liveQuery(json): Flow<QueryDiff>`.

### Errors

All native failures throw a typed `RaftError` subclass:

| Code | Class | Meaning |
|---|---|---|
| 1 | `RaftError.NullPointer` | Internal — should not surface in user code |
| 2 | `RaftError.InvalidUtf8` | A string argument wasn't valid UTF-8 |
| 3 | `RaftError.IoError` | Storage engine I/O failure |
| 4 | (not thrown) | Mapped to `null` returns |
| 5 | `RaftError.BufferTooSmall` | Internal, retried automatically |
| 6 | `RaftError.InvalidJson` | A document or query JSON failed to parse |
| 7 | `RaftError.TransactionConflict` | A tracked document was modified concurrently |
| 8 | `RaftError.InvalidHandle` | A freed transaction/result/subscription was reused |
| 9 | `RaftError.UnknownSubscription` | `unobserve` called with an unknown id |

```kotlin
try {
    db.withTransaction { txn -> /* ... */ }
} catch (e: RaftError.TransactionConflict) {
    // re-read and retry
}
```

---

## Sync — local merge and peer integration

**This is the most important conceptual section of this README.** Raft is designed so the merge primitives — what makes concurrent writes converge to the same result on every device — live inside the database. The network layer that actually moves bytes between devices is intentionally **outside** raft-db. You build it (your own backend), or you plug in [Relay](https://github.com/raft-db/relay) (a separate product), or you simply never wire one up — your single-device app works correctly without any of this.

### The mental model

```
┌─────────────────────────────────────────┐
│            Your Android app             │
├─────────────────────────────────────────┤
│           com.raftdb (Kotlin)           │
├─────────────────────────────────────────┤
│  libraftdb-jni.so  →  libraftdb.so      │
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

You don't interact with HLCs directly in the Kotlin API today — they're an implementation detail of the merge surface. They become observable when you implement a peer integration (see [Plugging in a peer](#plugging-in-a-peer)).

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

Today these enums are configured on the Rust side at schema creation time. A typed Kotlin configurator API ships alongside the schema builder work tracked separately.

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

```kotlin
// 1. Tail the mutation stream and push local writes
scope.launch {
    db.observeCollection("users").collect { event ->
        if (event.origin == MutationOrigin.LOCAL) {
            val doc = db.collectionGet("users", event.docId) ?: return@collect
            yourBackend.upsert("users", event.docId, doc)
        }
    }
}

// 2. Apply incoming server writes back into Raft
scope.launch {
    yourBackend.subscribeChanges().collect { change ->
        db.collectionPut("users", change.documentJson)
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

### Repository pattern with a `RaftCollection`

```kotlin
class UserRepository(private val db: RaftDb) {
    private val users = RaftCollection<User>(
        db = db,
        name = "users",
        serialize = { Json.encodeToString(it).toByteArray() },
        deserialize = { Json.decodeFromString(String(it)) },
    )

    suspend fun create(name: String, age: Int): User {
        val tentative = User(name = name, age = age)
        val id = users.putAuto(tentative)
        return tentative.copy(id = id)
    }

    suspend fun update(user: User) = users.putById(user.id, user)
    suspend fun load(id: Long): User? = users.getById(id)
    suspend fun delete(id: Long) = users.deleteById(id)
    fun stream(): Flow<MutationEvent> = users.observe()
}
```

### Atomic counter increments via transaction

```kotlin
suspend fun increment(db: RaftDb, counterId: Long) {
    var retries = 0
    while (retries < 5) {
        try {
            db.withTransaction { txn ->
                val raw = txn.get("counters", counterId) ?: return@withTransaction
                val c = Json.decodeFromString<Counter>(String(raw))
                txn.put("counters",
                    Json.encodeToString(c.copy(value = c.value + 1)).toByteArray())
            }
            return
        } catch (e: RaftError.TransactionConflict) {
            retries++
            delay((50L shl retries))  // exponential backoff
        }
    }
}
```

### Observing in a `ViewModel`

```kotlin
class UserListViewModel(private val db: RaftDb) : ViewModel() {
    val users: StateFlow<List<User>> = db.observeQuery(allUsersQuery.toByteArray())
        .map { diff -> /* maintain a local list, applying added/removed/updated */ }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), emptyList())
}
```

### Caching network responses with a TTL (raw-KV)

```kotlin
suspend fun cachedGet(url: String): String? {
    db.get("cache:$url".toByteArray())?.let {
        val entry = Json.decodeFromString<CacheEntry>(String(it))
        if (System.currentTimeMillis() < entry.expiresAt) return entry.body
    }
    val body = http.get(url)
    db.put(
        "cache:$url".toByteArray(),
        Json.encodeToString(CacheEntry(body, System.currentTimeMillis() + 60_000)).toByteArray(),
    )
    return body
}
```

---

## Troubleshooting

**`UnsatisfiedLinkError`** — the JNI shim is missing. Ensure your APK ships both `libraftdb.so` AND `libraftdb-jni.so` for the device's ABI. The Maven artifact bundles both; if you build from source, run the CMake step under `src/main/cpp/`.

**`RaftError.InvalidJson`** — your `collectionPut` payload or query JSON didn't parse. The Rust side is strict; check for trailing commas, NaN values, or non-UTF-8 bytes.

**`RaftError.TransactionConflict`** — another transaction modified a doc you read. Catch and retry, or restructure to read the smallest possible set of docs. Use exponential backoff for hot keys.

**Flow cancellation doesn't unregister the observer** — the cancellation runs `awaitClose { nativeUnobserve(...) }` synchronously; if it doesn't seem to take effect, check that you're cancelling the Job rather than the Flow itself.

**ProGuard / R8 strips JNI methods** — keep `com.raftdb.**` and `com.raftdb.RaftDb$Companion` so the JVM doesn't rename the `nativeXxx` declarations.

---

## License

Dual-licensed under Apache-2.0 and MIT. See `LICENSE` and `LICENSE-MIT`.
