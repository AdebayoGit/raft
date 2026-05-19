# react-native-raft

> Mobile-native embedded database for React Native, built with Nitro Modules over the Raft Rust core.

[![Version](https://img.shields.io/npm/v/react-native-raft.svg)](https://www.npmjs.com/package/react-native-raft)
[![Downloads](https://img.shields.io/npm/dm/react-native-raft.svg)](https://www.npmjs.com/package/react-native-raft)
[![License](https://img.shields.io/npm/l/react-native-raft.svg)](LICENSE)

Offline-first storage for RN apps with Promise-based APIs, live observers via JSI callbacks, optimistic transactions, and zero bridge round-trips. Raft is **local-first**: your local database is the source of truth, and any network sync is an optional layer on top.

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

```bash
bun add react-native-raft react-native-nitro-modules
# or: npm install / yarn add
```

iOS:

```bash
cd ios && pod install
```

Android: autolinked. Minimum requirements: React Native 0.76+, Node 18+.

---

## Quickstart

```ts
import { RaftDB } from 'react-native-raft'

interface User { id?: number; name: string; age: number }

async function example() {
  const db = RaftDB.open(`${RNFS.DocumentDirectoryPath}/raft`)

  // Typed collection — engine assigns the doc id
  const id = await db.collectionPutAuto('users',
    JSON.stringify({ name: 'Alice', age: 30 }))

  // Read
  const raw = await db.collectionGet('users', id)
  const alice: User = JSON.parse(raw!)

  db.close()
}
```

All FFI calls go through JSI (no bridge round-trips). Promise-returning methods are awaited like normal async code.

---

## Concepts

Raft has **two complementary storage surfaces**:

1. **Raw key-value** — the underlying LSM-tree store. Keys and values are `string` (UTF-8). Use when you control the id space (UUIDs, slugs, content hashes).
2. **Typed collections** — documents addressed by `number` (uint64 on the native side; JS Number gives ~2^53 of usable range, ample for a single device's id space). The engine assigns ids on `collectionPutAuto`, or you provide one via the document's `id` field. Typed collections enable indexed queries, change notifications, and the merge surface described in [Sync](#sync--local-merge-and-peer-integration).

The two surfaces address **different storage namespaces**. Putting a document via one and reading via the other will not work. Pick one per logical collection.

---

## API Reference

### Lifecycle

```ts
// Open or create a database on disk (synchronous)
const db = RaftDB.open('/path/to/db')

// Close releases the native handle. Safe to call multiple times.
db.close()
```

`RaftDB.open` does no network handshake — it's a pure local file open. The database is fully usable with zero network configuration.

### Raw key-value

```ts
await db.put('user:1', JSON.stringify({ name: 'Alice' }))
const value = await db.get('user:1')      // string | null
await db.delete('user:1')                  // returns the previous value or null
```

A two-phase read protocol queries the value size first, then reads into an exact-size buffer — no buffer-size limits to tune.

### Typed collections

```ts
// Auto-assigned id
const id = await db.collectionPutAuto('users',
  JSON.stringify({ name: 'Alice', age: 30 }))

// Caller-chosen id (the JSON's `id` field is honoured)
await db.collectionPut('users',
  JSON.stringify({ id: 42, name: 'Bob', age: 25 }))

// Read
const raw = await db.collectionGet('users', id)   // string | null

// Delete (no-op if id missing)
await db.collectionDelete('users', id)

// Cardinality
const count = await db.collectionCount('users')

// All ids (sorted ascending)
const ids: number[] = await db.collectionListIds('users')
```

For typed access, wrap the surface in your own repository class:

```ts
class UserRepo {
  constructor(private readonly db: RaftDB) {}

  async create(name: string, age: number): Promise<User> {
    const id = await this.db.collectionPutAuto('users',
      JSON.stringify({ name, age }))
    return { id, name, age }
  }

  async load(id: number): Promise<User | null> {
    const raw = await this.db.collectionGet('users', id)
    return raw ? (JSON.parse(raw) as User) : null
  }

  async delete(id: number): Promise<void> {
    await this.db.collectionDelete('users', id)
  }
}
```

### Queries

```ts
const queryJson = JSON.stringify({
  collection: 'users',
  where: { field: 'age', op: 'gte', value: 18 },
  limit: 100,
})

const results: string[] = await db.executeQuery(queryJson)
const users = results.map(s => JSON.parse(s) as User)
```

Query JSON shape is documented in the Rust core's `query` module. A typed builder will land alongside the schema work.

### Transactions

```ts
await db.withTransaction(async (txn) => {
  const raw = await txn.get('users', 42)
  if (raw === null) return
  const user = JSON.parse(raw) as User
  user.age += 1
  await txn.put('users', JSON.stringify(user))
})
// Committed if the callback returns; rolled back if it throws.
```

The transaction handle is **consumed** by `commit` or `rollback`. `withTransaction` handles both for you; you should not need to call them manually.

### Observation

`db.observeCollection(name, callback)` registers a native subscription via JSI callbacks. The callback fires synchronously on the native side and is marshalled to the JS thread by Nitro. Returns an unsubscribe function:

```ts
const unsubscribe = db.observeCollection('users', (event) => {
  // event: MutationEvent
  // event.mutation_type: 'Insert' | 'Update' | 'Delete'
  // event.collection: string
  // event.doc_id: number
  // event.origin: 'Local' | 'Remote'
  console.log(`${event.mutation_type} #${event.doc_id}`)
})

// later
unsubscribe()
```

Live query observers fire immediately with the initial snapshot, then again on every result-set change:

```ts
const unsub = db.observeQuery<User>(queryJson, (diff) => {
  // diff: QueryDiff<User>
  // diff.added:   User[]
  // diff.removed: User[]
  // diff.updated: User[]
})

unsub()
```

The legacy raw-KV `watch` observer is still available for prefix-based change notifications on the raw-KV namespace:

```ts
const unsubKv = db.watch('user:', (result) => {
  // result: { key: string, value: string | undefined }
})
```

### Errors

All native failures reject the returned Promise (or throw synchronously where applicable) with an `Error` whose `message` includes the C error code. You can match by message substring or by parsing the code:

| Code | Meaning |
|---|---|
| 1 | NullPointer — internal, shouldn't surface |
| 2 | InvalidUtf8 — a string argument wasn't valid UTF-8 |
| 3 | IoError — storage engine I/O failure |
| 4 | NotFound — mapped to `null` returns; never thrown |
| 5 | BufferTooSmall — internal, retried automatically |
| 6 | InvalidJson — a document or query JSON failed to parse |
| 7 | TransactionConflict — a tracked document was modified concurrently |
| 8 | InvalidHandle — a freed transaction/result/subscription was reused |
| 9 | UnknownSubscription — `unwatch` called with an unknown id |

```ts
try {
  await db.withTransaction(async (txn) => { /* ... */ })
} catch (e) {
  if (String(e).includes('code 7')) {
    // Conflict — re-read and retry
  } else {
    throw e
  }
}
```

---

## Sync — local merge and peer integration

**This is the most important conceptual section of this README.** Raft is designed so the merge primitives — what makes concurrent writes converge to the same result on every device — live inside the database. The network layer that actually moves bytes between devices is intentionally **outside** raft-db. You build it (your own backend), or you plug in [Relay](https://github.com/raft-db/relay) (a separate product), or you simply never wire one up — your single-device app works correctly without any of this.

### The mental model

```
┌─────────────────────────────────────────┐
│         Your React Native app           │
├─────────────────────────────────────────┤
│  react-native-raft  (TS + Nitro JSI)    │
├─────────────────────────────────────────┤
│   HybridRaft.swift / HybridRaft.kt      │
│            ↓                            │
│   libraftdb.{so,dylib} (Rust core)      │
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

You don't interact with HLCs directly in the JS API today — they're an implementation detail of the merge surface. They become observable when you implement a peer integration (see [Plugging in a peer](#plugging-in-a-peer)).

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

Today these enums are configured on the Rust side at schema creation time. A typed JS configurator API ships alongside the schema builder work tracked separately.

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

```ts
// 1. Tail the mutation stream and push local writes
const unsub = db.observeCollection('users', async (event) => {
  if (event.origin !== 'Local') return
  const doc = await db.collectionGet('users', event.doc_id)
  if (doc) await yourBackend.upsert('users', event.doc_id, doc)
})

// 2. Apply incoming server writes back into Raft
yourBackend.onChange(async (change) => {
  await db.collectionPut('users', change.documentJson)
})

// Cleanup on app teardown
return () => { unsub() }
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

### React hook for a typed collection

```tsx
function useCollection<T>(db: RaftDB, name: string): {
  items: T[]
  insert: (doc: Omit<T, 'id'>) => Promise<number>
  remove: (id: number) => Promise<void>
} {
  const [items, setItems] = useState<T[]>([])

  useEffect(() => {
    // Initial load via query
    db.executeQuery(JSON.stringify({ collection: name }))
      .then(rows => setItems(rows.map(r => JSON.parse(r) as T)))

    // Stay in sync via observer
    const unsub = db.observeCollection(name, async () => {
      const rows = await db.executeQuery(JSON.stringify({ collection: name }))
      setItems(rows.map(r => JSON.parse(r) as T))
    })
    return unsub
  }, [db, name])

  return {
    items,
    insert: async (doc) => db.collectionPutAuto(name, JSON.stringify(doc)),
    remove: async (id) => db.collectionDelete(name, id),
  }
}
```

### Atomic counter with retry-on-conflict

```ts
async function increment(db: RaftDB, counterId: number, retries = 5): Promise<void> {
  for (let i = 0; i < retries; i++) {
    try {
      await db.withTransaction(async (txn) => {
        const raw = await txn.get('counters', counterId)
        if (!raw) return
        const c = JSON.parse(raw)
        c.value += 1
        await txn.put('counters', JSON.stringify(c))
      })
      return
    } catch (e) {
      if (!String(e).includes('code 7')) throw e
      await new Promise(r => setTimeout(r, 50 * (1 << i)))
    }
  }
}
```

### Caching network responses with a TTL (raw-KV)

```ts
async function cachedGet(db: RaftDB, url: string): Promise<string> {
  const raw = await db.get(`cache:${url}`)
  if (raw) {
    const entry = JSON.parse(raw) as { body: string; expiresAt: number }
    if (Date.now() < entry.expiresAt) return entry.body
  }
  const body = await fetch(url).then(r => r.text())
  await db.put(`cache:${url}`, JSON.stringify({ body, expiresAt: Date.now() + 60_000 }))
  return body
}
```

### Per-user partitioning via raw-KV prefixes

```ts
const userKey = (userId: string, field: string) => `u:${userId}:${field}`

await db.put(userKey('alice', 'theme'), 'dark')
const theme = await db.get(userKey('alice', 'theme'))
```

---

## Troubleshooting

**`UnsatisfiedLinkError` on Android** — `libraftdb.so` isn't being packaged. Rebuild with `cd android && ./gradlew clean assembleDebug`. Check that `node_modules/react-native-raft/android/src/main/jniLibs/<abi>/libraftdb.so` exists.

**iOS build error: `'rft_open' is undeclared`** — `pod install` didn't pick up the static library. Try `cd ios && pod deintegrate && pod install`.

**`TypeError: undefined is not a function (RaftHybrid.collectionPut)`** — the Nitro module wasn't regenerated after a spec change. Run `bun run nitrogen` (or equivalent) and rebuild the native binaries.

**Observation callback never fires** — verify the database is open and the collection name matches exactly. The callback runs on the JS thread; if you're in a tight loop it may be starved.

**`code 7` in transactions** — another transaction modified a doc you read. Retry with exponential backoff (see [Use case recipes](#use-case-recipes)).

**Number precision** — JS Number is 53-bit; doc ids beyond `2^53` will lose precision. Practically this is only relevant if you import ids from another system with a higher counter.

---

## License

Dual-licensed under Apache-2.0 and MIT. See `LICENSE` and `LICENSE-MIT`.
