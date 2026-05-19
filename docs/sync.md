# Sync — the merge surface

[← Back to docs index](README.md)

This is the conceptual heart of Raft.

When you only have one device, a database is straightforward — writes are total-ordered by arrival time, reads observe the latest write, conflicts don't exist. When two devices write to the same data, you need to answer a hard question: **how do these writes combine?**

Raft's answer lives in the **merge surface** — a set of primitives baked into the database that make concurrent writes converge to the same result on every device, deterministically and without coordination. The merge surface is always present in Raft. The network layer that actually moves bytes between devices — sometimes called "sync" colloquially — is **not** part of raft-db. You provide it (your own backend, or [Relay](https://github.com/raft-db/relay)).

This doc explains the merge surface in depth, the knobs it exposes, and how to wire a peer to it.

## Contents

- [The mental model](#the-mental-model)
- [Why CRDTs (and not "just last-write-wins")](#why-crdts-and-not-just-last-write-wins)
- [Hybrid Logical Clocks](#hybrid-logical-clocks)
- [CRDTs](#crdts)
  - [LWW Register](#lww-register)
  - [OR-Set](#or-set)
  - [Counter](#counter)
- [`SyncAuthority`](#syncauthority)
- [`ConflictStrategy`](#conflictstrategy)
- [Composing authority and strategy](#composing-authority-and-strategy)
- [Plugging in a peer](#plugging-in-a-peer)
  - [Pattern A — Your own backend](#pattern-a--your-own-backend)
  - [Pattern B — Relay](#pattern-b--relay)
- [Worked examples](#worked-examples)
- [Current state and limits](#current-state-and-limits)

---

## The mental model

```
┌─────────────────────────────────────────┐
│              Your app                   │
├─────────────────────────────────────────┤
│           Platform bindings             │
│   (Swift / Kotlin / Dart / TypeScript)  │
├─────────────────────────────────────────┤
│           libraftdb (Rust core)         │
│                                         │
│   ┌─────────────────────────────────┐   │
│   │  Mutation log                   │   │← every write stamped (HLC, device_id)
│   │  Document store + indexes       │   │
│   │  ConflictStrategy + Authority   │   │← the merge surface
│   │  CRDT primitives                │   │
│   └─────────────────────────────────┘   │
└────────────▲────────────────────────────┘
             │
             │  optional, external:
             │
  ┌──────────┴──────────────────────────────┐
  │  Your backend  /  Relay  /  Nothing     │ ← network not part of raft-db
  └─────────────────────────────────────────┘
```

The crucial point: **every write goes through the merge surface, even when no peer exists.** Single-device apps see the surface as a no-op (no concurrent writes to merge), but the metadata (HLC, device id) is always stamped. The moment a peer arrives, the merge rules kick in automatically.

This design has a payoff and a cost:

- **Payoff**: adding a peer later is "wire the I/O", not "rebuild the data model". The merge semantics are already correct.
- **Cost**: every doc carries some merge metadata (per-field for CRDTs, per-write for LWW). For simple single-device apps you can opt out via `LastWriteWins` everywhere; for serious multi-device apps you need the metadata anyway.

## Why CRDTs (and not "just last-write-wins")

Last-write-wins sounds simple: latest timestamp wins, done. It works fine until you hit any of:

1. **Two devices write the same field "at the same time."** Wall-clock ties happen often (NTP precision is ~10ms; users tap fast). With LWW, whichever timestamp is microscopically later wins and the other write vanishes silently.

2. **A device is offline for a week, comes back, syncs.** Its writes are "older" by wall clock. Strict LWW discards them. The user just lost a week of work.

3. **Sets that diverge.** Device A adds tag `urgent` while device B removes tag `urgent`. LWW gives you one or the other based on a microsecond — not "the user wanted both ops to count."

4. **Counters.** Device A increments by 1, device B increments by 1. LWW gives you `+1`, not `+2`.

CRDTs solve these specifically:

- **OR-Set** preserves both the add and the remove as intent records, then resolves by causality. Add-after-remove leaves the element present; remove-of-an-old-add removes it. No silent loss.
- **Counter** records *each device's per-device increment* separately. The total is the sum across devices. Both `+1`s count.
- **HLC-based LWW** uses logical time, not wall clock. Causally later writes always beat causally earlier ones, regardless of which clock thinks it's later.

You don't have to use them for everything — `LastWriteWins` is a per-field option, perfect for fields where "one of two concurrent writes is fine." But for fields where dropping a write is a bug, CRDTs are the answer.

## Hybrid Logical Clocks

Wall-clock timestamps fail under three classes of clock weirdness:

- **NTP corrections** (clock jumps backwards a few seconds)
- **Clock drift** (devices in different time zones, dual-boot machines)
- **Same-ms ties** (two writes at the same millisecond don't have a stable order)

Pure logical clocks (Lamport timestamps) fix the order problem but lose physical time — you can't ask "did this happen this morning?"

**Hybrid Logical Clocks** combine both:

```
HlcTimestamp = (physical_ms: u64, logical: u16)
```

Comparison is tuple-lexicographic: `(p1, l1) < (p2, l2)` iff `p1 < p2`, or `p1 == p2 && l1 < l2`.

**The update rule** (executed every time the device emits or receives a write):

```
on local write:
    physical = max(wall_clock_ms, last_hlc.physical)
    if physical == last_hlc.physical:
        logical = last_hlc.logical + 1
    else:
        logical = 0
    last_hlc = (physical, logical)

on receive (remote_hlc):
    new_physical = max(wall_clock_ms, last_hlc.physical, remote_hlc.physical)
    new_logical = case:
        new_physical == last_hlc.physical == remote_hlc.physical:
            max(last_hlc.logical, remote_hlc.logical) + 1
        new_physical == last_hlc.physical:
            last_hlc.logical + 1
        new_physical == remote_hlc.physical:
            remote_hlc.logical + 1
        else:
            0
    last_hlc = (new_physical, new_logical)
```

Properties:

- **Monotonicity** — `last_hlc` only ever grows.
- **Causality** — if event A causally precedes event B (B "saw" A), then `hlc(A) < hlc(B)`.
- **Bounded skew** — `physical_ms` is bounded by `max(any_device_wall_clock + max_skew)`, so HLCs don't drift unbounded into the future even when devices have wildly wrong clocks.
- **Total order** — combined with `device_id` as final tiebreaker, every pair of writes in the system has a deterministic order.

You don't interact with HLCs from the platform APIs today. They're an implementation detail of the merge surface. When you implement a peer integration, the HLC of an incoming write tells you whether it's newer or older than your local state — but the engine already does that comparison for you when you call `collectionPut`.

## CRDTs

### LWW Register

A single value with a `(HlcTimestamp, device_id)` stamp.

**Merge rule:** keep the version with the higher `(HLC, device_id)`. HLC ties break on device id.

```
local:  ("Alice",  hlc=10, device=A)
remote: ("Alyce",  hlc=12, device=B)
merge:  ("Alyce",  hlc=12, device=B)   ← remote is newer

local:  ("Alice",  hlc=10, device=A)
remote: ("Alyce",  hlc=10, device=B)
merge:  ("Alyce",  hlc=10, device=B)   ← HLC tie; B > A by device id
```

**Properties:**

- Commutative: merging in any order yields the same result.
- Idempotent: merging the same value twice is a no-op.
- Associative: `merge(merge(a, b), c) == merge(a, merge(b, c))`.

These three properties together (commutativity, idempotency, associativity) are the formal definition of a CRDT — they're what guarantee convergence regardless of order or repetition.

**Default for:** all scalar field types (`String`, `Int`, `Float`, `Bool`, `Bytes`, `Reference`).

**Storage cost:** `value + (HLC, device_id)` per field. ~24 bytes overhead per field.

**Use when:** "the most recent write wins, but don't drop concurrent writes if they're at different logical times."

### OR-Set

An *Observed-Remove Set*. Each `add` records a tag `(device_id, hlc)`; `remove` records that a specific tag is no longer live. An element is **present** iff at least one of its add-tags is not in the removed set.

```
device A: add "urgent"      → tag (A, 10)         → set: {"urgent"@(A,10)}
device B: add "urgent"      → tag (B, 11)         → set: {"urgent"@(A,10), "urgent"@(B,11)}
device A: remove "urgent"   → tombstones (A,10)   → set still contains (B,11)
                                                  → "urgent" still PRESENT
```

That's the classic "concurrent add wins over remove" property. The add by B happened with no knowledge of A's remove, so it counts.

**Merge rule:** union the add-tags, union the tombstones. An element is present iff at least one add-tag for it isn't tombstoned.

**Properties:**

- Commutative, idempotent, associative.
- "Add wins" semantics — explicit additions concurrent with removes survive.
- Each element has its own add/remove history; multiple writers don't interfere with each other's intent.

**Default for:** `Collection` field type.

**Storage cost:** O(n × distinct_tags) per set. Tombstones accumulate, but compaction is straightforward (drop tombstones whose tag is older than all live add-tags for the element).

**Use when:** the field is a set / list of elements that multiple devices can add or remove independently — tags, group members, permissions, attached files.

### Counter

A **PN-Counter** (positive-negative). Each device maintains its own `(p_device, n_device)` running totals; the global count is `sum(p_*) - sum(n_*)` over all devices.

```
device A:  +3   →  p_A = 3
device B:  +5   →  p_B = 5
device A:  -1   →  n_A = 1
total:          (3 + 5) - (1 + 0) = 7
```

**Merge rule:** for each device, keep `max(local.p_d, remote.p_d)` and `max(local.n_d, remote.n_d)`. The per-device totals are monotonic; max gives you the highest observed value.

**Properties:**

- Commutative, idempotent, associative.
- All increments and decrements count — none are dropped.
- Concurrency-safe: two devices incrementing simultaneously both contribute to the total.

**Storage cost:** O(devices_that_have_ever_written) per counter.

**Use when:** the value is fundamentally a tally — likes, view counts, inventory deltas, vote totals.

## `SyncAuthority`

Per-**collection** declaration that tells the merge surface how to resolve conflicts when a peer's write meets a local write. Three modes:

| Mode | Local writes | Conflict resolution | Read path |
|---|---|---|---|
| `LocalFirst` _(default)_ | Always applied; HLC-stamped | CRDT merge — no data loss | Local |
| `RemoteAuthority` | Provisional | Remote unconditionally overwrites local | Local |
| `RemoteFirst` | Always applied | CRDT merge same as `LocalFirst` | Remote if reachable, local fallback |

### `LocalFirst` (default)

Your local writes are immediately durable and visible. When a remote write for the same doc arrives, the merge surface applies per-field CRDT rules to produce the converged state.

**Use when:** the local device is at least as authoritative as any peer. User profile fields, draft state, app settings.

### `RemoteAuthority`

Your local writes are stored locally **provisionally** — they're visible until a remote write for the same doc arrives, at which point the remote write **wins unconditionally** (no CRDT merge, no HLC comparison). Use this when the peer is the source of truth and local edits are caches or pending submissions.

```
local write at t=10:  balance = 100
remote write at t=5:  balance = 50
result: balance = 50      ← remote wins even though it's "older"
```

**Use when:** server-managed data that the client shouldn't really be editing — feature flags, balances, pricing, inventory counts.

### `RemoteFirst`

CRDT merge on writes (same as `LocalFirst`), but **reads** try the remote first. If the device is online and the peer is reachable, the read returns the freshest remote value; otherwise it falls back to local.

**Use when:** the data is collaborative and the freshness matters more than latency. Chat messages, shared document state.

> **Note:** the `RemoteFirst` read-path behaviour requires a peer that supports "fetch by id." Without one, `RemoteFirst` degrades to `LocalFirst` semantics.

### Without a peer

With **zero network code**:
- `LocalFirst` works perfectly (all CRDT machinery still runs)
- `RemoteFirst` behaves identically to `LocalFirst` (no peer to consult on reads)
- `RemoteAuthority` is effectively `LocalFirst` (no remote writes to override local)

You can ship a `RemoteAuthority` collection in a single-device app without breaking anything. When you wire a peer later, the semantics activate.

## `ConflictStrategy`

Per-**field** declaration that gives finer control than `SyncAuthority`. The strategy overrides the default CRDT choice for a field.

| Strategy | Storage cost | Data safety | Typical use |
|---|---|---|---|
| `Crdt(LwwRegister)` | Low | No data loss for scalar overwrites | Names, descriptions |
| `Crdt(OrSet)` | Medium (per-device tags) | No data loss for set ops | Tags, group members |
| `Crdt(Counter)` | Low (per-device count) | No data loss for +/- ops | Likes, view counts |
| `LastWriteWins` | Lowest | Silently drops concurrent writes | Fields where one-of-two is fine |
| `ServerAuthority` | Lowest | Discards conflicting local writes | Inventory, balances, server-set fields |
| `Custom(id)` | Medium | Developer-controlled | Domain-specific merges |

### `Crdt(kind)`

Use the CRDT primitive of the given kind. `kind` is one of `LwwRegister`, `OrSet`, `Counter`. Default for fields based on their type — strings default to `Crdt(LwwRegister)`, collections to `Crdt(OrSet)`, etc.

### `LastWriteWins`

Strict last-write-wins by HLC + device id, **without** preserving per-write metadata for older writes. Cheapest option — the entire field is a single value, no overhead. Concurrent writes deterministically resolve, but one of them is dropped.

**Use when:** the field's history doesn't matter and "one of two" is acceptable. Examples: `last_login_at`, `theme_preference`, `language`.

### `ServerAuthority`

The peer's write **always** wins, regardless of HLC. Local writes are tentative; the next remote write erases conflicting local state. Effectively `RemoteAuthority` at the field level.

**Use when:** the field is server-managed. Examples: `balance`, `inventory_count`, `subscription_tier`, `pricing_cents`.

### `Custom(id)`

A developer-provided merge function. You register a closure keyed by `id` at startup; the engine calls it when this field has a conflict and both `local` and `remote` values are available. Returns the merged value, or surfaces the conflict to the application.

**Use when:** the merge logic is domain-specific — e.g. "always keep the longer string", "merge two arrays with deduplication", "prefer the value from the device with the higher trust score."

The custom resolver API (`ConflictResolver::register`) is currently Rust-side only. A platform-facing API is on the roadmap.

## Composing authority and strategy

`SyncAuthority` is the collection-level **default**; `ConflictStrategy` is the field-level **override**.

```
collection: users  (SyncAuthority::LocalFirst)
  field name        — Crdt(LwwRegister)     ← uses default for String
  field tags        — Crdt(OrSet)            ← uses default for Collection
  field last_seen   — LastWriteWins          ← override: don't care about losing one
  field balance     — ServerAuthority        ← override: server controls this
  field score       — Crdt(Counter)          ← override: a String type with counter semantics
```

When the engine resolves a conflict:

1. Look up the field's `ConflictStrategy`. If `Custom(id)`, invoke the resolver. If `ServerAuthority`, return remote.
2. Otherwise the `Crdt(kind)` rule applies.
3. The collection's `SyncAuthority` provides the *fallback* for the field if no explicit `ConflictStrategy` is set.

So `SyncAuthority::RemoteAuthority` is shorthand for "every field defaults to `ServerAuthority`." `SyncAuthority::LocalFirst` is shorthand for "every field uses its type's default CRDT."

## Plugging in a peer

Raft doesn't include a network layer, but it gives you the hooks to build one. A peer is anything that:

1. **Tails local writes** and pushes them to a server
2. **Receives remote writes** and applies them to the local database

Step 1 is the [`observeCollection`](observation.md) surface — tail every mutation, filter by `origin: Local`, push.

Step 2 is just `collectionPut` (for typed) or `put` (for raw KV) — the merge surface kicks in automatically based on the collection's authority and the field strategies.

### Pattern A — Your own backend

You already have a server. Add a per-collection sync adapter on the client.

#### Swift

```swift
final class UserSyncAdapter {
    private let db: RaftDB
    private let api: YourAPIClient
    private var tailTask: Task<Void, Never>?

    init(db: RaftDB, api: YourAPIClient) {
        self.db = db
        self.api = api
    }

    func start() {
        tailTask = Task {
            // Push local writes upstream
            for await event in db.observe(collection: "users") {
                guard event.origin == .local else { continue }
                do {
                    if event.mutationType == .delete {
                        try await api.delete("users", id: event.docId)
                    } else if let raw = try await db.collectionGet("users", id: event.docId) {
                        try await api.upsert("users", id: event.docId, body: raw)
                    }
                } catch {
                    // Retry later — see "Backoff and recovery" below
                }
            }
        }

        Task {
            // Apply remote writes locally
            for await change in api.subscribeChanges("users") {
                try await db.collectionPut("users", document: change.documentJson)
            }
        }
    }

    func stop() { tailTask?.cancel() }
}
```

#### Kotlin

```kotlin
class UserSyncAdapter(private val db: RaftDb, private val api: YourApiClient) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    fun start() {
        scope.launch {
            db.observeCollection("users").collect { event ->
                if (event.origin != MutationOrigin.LOCAL) return@collect
                if (event.mutationType == MutationKind.DELETE) {
                    api.delete("users", event.docId)
                } else {
                    db.collectionGet("users", event.docId)?.let { raw ->
                        api.upsert("users", event.docId, raw)
                    }
                }
            }
        }

        scope.launch {
            api.subscribeChanges("users").collect { change ->
                db.collectionPut("users", change.documentJson)
            }
        }
    }

    fun stop() = scope.cancel()
}
```

#### Dart (when observe is wired)

```dart
class UserSyncAdapter {
    UserSyncAdapter({required this.db, required this.api});
    final RaftDb db;
    final YourApiClient api;

    StreamSubscription? _localSub;
    StreamSubscription? _remoteSub;

    void start() {
        _localSub = db.observeCollection('users').listen((event) async {
            if (event.origin != MutationOrigin.local) return;
            if (event.mutationType == MutationKind.delete) {
                await api.delete('users', event.docId);
            } else {
                final raw = await db.collectionGet('users', event.docId);
                if (raw != null) await api.upsert('users', event.docId, raw);
            }
        });

        _remoteSub = api.subscribeChanges('users').listen((change) async {
            await db.collectionPut('users', change.documentJson);
        });
    }

    Future<void> stop() async {
        await _localSub?.cancel();
        await _remoteSub?.cancel();
    }
}
```

#### TypeScript

```ts
class UserSyncAdapter {
    private unsubLocal?: () => void
    private unsubRemote?: () => void

    constructor(private db: RaftDB, private api: YourApiClient) {}

    start() {
        this.unsubLocal = this.db.observeCollection('users', async (event) => {
            if (event.origin !== 'Local') return
            if (event.mutation_type === 'Delete') {
                await this.api.delete('users', event.doc_id)
            } else {
                const raw = await this.db.collectionGet('users', event.doc_id)
                if (raw) await this.api.upsert('users', event.doc_id, raw)
            }
        })

        this.unsubRemote = this.api.subscribeChanges('users', async (change) => {
            await this.db.collectionPut('users', change.documentJson)
        })
    }

    stop() {
        this.unsubLocal?.()
        this.unsubRemote?.()
    }
}
```

#### Backoff and recovery

The patterns above push **best-effort**. For production:

- Wrap pushes in retry-with-backoff. On `5xx`, retry with jitter; on `4xx`, log and skip.
- Persist a "high-water mark" (e.g. the last successfully-pushed `(collection, doc_id, hlc)`) so you can resume after a crash.
- On startup, **first** drain the remote-subscribe stream (so you have the latest), **then** tail local writes from the high-water mark forward.

The exact contract depends on your backend's API.

### Pattern B — Relay

[Relay](https://github.com/raft-db/relay) is a separate product that gives you a Redis-style network sync layer. The client side becomes:

```swift
let relay = try await Relay.connect("wss://relay.example.com", clientId: deviceId)
try await relay.attach(db: db, collections: ["users", "posts"])
// Done. Relay handles both directions transparently.
```

Relay is **not** part of raft-db — it consumes the merge surface this doc describes.

## Worked examples

### A user profile with mixed strategies

```
collection: users   (SyncAuthority::LocalFirst)
  field name        — Crdt(LwwRegister)
  field email       — Crdt(LwwRegister)
  field bio         — Crdt(LwwRegister)
  field tags        — Crdt(OrSet)
  field balance     — Crdt(Counter), ServerAuthority   ← server controls totals
  field tier        — ServerAuthority                  ← server controls plan
  field last_seen   — LastWriteWins                    ← cosmetic; one-of-two is fine
  field draft_post  — Crdt(LwwRegister)                ← user's draft, local-first
```

#### Two devices both edit the bio

- Device A at HLC 100, device id A: bio = "Cool dev"
- Device B at HLC 102, device id B: bio = "Awesome dev"
- Sync → merge: bio = "Awesome dev" (B is later by HLC)

#### Both add the same tag

- Device A: tags.add("rust"), tag = (A, 105)
- Device B: tags.add("rust"), tag = (B, 106)
- Sync → both tags present, "rust" stays in the set

#### A locks the user but the server pushes a new tier

- Device A locally: tier = "pro" (HLC 110)
- Server pushes: tier = "free" (HLC 50)
- Sync → tier = "free" (ServerAuthority wins regardless of HLC)

### A counter (likes on a post)

```
collection: posts
  field likes — Crdt(Counter)
```

- Device A (offline): like × 3 → p_A = 3
- Device B (offline): like × 5 → p_B = 5
- Both reconnect → likes = p_A + p_B = 8

No like is lost. The counter is exact; concurrent writes accumulate.

### A feature-flag collection

```
collection: feature_flags   (SyncAuthority::RemoteAuthority)
  field enabled — LastWriteWins
  field rollout — LastWriteWins
```

- The local device caches flags for offline reads.
- Any local write is **provisional** — the next server push overrides.
- This is "don't let the client meaningfully edit this" enforced at the collection level.

### A presence collection (ephemeral)

```
collection: presence   (SyncAuthority::LocalFirst)
  field last_active — LastWriteWins
  field status      — LastWriteWins
```

Frequent low-value updates. CRDT overhead would be wasteful. LWW is correct because losing one of two updates 50ms apart doesn't matter.

### Collaborative editing

For each shared document:

```
collection: documents   (SyncAuthority::LocalFirst)
  field title  — Crdt(LwwRegister)
  field tags   — Crdt(OrSet)
  field body   — Custom("CRDT-aware-text-merge")
```

For `body`, register a `Custom` resolver that performs a CRDT-aware string merge (e.g. Yjs / Automerge text). The engine routes conflicts on this field to your function.

## Current state and limits

### What works today, with zero network code

- Local-only embedded database (open, read, write, query, transaction, observe)
- HLC timestamps stamped on every write
- CRDT merge logic active for any concurrent local writes
- Schema-declared `SyncAuthority` validated and applied
- Schema-declared `ConflictStrategy` validated and applied
- Mutation log preserved as the source of truth
- `MutationEvent.origin` set to `Local` for all in-process writes

### What requires a peer to be useful

- `SyncAuthority::RemoteAuthority` and `RemoteFirst` semantics — degrade to `LocalFirst` without a peer
- The `RemoteFirst` read-through-network behaviour
- `ConflictStrategy::ServerAuthority` — needs a server-stamped remote write to override local
- Any actual cross-device data movement (obviously)

### What is explicitly out of scope for raft-db

- The wire protocol for sending writes to a peer
- A sync engine that pushes/pulls deltas
- A reference sync server
- Conflict resolution UI (showing the user "these don't agree, pick one")

Those live in Relay (or your own backend). raft-db gives you the merge primitives so whichever peer you choose can be a thin layer over `observeCollection` + `collectionPut`.

### Roadmap notes

- A platform-facing `Custom` resolver registration API (Swift / Kotlin / Dart / TS callbacks called by the Rust resolver). Currently Rust-side only.
- An `applyRemote(documentJson, hlc, deviceId)` helper that lets a peer adapter pass through the merge surface explicitly with the server's HLC instead of letting the engine restamp.
- Dart observe wiring (the only platform where observe is currently deferred — see [observation.md](observation.md)).

## Related

- [Observation](observation.md) — how to tail local mutations into a peer adapter
- [Typed collections](collections.md) — what the merge surface operates on
- [Transactions](transactions.md) — orthogonal to merge; both work together
- [Errors](errors.md) — `InvalidJson` if a remote write doesn't match the schema
