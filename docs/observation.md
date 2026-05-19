# Observation

[← Back to docs index](README.md)

Raft exposes two observer surfaces that emit changes as they happen:

- **`observeCollection`** — fires per mutation (insert / update / delete) on a typed collection. Cheap; one event per write.
- **`observeQuery`** — fires a snapshot of the matching result set immediately, then a diff (`added` / `removed` / `updated`) every time the result set changes.

Both are backed by **real native callbacks** on Swift, Kotlin, and React Native — the C side invokes a function pointer synchronously from the Rust tokio worker thread. The platforms marshal that into idiomatic streams.

Dart is the exception: observe is currently deferred there because Dart's only safe cross-thread callback mechanism is asynchronous, and the Rust-side `CString` is freed when the synchronous call returns. The fix is a Rust-side `Dart_PostCObject_DL` adapter — tracked separately. The Dart API surface below is sketched for when it lands.

## Method matrix

### `observeCollection`

| Platform | Signature |
|---|---|
| Swift | `db.observe(collection: String) -> AsyncStream<MutationEvent>` |
| Kotlin | `db.observeCollection(collection: String): Flow<MutationEvent>` |
| Dart | `db.observeCollection(String collection) -> Stream<MutationEvent>` _(deferred)_ |
| TypeScript | `db.observeCollection(collection: string, cb: (event: MutationEvent) => void): () => void` |

### `observeQuery`

| Platform | Signature |
|---|---|
| Swift | `db.observeQuery(_ queryJson: Data) -> AsyncStream<QueryDiff>` |
| Kotlin | `db.observeQuery(queryJson: ByteArray): Flow<QueryDiff>` |
| Dart | `db.observeQuery(Uint8List queryJson) -> Stream<QueryDiff>` _(deferred)_ |
| TypeScript | `db.observeQuery<T>(queryJson: string, cb: (diff: QueryDiff<T>) => void): () => void` |

## Event shapes

### `MutationEvent`

```ts
{
  collection:    "users",
  doc_id:        42,
  mutation_type: "Insert" | "Update" | "Delete",
  origin:        "Local" | "Remote",
}
```

On Swift / Kotlin the enums are typed (`.insert`, `MutationKind.INSERT`); on TypeScript / Dart they're parsed from JSON strings.

`origin` is `Local` when the mutation originated on this device, `Remote` when it came in via a peer integration. Useful to avoid echo loops when implementing a peer adapter.

### `QueryDiff<T>`

```ts
{
  added:   T[]   // documents that now match but didn't
  removed: T[]   // documents that no longer match
  updated: T[]   // documents still matching but with changed fields
}
```

Decode each entry with your platform's JSON library. The diff is computed against the previous result set, so the **first emission is always all-added** (initial snapshot).

## Example: tail a collection

### Swift

```swift
let task = Task {
    for await event in db.observe(collection: "users") {
        switch event.mutationType {
        case .insert: print("inserted \(event.docId)")
        case .update: print("updated \(event.docId)")
        case .delete: print("deleted \(event.docId)")
        }
    }
}
// Cancel to unsubscribe
task.cancel()
```

### Kotlin

```kotlin
val job = scope.launch {
    db.observeCollection("users").collect { event ->
        when (event.mutationType) {
            MutationKind.INSERT -> println("inserted ${event.docId}")
            MutationKind.UPDATE -> println("updated ${event.docId}")
            MutationKind.DELETE -> println("deleted ${event.docId}")
        }
    }
}
// Cancel to unsubscribe
job.cancel()
```

### Dart (when wired)

```dart
final sub = db.observeCollection('users').listen((event) {
    switch (event.mutationType) {
        case MutationKind.insert:
            print('inserted ${event.docId}'); break;
        case MutationKind.update:
            print('updated ${event.docId}'); break;
        case MutationKind.delete:
            print('deleted ${event.docId}'); break;
    }
});
// Cancel to unsubscribe
await sub.cancel();
```

### TypeScript

```ts
const unsubscribe = db.observeCollection('users', (event) => {
    console.log(`${event.mutation_type} #${event.doc_id}`)
})
// Cancel to unsubscribe
unsubscribe()
```

## Example: live-query view-model

The result set of a query, kept in sync with the database. The "list" stays current as documents are added, removed, or updated.

### Swift (SwiftUI)

```swift
@MainActor
final class UserListViewModel: ObservableObject {
    @Published private(set) var users: [User] = []
    private var task: Task<Void, Never>?

    func start(db: RaftDB) {
        let query = try! JSONEncoder().encode(["collection": "users"])
        task = Task { [weak self] in
            for await diff in db.observeQuery(query) {
                guard let self else { return }
                await MainActor.run {
                    var current = self.users
                    // Apply diff.removed
                    let removed = Set(diff.removed.compactMap { try? JSONDecoder().decode(User.self, from: $0).id })
                    current.removeAll { removed.contains($0.id) }
                    // Apply diff.added + diff.updated
                    for raw in diff.added + diff.updated {
                        if let u = try? JSONDecoder().decode(User.self, from: raw) {
                            if let idx = current.firstIndex(where: { $0.id == u.id }) {
                                current[idx] = u
                            } else {
                                current.append(u)
                            }
                        }
                    }
                    self.users = current
                }
            }
        }
    }

    deinit { task?.cancel() }
}
```

### Kotlin (StateFlow)

```kotlin
class UserListViewModel(private val db: RaftDb) : ViewModel() {

    val users: StateFlow<List<User>> = db.observeQuery(allUsersQuery.toByteArray())
        .scan(emptyList<User>()) { current, diff ->
            val removedIds = diff.removed.map {
                Json.decodeFromString<User>(String(it)).id
            }.toSet()
            val updated = (diff.added + diff.updated).map {
                Json.decodeFromString<User>(String(it))
            }
            (current.filterNot { it.id in removedIds } + updated)
                .distinctBy { it.id }
        }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), emptyList())
}
```

### Dart (when wired)

```dart
class UserListController extends ChangeNotifier {
    UserListController(this.db) {
        _sub = db.observeQuery(queryJson).listen(_apply);
    }
    final RaftDb db;
    final List<User> users = [];
    late final StreamSubscription _sub;

    void _apply(QueryDiff diff) {
        final removedIds = diff.removed
            .map((r) => User.fromJson(jsonDecode(utf8.decode(r))).id)
            .toSet();
        users.removeWhere((u) => removedIds.contains(u.id));
        for (final raw in [...diff.added, ...diff.updated]) {
            final u = User.fromJson(jsonDecode(utf8.decode(raw)));
            final idx = users.indexWhere((x) => x.id == u.id);
            if (idx >= 0) {
                users[idx] = u;
            } else {
                users.add(u);
            }
        }
        notifyListeners();
    }

    @override
    void dispose() {
        _sub.cancel();
        super.dispose();
    }
}
```

### TypeScript (React hook)

```tsx
function useUsers(db: RaftDB): User[] {
    const [users, setUsers] = useState<User[]>([])
    useEffect(() => {
        const unsub = db.observeQuery<User>(
            JSON.stringify({ collection: 'users' }),
            (diff) => {
                setUsers(prev => {
                    const removed = new Set(diff.removed.map(u => u.id))
                    const filtered = prev.filter(u => !removed.has(u.id))
                    const updated = [...diff.added, ...diff.updated]
                    const merged = [...filtered]
                    for (const u of updated) {
                        const idx = merged.findIndex(x => x.id === u.id)
                        if (idx >= 0) merged[idx] = u
                        else merged.push(u)
                    }
                    return merged
                })
            }
        )
        return unsub
    }, [db])
    return users
}
```

## Example: peer integration via observe

When integrating a peer (your own backend or Relay), use `observeCollection` to tail local writes and push them upstream. Filter on `origin: Local` so server pushes don't echo back.

### Swift

```swift
Task {
    for await event in db.observe(collection: "users") {
        guard event.origin == .local else { continue }
        if let raw = try await db.collectionGet("users", id: event.docId) {
            try await yourBackend.upsert("users", id: event.docId, body: raw)
        } else if event.mutationType == .delete {
            try await yourBackend.delete("users", id: event.docId)
        }
    }
}
```

The same shape works on Kotlin (`Flow.filter { it.origin == LOCAL }`) and TypeScript (filter inside the callback).

## Subscription lifetimes

| Platform | How to cancel | What happens natively |
|---|---|---|
| Swift | Cancel the consuming `Task` | `onTermination` calls `rft_unobserve`, releases the retained `ObserveContext` |
| Kotlin | Cancel the collecting Job (or `Flow.take(...)`) | `awaitClose { nativeUnobserve(...) }` runs |
| Dart | `subscription.cancel()` | Same as Swift, via the dart:ffi adapter (when wired) |
| TypeScript | Call the returned `unsubscribe` function | Cleans up the JSI registration |

**Always cancel observers** when the consuming view-model / hook / scope ends. A leaked subscription holds a native callback + JNI/JS reference indefinitely.

## Edge cases

### Initial snapshot ordering

`observeQuery` emits the **initial snapshot synchronously** before returning — but as a regular `QueryDiff` with everything in `added`. There's no "initial vs. update" flag; treat the first emission like any other.

### Observer fires for the writer's own write

`observeCollection` fires for **every** insert / update / delete, including the writer's own. That's intentional — many UI patterns benefit from a single feedback path. If you want to skip self-writes, track them via app-level state or filter on `event.origin == .local` only when the peer adapter is wired.

### Lagging subscribers

The Rust core uses a `tokio::broadcast` channel with a per-subscriber receiver. If a subscriber falls behind by more than the channel capacity (default 1024 events), the Rust side emits `RecvError::Lagged` and the platforms silently skip those events to keep the stream live.

If you need precise reconciliation after a lag, re-query the collection and reconcile against the in-memory state. The query is index-aware and cheap.

### Two subscribers to the same collection

Each subscription is independent. Two streams over `"users"` each get a copy of every event. There's no broadcast deduplication.

### Cancel during emission

Cancelling a subscription during a callback execution is safe — the native side waits for the current callback to return before tearing down (Swift / Kotlin / RN). The Dart adapter (when wired) will follow the same convention.

## Performance notes

- **Broadcast cost is per-subscriber, not per-write.** A single subscriber sees ~µs latency end-to-end. With 100 subscribers, the writer thread serializes the JSON 100 times — keep subscriber count proportional to the UI.
- **Query observers re-evaluate the predicate on every mutation in the collection.** For complex predicates over hot collections, prefer a coarse observer + client-side filter.
- **JSON encoding dominates per-event cost.** A 500-byte document → ~50 µs encode + 30 µs FFI hop. Plan for ~10⁴ events/sec/subscriber before you saturate.

## Related

- [Typed collections](collections.md) — the surface observers operate on
- [Queries](queries.md) — `observeQuery` shares the same query JSON shape
- [Sync](sync.md) — `origin: Local` vs `origin: Remote`, and how observers help wire a peer
