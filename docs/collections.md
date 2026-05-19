# Typed collections

[← Back to docs index](README.md)

The document-oriented surface. Documents are JSON, addressed by **`uint64` doc ids** that the engine assigns (or you provide via the document's `id` field). Collections enable:

- Schema-aware validation (when a schema is defined on the Rust side)
- Indexed queries via [`executeQuery`](queries.md)
- Live mutation observers ([`observeCollection`](observation.md))
- Per-field merge semantics ([`ConflictStrategy`](sync.md#conflictstrategy))

Use typed collections when you want the engine to manage ids, you want change notifications, or you'll attach a peer later. Use [raw KV](raw-kv.md) when you control the key shape.

## Method matrix

All collection methods are on the database handle. `<C>` is the collection name (a `String`); `<id>` is the doc id (`uint64` on the wire).

| Operation | Swift | Kotlin | Dart | TypeScript |
|---|---|---|---|---|
| Put (id from doc) | `collectionPut(_:document:)` | `collectionPut(c, json)` | `collectionPut(c, json)` | `collectionPut(c, json)` |
| Put-auto (engine id) | `collectionPutAuto(_:document:) -> UInt64` | `collectionPutAuto(c, json): Long` | `collectionPutAuto(c, json) -> int` | `collectionPutAuto(c, json) -> Promise<number>` |
| Get | `collectionGet(_:id:) -> Data?` | `collectionGet(c, id): ByteArray?` | `collectionGet(c, id) -> Uint8List?` | `collectionGet(c, id) -> Promise<string \| null>` |
| Delete | `collectionDelete(_:id:)` | `collectionDelete(c, id)` | `collectionDelete(c, id)` | `collectionDelete(c, id)` |
| Count | `collectionCount(_:) -> Int` | `collectionCount(c): Long` | `collectionCount(c) -> int` | `collectionCount(c) -> Promise<number>` |
| List ids | `collectionListIds(_:) -> [UInt64]` | `collectionListIds(c): LongArray` | `collectionListIds(c) -> List<int>` | `collectionListIds(c) -> Promise<number[]>` |

Notes:

- `collectionPut` requires the document JSON to contain an integer `id` field. The engine **overwrites** any existing document with that id and bumps the internal version.
- `collectionPutAuto` ignores any `id` in the JSON and assigns the next id from a per-collection counter. The assigned id is returned.
- `collectionGet` returns the raw JSON bytes — you decode them per platform.
- Deleting an absent doc is **not an error**.
- `listIds` returns ids in **ascending order**.

## Convenience wrappers

Each platform ships a `RaftCollection<T>` wrapper that handles `Codable` / `kotlinx.serialization` / JSON encoding for a single type. Use it when one collection maps to one struct.

### Swift

```swift
struct User: Codable, Sendable {
    var id: UInt64 = 0
    let name: String
    let age: Int
}

let users = RaftCollection<User>(db: db, name: "users")

let id = try await users.putAuto(User(name: "Alice", age: 30))
let alice: User? = try await users.get(docId: id)
let count = try await users.count()
let ids: [UInt64] = try await users.listIds()
try await users.delete(docId: id)
```

### Kotlin

```kotlin
@Serializable
data class User(val id: Long = 0, val name: String, val age: Int)

val users = RaftCollection<User>(
    db = db,
    name = "users",
    serialize = { Json.encodeToString(it).toByteArray() },
    deserialize = { Json.decodeFromString(String(it)) },
)

val id = users.putAuto(User(name = "Alice", age = 30))
val alice: User? = users.getById(id)
val count: Long = users.count()
val ids: LongArray = users.listIds()
users.deleteById(id)
```

### Dart

```dart
class User {
    User({required this.name, required this.age, this.id = 0});
    final int id;
    final String name;
    final int age;

    Map<String, dynamic> toJson() => {'id': id, 'name': name, 'age': age};
    factory User.fromJson(Map<String, dynamic> j) =>
        User(id: j['id'], name: j['name'], age: j['age']);
}

final users = db.collection<User>(
    name: 'users',
    serialize: (u) => Uint8List.fromList(utf8.encode(jsonEncode(u.toJson()))),
    deserialize: (b) => User.fromJson(jsonDecode(utf8.decode(b))),
);

final id = await users.putAuto(User(name: 'Alice', age: 30));
final alice = await users.getById(id);     // User?
final count = await users.count();
final ids = await users.listIds();         // List<int>
await users.deleteById(id);
```

### TypeScript

```ts
interface User { id?: number; name: string; age: number }

// No bundled wrapper — write a thin class
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

    async count(): Promise<number> {
        return this.db.collectionCount('users')
    }

    async listIds(): Promise<number[]> {
        return this.db.collectionListIds('users')
    }
}
```

## Example: cross-platform — create, update, list

A single workflow shown identically across the four platforms.

### Swift

```swift
let id1 = try await users.putAuto(User(name: "Alice", age: 30))
let id2 = try await users.putAuto(User(name: "Bob", age: 25))

// Update Alice — she's id1, the JSON must carry the same id
try await users.put(docId: id1, document: User(id: id1, name: "Alice Cooper", age: 30))

let all = try await users.listIds()
for id in all {
    if let user = try await users.get(docId: id) {
        print("\(id): \(user.name) age=\(user.age)")
    }
}
```

### Kotlin

```kotlin
val id1 = users.putAuto(User(name = "Alice", age = 30))
val id2 = users.putAuto(User(name = "Bob", age = 25))

users.putById(id1, User(id = id1, name = "Alice Cooper", age = 30))

for (id in users.listIds()) {
    users.getById(id)?.let { user ->
        println("$id: ${user.name} age=${user.age}")
    }
}
```

### Dart

```dart
final id1 = await users.putAuto(User(name: 'Alice', age: 30));
final id2 = await users.putAuto(User(name: 'Bob', age: 25));

await users.putById(id1, User(id: id1, name: 'Alice Cooper', age: 30));

for (final id in await users.listIds()) {
    final user = await users.getById(id);
    if (user != null) {
        print('$id: ${user.name} age=${user.age}');
    }
}
```

### TypeScript

```ts
const id1 = await repo.create('Alice', 30)
const id2 = await repo.create('Bob', 25)

await db.collectionPut('users', JSON.stringify({
    id: id1.id, name: 'Alice Cooper', age: 30,
}))

for (const id of await repo.listIds()) {
    const user = await repo.load(id)
    if (user) console.log(`${id}: ${user.name} age=${user.age}`)
}
```

## Example: bulk import with a transaction

When importing many documents, wrap them in a transaction so the read-set is empty and there's only one fsync.

### Swift

```swift
try await db.withTransaction { txn in
    for record in csvRows {
        let doc = User(name: record.name, age: record.age)
        try txn.put(collection: "users", document: try JSONEncoder().encode(doc))
    }
}
```

### Kotlin

```kotlin
db.withTransaction { txn ->
    for (record in csvRows) {
        val doc = User(name = record.name, age = record.age)
        txn.put("users", Json.encodeToString(doc).toByteArray())
    }
}
```

### Dart

```dart
await db.withTransaction((txn) async {
    for (final record in csvRows) {
        await txn.put('users', Uint8List.fromList(utf8.encode(jsonEncode({
            'name': record.name, 'age': record.age,
        }))));
    }
});
```

### TypeScript

```ts
await db.withTransaction(async (txn) => {
    for (const record of csvRows) {
        await txn.put('users', JSON.stringify({
            name: record.name, age: record.age,
        }))
    }
})
```

> Note: `transaction.put` requires the JSON to carry an `id` field for non-auto inserts. For auto-id bulk import, call `collectionPutAuto` outside a transaction in a loop — the txn surface is for batches that need atomicity, not for hiding the write path.

## Example: schema-aware migrations

When a Rust-side schema is defined for a collection, `collectionPut` validates the document shape and returns `InvalidJson` (code 6) if a required field is missing. The platforms surface this as a typed error — see [errors.md](errors.md#invalidjson-code-6).

To add a new field, define it on the Rust side as optional, then write a one-off migration that reads every doc, fills in the new field, and writes it back. Run that inside a single transaction so the migration is atomic.

### Swift

```swift
try await db.withTransaction { txn in
    for id in try await users.listIds() {
        guard let raw = try txn.get(collection: "users", id: id) else { continue }
        var user = try JSONDecoder().decode(LegacyUser.self, from: raw)
        let migrated = User(
            id: user.id, name: user.name, age: user.age,
            timezone: "UTC",     // new field default
        )
        try txn.put(collection: "users", document: try JSONEncoder().encode(migrated))
    }
}
```

The same pattern applies to the other three platforms — read, transform, write, atomically.

## Edge cases

### Putting a doc with an unknown `id` field

`collectionPut` accepts any `id` you give it — there's no foreign-key check. If you pass `id: 0` (or omit it) and the collection counter is at 5, the new doc lives at id `0` (not `6`). To let the engine assign, use `collectionPutAuto`.

### Reading after delete in the same transaction

Inside a transaction, the read-after-delete returns `null` (the buffered delete is reflected to subsequent reads in the same txn). Outside a transaction, deletes only become visible after the underlying write returns.

### Two collections with the same name

Collection namespace is global to the database file. Opening "users" twice gives you the same underlying storage. There's no per-collection isolation beyond the name.

### Doc id range

Doc ids are `uint64`. The platforms type them as:

- Swift: `UInt64`
- Kotlin: `Long` (signed; ids beyond `2^63 - 1` would alias to negative, but the assigner is single-counter and starts at 1)
- Dart: `int` (JS Number safe range: `2^53`)
- TypeScript: `number` (same `2^53` ceiling)

In practice the assigner never gets past `2^53` per device. If you import ids from another system with a higher counter, prefer Swift / Kotlin for accuracy.

## Performance notes

- **`collectionPutAuto` is one atomic op** — it touches the WAL + MemTable + the doc-id counter index. Throughput is similar to raw KV.
- **`collectionCount` is O(1)** — the engine maintains a live counter per collection. Don't worry about calling it on hot paths.
- **`listIds` is O(n)** — it scans the doc-id index for the collection. For large collections (~10⁵+ docs) pair it with a query that already filters before reading.
- **A collection isn't a separate file** — all collections share the same LSM-tree. The "isolation" is logical (the prefix), not physical.

## Related

- [Raw KV](raw-kv.md) — alternative surface for caller-controlled key shapes
- [Queries](queries.md) — execute predicate queries against typed collections
- [Observation](observation.md) — `observeCollection` and `observeQuery` for live updates
- [Transactions](transactions.md) — atomic multi-doc updates
- [Sync](sync.md) — per-collection authority and per-field strategy
