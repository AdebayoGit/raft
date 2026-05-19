# Raw key-value

[← Back to docs index](README.md)

The byte-addressed surface. Maps `bytes → bytes` directly on the LSM-tree storage engine, with no schema, no indexing, and no merge metadata.

Use raw KV when you:

- control the key space yourself (UUIDs, slugs, hash digests, prefix-partitioned keys)
- store opaque blobs (cached HTTP responses, encoded thumbnails, serialized state)
- want a smaller per-entry footprint than the typed-collection surface (no doc-id index, no per-field CRDT metadata)

Don't use raw KV when you need:

- **engine-assigned ids** — see [collections.md](collections.md)
- **indexed queries** — see [queries.md](queries.md)
- **per-collection or per-field merge semantics** — see [sync.md](sync.md). Raw KV has only LWW-on-the-whole-value semantics.

## Method matrix

| Operation | Swift | Kotlin | Dart | TypeScript |
|---|---|---|---|---|
| Write | `put(key: Data, value: Data) async throws` | `put(key: ByteArray, value: ByteArray)` (suspend) | `put(Uint8List key, Uint8List value)` (Future) | `put(key: string, value: string): Promise<void>` |
| Read | `get(key: Data) async throws -> Data?` | `get(key: ByteArray): ByteArray?` (suspend) | `get(Uint8List key)` (Future of `Uint8List?`) | `get(key: string): Promise<string \| null>` |
| Delete | `delete(key: Data) async throws` | `delete(key: ByteArray)` (suspend) | `delete(Uint8List key)` (Future) | `delete(key: string): Promise<string \| null>` (returns prior value) |

Notes:

- **Swift / Kotlin / Dart** accept arbitrary byte buffers. **TypeScript** uses `string` for ergonomics; under the hood the strings cross the FFI as UTF-8.
- **Delete is idempotent.** Deleting an absent key writes a tombstone but doesn't error.
- **Read uses a two-phase protocol** internally: the first call queries the value size, the second reads into an exact-size buffer. No fixed-size truncation surprises.

## Example: store and read a JSON blob

### Swift

```swift
let key = Data("user:1".utf8)
let value = try JSONEncoder().encode(["name": "Alice", "age": 30])

try await db.put(key: key, value: value)

if let raw = try await db.get(key: key) {
    let user = try JSONDecoder().decode([String: AnyCodable].self, from: raw)
}

try await db.delete(key: key)
```

### Kotlin

```kotlin
val key = "user:1".toByteArray()
val value = Json.encodeToString(mapOf("name" to "Alice", "age" to 30)).toByteArray()

db.put(key, value)

db.get(key)?.let { raw ->
    val user = Json.decodeFromString<Map<String, Any>>(String(raw))
}

db.delete(key)
```

### Dart

```dart
final key = utf8.encode('user:1');
final value = utf8.encode(jsonEncode({'name': 'Alice', 'age': 30}));

await db.put(key, value);

final raw = await db.get(key);
if (raw != null) {
    final user = jsonDecode(utf8.decode(raw));
}

await db.delete(key);
```

### TypeScript

```ts
const key = 'user:1'
const value = JSON.stringify({ name: 'Alice', age: 30 })

await db.put(key, value)

const raw = await db.get(key)
if (raw !== null) {
    const user = JSON.parse(raw)
}

const previous = await db.delete(key)   // returns prior value or null
```

## Example: prefix-partitioned per-user keys

Use a prefix convention to organize keys without typed collections. Keep the prefix scheme stable — the LSM-tree colocates writes that share a prefix, which keeps reads in a single SSTable block when the working set is hot.

### Swift

```swift
func userKey(_ userId: String, _ field: String) -> Data {
    Data("u:\(userId):\(field)".utf8)
}

try await db.put(key: userKey("alice", "theme"), value: Data("dark".utf8))
try await db.put(key: userKey("alice", "locale"), value: Data("en_US".utf8))

let theme = try await db.get(key: userKey("alice", "theme"))
    .flatMap { String(data: $0, encoding: .utf8) }
```

### Kotlin

```kotlin
fun userKey(userId: String, field: String) = "u:$userId:$field".toByteArray()

db.put(userKey("alice", "theme"), "dark".toByteArray())
db.put(userKey("alice", "locale"), "en_US".toByteArray())

val theme = db.get(userKey("alice", "theme"))?.toString(Charsets.UTF_8)
```

### Dart

```dart
Uint8List userKey(String userId, String field) =>
    utf8.encode('u:$userId:$field') as Uint8List;

await db.put(userKey('alice', 'theme'), utf8.encode('dark'));
await db.put(userKey('alice', 'locale'), utf8.encode('en_US'));

final raw = await db.get(userKey('alice', 'theme'));
final theme = raw == null ? null : utf8.decode(raw);
```

### TypeScript

```ts
const userKey = (userId: string, field: string) => `u:${userId}:${field}`

await db.put(userKey('alice', 'theme'), 'dark')
await db.put(userKey('alice', 'locale'), 'en_US')

const theme = await db.get(userKey('alice', 'theme'))
```

## Example: cache with a TTL

Raw KV is the natural fit for caches because:

- the key shape is yours to design (`cache:<url>`, `etag:<resource>`)
- value lifetime is encoded inside the value (no separate index)
- you control eviction (delete the key, or write a fresh entry)

### Swift

```swift
struct CacheEntry: Codable {
    let body: Data
    let expiresAt: Date
}

func cachedGet(_ url: URL) async throws -> Data {
    let key = Data("cache:\(url.absoluteString)".utf8)
    if let raw = try await db.get(key: key) {
        let entry = try JSONDecoder().decode(CacheEntry.self, from: raw)
        if entry.expiresAt > Date() { return entry.body }
    }
    let (body, _) = try await URLSession.shared.data(from: url)
    let entry = CacheEntry(body: body, expiresAt: Date().addingTimeInterval(60))
    try await db.put(key: key, value: try JSONEncoder().encode(entry))
    return body
}
```

### Kotlin

```kotlin
@Serializable data class CacheEntry(val body: String, val expiresAt: Long)

suspend fun cachedGet(url: String): String {
    val key = "cache:$url".toByteArray()
    db.get(key)?.let {
        val entry = Json.decodeFromString<CacheEntry>(String(it))
        if (System.currentTimeMillis() < entry.expiresAt) return entry.body
    }
    val body = http.get(url)
    db.put(key, Json.encodeToString(CacheEntry(body, System.currentTimeMillis() + 60_000)).toByteArray())
    return body
}
```

### Dart

```dart
Future<String> cachedGet(String url) async {
    final key = utf8.encode('cache:$url');
    final raw = await db.get(key);
    if (raw != null) {
        final entry = jsonDecode(utf8.decode(raw));
        if (DateTime.now().millisecondsSinceEpoch < entry['expiresAt']) {
            return entry['body'] as String;
        }
    }
    final response = await http.get(Uri.parse(url));
    await db.put(key, utf8.encode(jsonEncode({
        'body': response.body,
        'expiresAt': DateTime.now().millisecondsSinceEpoch + 60000,
    })));
    return response.body;
}
```

### TypeScript

```ts
async function cachedGet(url: string): Promise<string> {
    const key = `cache:${url}`
    const raw = await db.get(key)
    if (raw) {
        const entry = JSON.parse(raw) as { body: string; expiresAt: number }
        if (Date.now() < entry.expiresAt) return entry.body
    }
    const body = await fetch(url).then(r => r.text())
    await db.put(key, JSON.stringify({ body, expiresAt: Date.now() + 60_000 }))
    return body
}
```

## Edge cases

### Empty values vs missing keys

An empty value (zero bytes) is **distinct** from a missing key. The two-phase read returns `bufferTooSmall + length=0` for a present-but-empty value, which the platforms map to an empty buffer/string. A missing key returns `null` / `nil` / `undefined`.

| Scenario | Swift | Kotlin | Dart | TypeScript |
|---|---|---|---|---|
| Key absent | `nil` | `null` | `null` | `null` |
| Key present, empty | `Data()` (count 0) | `byteArrayOf()` | `Uint8List(0)` | `""` |

If your app uses zero-length values as a sentinel, be careful not to conflate them with absence.

### Concurrent writes to the same key

Within a single device, the LSM-tree serializes writes. Concurrent calls from different isolates / threads to the same key end up with one of the two values — there's no merge logic at the raw-KV layer because the value is opaque bytes.

If you need merge semantics on a single key, model it as a typed collection with a single document and let the per-field [`ConflictStrategy`](sync.md#conflictstrategy) do the work.

### Large values

Values up to 16 MiB are well-supported. Larger values (>64 MiB) will work but reduce compaction efficiency. For binary blobs that big, prefer external storage (filesystem, blob store) and reference them by key.

## Performance notes

- **Reads are O(log n) in the number of SSTable levels** — bloom filters short-circuit most non-matching levels.
- **Writes go to the WAL + MemTable**, returning before the SSTable flush. The fsync policy is per-flush, not per-write.
- **Range scans** are not exposed on the raw KV API today. Use typed collections + `listIds` or queries instead.
- **Prefix grouping helps spatial locality** — keys sharing a prefix are likely to compact into the same SSTable block. For per-user data, prefer `u:<id>:<field>` over `<field>:<id>`.

## Related

- [Typed collections](collections.md) — if you want engine-assigned ids and indexed queries
- [Errors](errors.md) — error codes returned from `put` / `get` / `delete`
- [Sync](sync.md) — why the raw-KV surface has no merge metadata
