# Queries

[← Back to docs index](README.md)

The query surface executes JSON-encoded predicate queries against a typed collection. Queries don't traverse the raw-KV namespace.

There is **no SQL parser** in Raft. The query API is structured JSON, hand-validated by the engine, executed by an index-aware planner. The reasons:

- Compile-time-checked typed builders are easier to layer on top of structured JSON than on top of a string DSL.
- No SQL injection class of bug at the FFI boundary.
- The planner can choose secondary indexes (B-tree / hash) without parsing arithmetic precedence.

## Query JSON shape

```json
{
  "collection": "users",
  "where": {
    "field": "age",
    "op":    "gte",
    "value": 18
  },
  "order_by": [
    { "field": "age",  "asc": false },
    { "field": "name", "asc": true  }
  ],
  "limit":  100,
  "offset": 0
}
```

Field-level predicates support `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`, `prefix`. Logical composition uses `and` / `or`:

```json
{
  "collection": "users",
  "where": {
    "and": [
      { "field": "age",     "op": "gte",    "value": 18 },
      { "or": [
        { "field": "country", "op": "eq", "value": "US" },
        { "field": "country", "op": "eq", "value": "CA" }
      ] }
    ]
  }
}
```

The Rust core's `query` module is the canonical reference for the full predicate vocabulary; this doc lists the most common operators.

## Method matrix

| Operation | Swift | Kotlin | Dart | TypeScript |
|---|---|---|---|---|
| Execute | `executeQuery(_ queryJson: Data) async throws -> [Data]` | `executeQuery(queryJson: ByteArray): List<ByteArray>` (suspend) | `executeQuery(Uint8List queryJson) -> Future<List<Uint8List>>` | `executeQuery(queryJson: string): Promise<string[]>` |

Returns one entry per matching document; each entry is the document's raw JSON bytes (or string on RN).

## Example: find adults

### Swift

```swift
let query: [String: Any] = [
    "collection": "users",
    "where": ["field": "age", "op": "gte", "value": 18],
    "limit": 100,
]
let queryData = try JSONSerialization.data(withJSONObject: query)

let results = try await db.executeQuery(queryData)
for raw in results {
    let user = try JSONDecoder().decode(User.self, from: raw)
    print(user.name)
}
```

### Kotlin

```kotlin
val query = """
{
  "collection": "users",
  "where": {"field": "age", "op": "gte", "value": 18},
  "limit": 100
}
""".trimIndent()

val results = db.executeQuery(query.toByteArray())
for (raw in results) {
    val user = Json.decodeFromString<User>(String(raw))
    println(user.name)
}
```

### Dart

```dart
final query = jsonEncode({
    'collection': 'users',
    'where': {'field': 'age', 'op': 'gte', 'value': 18},
    'limit': 100,
});

final results = await db.executeQuery(Uint8List.fromList(utf8.encode(query)));
for (final raw in results) {
    final user = User.fromJson(jsonDecode(utf8.decode(raw)));
    print(user.name);
}
```

### TypeScript

```ts
const query = JSON.stringify({
    collection: 'users',
    where: { field: 'age', op: 'gte', value: 18 },
    limit: 100,
})

const results = await db.executeQuery(query)
for (const raw of results) {
    const user = JSON.parse(raw) as User
    console.log(user.name)
}
```

## Example: paginated list

### Swift

```swift
func loadPage(_ offset: Int, _ limit: Int) async throws -> [User] {
    let q: [String: Any] = [
        "collection": "users",
        "order_by": [["field": "created_at", "asc": false]],
        "limit": limit,
        "offset": offset,
    ]
    let data = try JSONSerialization.data(withJSONObject: q)
    return try await db.executeQuery(data).map {
        try JSONDecoder().decode(User.self, from: $0)
    }
}
```

### Kotlin

```kotlin
suspend fun loadPage(offset: Int, limit: Int): List<User> {
    val q = """
    {
      "collection": "users",
      "order_by": [{"field": "created_at", "asc": false}],
      "limit": $limit,
      "offset": $offset
    }
    """.trimIndent()
    return db.executeQuery(q.toByteArray()).map {
        Json.decodeFromString<User>(String(it))
    }
}
```

### Dart

```dart
Future<List<User>> loadPage(int offset, int limit) async {
    final q = jsonEncode({
        'collection': 'users',
        'order_by': [{'field': 'created_at', 'asc': false}],
        'limit': limit,
        'offset': offset,
    });
    final rows = await db.executeQuery(Uint8List.fromList(utf8.encode(q)));
    return rows.map((r) => User.fromJson(jsonDecode(utf8.decode(r)))).toList();
}
```

### TypeScript

```ts
async function loadPage(offset: number, limit: number): Promise<User[]> {
    const q = JSON.stringify({
        collection: 'users',
        order_by: [{ field: 'created_at', asc: false }],
        limit, offset,
    })
    const rows = await db.executeQuery(q)
    return rows.map(r => JSON.parse(r) as User)
}
```

## Example: composite predicate

Find users in the US or Canada, aged 18+, sorted by name.

### Swift

```swift
let q: [String: Any] = [
    "collection": "users",
    "where": [
        "and": [
            ["field": "age", "op": "gte", "value": 18],
            ["or": [
                ["field": "country", "op": "eq", "value": "US"],
                ["field": "country", "op": "eq", "value": "CA"],
            ]],
        ],
    ],
    "order_by": [["field": "name", "asc": true]],
]
```

### Kotlin

```kotlin
val q = """
{
  "collection": "users",
  "where": {
    "and": [
      {"field": "age", "op": "gte", "value": 18},
      {"or": [
        {"field": "country", "op": "eq", "value": "US"},
        {"field": "country", "op": "eq", "value": "CA"}
      ]}
    ]
  },
  "order_by": [{"field": "name", "asc": true}]
}
""".trimIndent()
```

### Dart

```dart
final q = jsonEncode({
    'collection': 'users',
    'where': {
        'and': [
            {'field': 'age', 'op': 'gte', 'value': 18},
            {'or': [
                {'field': 'country', 'op': 'eq', 'value': 'US'},
                {'field': 'country', 'op': 'eq', 'value': 'CA'},
            ]},
        ],
    },
    'order_by': [{'field': 'name', 'asc': true}],
});
```

### TypeScript

```ts
const q = JSON.stringify({
    collection: 'users',
    where: {
        and: [
            { field: 'age', op: 'gte', value: 18 },
            { or: [
                { field: 'country', op: 'eq', value: 'US' },
                { field: 'country', op: 'eq', value: 'CA' },
            ] },
        ],
    },
    order_by: [{ field: 'name', asc: true }],
})
```

## Index selection

The planner picks an index when:

- the `where` clause has a leading equality or range predicate on an indexed field
- the `order_by` matches an existing index's column order

For unindexed fields the planner falls back to a full scan with a per-document predicate filter. For collections under ~10⁴ documents the cost difference is negligible; for larger collections, declare a secondary index on hot fields via the Rust-side schema builder.

## Live queries

A query can also be **observed** rather than executed once. `observeQuery` emits the initial snapshot as `QueryDiff.added`, then a diff every time the result set changes. See [observation.md](observation.md#live-queries).

## Edge cases

### Empty result

`executeQuery` returns an empty array, not `null`. Don't add a `null` guard.

### Invalid JSON

If the query JSON itself doesn't parse, you get `RaftError.invalidJson` (code 6) — same code the engine returns for malformed document JSON. The error message identifies which side; check the platform's error mapping.

### Unknown collection

A query against a non-existent collection returns an empty result set (the collection is created on first write — there's no "create collection" call). If your collection name is mistyped you'll silently see zero results.

### Result memory cost

Each match is materialised as JSON on the FFI boundary. For a 10⁴-row scan with large documents this is memory-heavy. Prefer paginated `limit + offset` for the UI; the planner doesn't stream.

## Performance notes

- **Indexed lookups are O(log n) + result-set scan.**
- **Full scans are O(n)** with a small per-doc filter cost. The bloom filter on each SSTable lets the planner skip levels that can't contain the predicate's keys.
- **Order-by-only queries (no `where`)** use the index if `order_by` matches one; otherwise the engine materialises and sorts. For >10⁴ rows declare an index.
- **JSON encoding is the per-result hot cost.** The platforms re-decode each result. If you only need 5 fields, ask the future projection API to elide the rest (not in v0.1.0).

## Related

- [Typed collections](collections.md) — where documents live
- [Observation](observation.md) — live queries that emit diffs
- [Errors](errors.md) — `InvalidJson` (code 6) and others
