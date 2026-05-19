# Transactions

[← Back to docs index](README.md)

Raft supports **optimistic concurrency**: a transaction tracks every document you read and validates the read-set at commit time. If any tracked document was modified between read and commit, the commit fails with `TransactionConflict` (code 7) and no writes are applied. You catch the error and retry.

This model trades a small chance of retry for zero pessimistic locking — there's no global lock manager, no deadlock graph, and a single-writer can run at peak speed.

## When to use a transaction

- **Read-modify-write on a doc** that other writers may touch (counters, balances, allocation lists).
- **Multi-doc updates that need atomicity** (transfer money between two accounts, move an item between two collections).
- **Bulk imports** where you'd rather one fsync than thousands.

When you **don't** need a transaction: a single `put` or `delete` is already atomic. Wrapping it in a transaction just adds the txn handle cost.

## Method matrix

| Operation | Swift | Kotlin | Dart | TypeScript |
|---|---|---|---|---|
| Begin | `beginTransaction() throws -> RaftTransaction` | `beginTransaction(): RaftTransaction` (suspend) | `beginTransaction() -> Future<RaftTransaction>` | _internal — use `withTransaction`_ |
| Read | `txn.get(collection: id:) throws -> Data?` | `txn.get(c, id): ByteArray?` (suspend) | `txn.get(c, id) -> Future<Uint8List?>` | `txn.get(c, id): Promise<string \| null>` |
| Write | `txn.put(collection: document:)` | `txn.put(c, json)` | `txn.put(c, json)` | `txn.put(c, json)` |
| Delete | `txn.delete(collection: id:)` | `txn.delete(c, id)` | `txn.delete(c, id)` | `txn.delete(c, id)` |
| Commit | `txn.commit() throws` | `txn.commit()` (suspend) | `txn.commit() -> Future` | _internal_ |
| Rollback | `txn.rollback()` | `txn.rollback()` (suspend) | `txn.rollback() -> Future` | _internal_ |
| Scoped helper | `db.withTransaction { txn in … }` | `db.withTransaction { txn -> … }` | `db.withTransaction((txn) async { … })` | `db.withTransaction(async (txn) => { … })` |

Lifecycle:

- **Begin** allocates the handle.
- **Read / write / delete** are buffered or tracked inside the transaction. Writes don't go to disk until commit.
- **Commit** validates the read set and applies all buffered changes atomically. Consumes the handle.
- **Rollback** discards the buffer. Consumes the handle.

Calling commit or rollback twice on the same handle throws `InvalidHandle` (code 8).

## Use `withTransaction`

The scoped helper is the preferred entry point on every platform — it handles cleanup so you can't leak a handle. Use the manual `beginTransaction` only when the lifecycle straddles multiple async boundaries (rare).

### Swift

```swift
try await db.withTransaction { txn in
    guard let raw = try txn.get(collection: "accounts", id: 1) else { return }
    var account = try JSONDecoder().decode(Account.self, from: raw)
    account.balance += 100
    try txn.put(collection: "accounts", document: try JSONEncoder().encode(account))
}
```

### Kotlin

```kotlin
db.withTransaction { txn ->
    val raw = txn.get("accounts", 1L) ?: return@withTransaction
    val account = Json.decodeFromString<Account>(String(raw))
    val updated = account.copy(balance = account.balance + 100)
    txn.put("accounts", Json.encodeToString(updated).toByteArray())
}
```

### Dart

```dart
await db.withTransaction((txn) async {
    final raw = await txn.get('accounts', 1);
    if (raw == null) return;
    final account = jsonDecode(utf8.decode(raw));
    account['balance'] = account['balance'] + 100;
    await txn.put('accounts',
        Uint8List.fromList(utf8.encode(jsonEncode(account))));
});
```

### TypeScript

```ts
await db.withTransaction(async (txn) => {
    const raw = await txn.get('accounts', 1)
    if (!raw) return
    const account = JSON.parse(raw)
    account.balance += 100
    await txn.put('accounts', JSON.stringify(account))
})
```

## Example: atomic counter increment with retry

The canonical conflict-prone pattern. Retry with exponential backoff because conflict is recoverable.

### Swift

```swift
func increment(counterId: UInt64) async throws {
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
    throw RaftError.transactionConflict
}
```

### Kotlin

```kotlin
suspend fun increment(counterId: Long) {
    var attempt = 0
    while (attempt < 5) {
        try {
            db.withTransaction { txn ->
                val raw = txn.get("counters", counterId) ?: return@withTransaction
                val c = Json.decodeFromString<Counter>(String(raw))
                txn.put("counters",
                    Json.encodeToString(c.copy(value = c.value + 1)).toByteArray())
            }
            return
        } catch (e: RaftError.TransactionConflict) {
            delay((50L shl attempt))
            attempt++
        }
    }
    throw RaftError.TransactionConflict()
}
```

### Dart

```dart
Future<void> increment(int counterId) async {
    for (var attempt = 0; attempt < 5; attempt++) {
        try {
            await db.withTransaction((txn) async {
                final raw = await txn.get('counters', counterId);
                if (raw == null) return;
                final c = jsonDecode(utf8.decode(raw));
                c['value'] = c['value'] + 1;
                await txn.put('counters',
                    Uint8List.fromList(utf8.encode(jsonEncode(c))));
            });
            return;
        } on RaftDbException catch (e) {
            if (e.code != 7) rethrow;
            await Future.delayed(Duration(milliseconds: 50 << attempt));
        }
    }
    throw RaftDbException('Exhausted retries', code: 7);
}
```

### TypeScript

```ts
async function increment(counterId: number): Promise<void> {
    for (let attempt = 0; attempt < 5; attempt++) {
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
            await new Promise(r => setTimeout(r, 50 * (1 << attempt)))
        }
    }
    throw new Error('Exhausted transaction retries')
}
```

## Example: transfer between two accounts

Two reads + two writes, atomic.

### Swift

```swift
try await db.withTransaction { txn in
    guard let fromRaw = try txn.get(collection: "accounts", id: fromId),
          let toRaw   = try txn.get(collection: "accounts", id: toId)
    else { throw TransferError.accountMissing }

    var from = try JSONDecoder().decode(Account.self, from: fromRaw)
    var to   = try JSONDecoder().decode(Account.self, from: toRaw)

    guard from.balance >= amount else { throw TransferError.insufficient }
    from.balance -= amount
    to.balance   += amount

    try txn.put(collection: "accounts", document: try JSONEncoder().encode(from))
    try txn.put(collection: "accounts", document: try JSONEncoder().encode(to))
}
```

### Kotlin

```kotlin
db.withTransaction { txn ->
    val fromRaw = txn.get("accounts", fromId) ?: throw TransferError.AccountMissing
    val toRaw   = txn.get("accounts", toId)   ?: throw TransferError.AccountMissing
    val from = Json.decodeFromString<Account>(String(fromRaw))
    val to   = Json.decodeFromString<Account>(String(toRaw))

    if (from.balance < amount) throw TransferError.Insufficient
    val updatedFrom = from.copy(balance = from.balance - amount)
    val updatedTo   = to.copy(balance   = to.balance   + amount)

    txn.put("accounts", Json.encodeToString(updatedFrom).toByteArray())
    txn.put("accounts", Json.encodeToString(updatedTo).toByteArray())
}
```

### Dart

```dart
await db.withTransaction((txn) async {
    final fromRaw = await txn.get('accounts', fromId);
    final toRaw   = await txn.get('accounts', toId);
    if (fromRaw == null || toRaw == null) throw 'missing account';

    final from = jsonDecode(utf8.decode(fromRaw));
    final to   = jsonDecode(utf8.decode(toRaw));
    if (from['balance'] < amount) throw 'insufficient funds';

    from['balance'] -= amount;
    to['balance']   += amount;

    await txn.put('accounts', Uint8List.fromList(utf8.encode(jsonEncode(from))));
    await txn.put('accounts', Uint8List.fromList(utf8.encode(jsonEncode(to))));
});
```

### TypeScript

```ts
await db.withTransaction(async (txn) => {
    const fromRaw = await txn.get('accounts', fromId)
    const toRaw   = await txn.get('accounts', toId)
    if (!fromRaw || !toRaw) throw new Error('missing account')

    const from = JSON.parse(fromRaw)
    const to   = JSON.parse(toRaw)
    if (from.balance < amount) throw new Error('insufficient funds')

    from.balance -= amount
    to.balance   += amount

    await txn.put('accounts', JSON.stringify(from))
    await txn.put('accounts', JSON.stringify(to))
})
```

## Example: bulk import without conflict tracking

If you're writing many docs and don't read any (no read-set), there's no conflict risk. The transaction is still useful for the single-fsync property:

### Swift

```swift
try await db.withTransaction { txn in
    for record in csvRows {
        let doc = User(name: record.name, age: record.age)
        try txn.put(collection: "users",
                    document: try JSONEncoder().encode(doc))
    }
}
```

(Kotlin / Dart / TS follow the same pattern — see [collections.md → bulk import](collections.md#example-bulk-import-with-a-transaction).)

## Conflict semantics

A `commit()` fails with `TransactionConflict` (code 7) when the engine detects, at validation time, that **any** document you read inside the transaction has a different version than when you read it. Specifically:

- Read a doc → its version `v` is recorded.
- Some other writer commits a change to that doc → its version becomes `v+1`.
- You commit → validation sees `v ≠ v+1` → reject.

Important nuances:

- **Reads outside the transaction don't track.** Only `txn.get(...)` is tracked. `db.collectionGet(...)` outside the txn is not.
- **Reads on deleted/missing docs still track.** If you `txn.get("users", 42)` and it returns `null`, your txn fails if someone *inserts* `users:42` before you commit. (Intent: "I checked it didn't exist" → "actually it does" → conflict.)
- **The read set is per-doc, not per-collection.** Bulk import (write-only) never conflicts; bulk update (read-then-write) conflicts only on the touched docs.

## Edge cases

### Using the handle after commit/rollback

The handle is **consumed**. Calling another method throws `InvalidHandle` (code 8). Always re-`beginTransaction` for a new attempt.

### Dropping the handle without finalising

The Swift `RaftTransaction.deinit`, Kotlin's `RaftTransaction`'s GC finalizer, the Dart/RN's `withTransaction` scope all roll back unfinalised handles. But don't rely on GC timing — call `commit()` or `rollback()` explicitly.

### Read your own writes inside a transaction

A `txn.get(collection, id)` after a `txn.put(collection, ...)` of the same id returns the buffered value, not the on-disk value. This makes "read-modify-write" inside the same txn correct without manual bookkeeping.

### Long-running transactions

A transaction holds a snapshot reference. Long-lived transactions delay compaction and consume memory proportional to the docs you read. Keep transactions tight: read → mutate → commit in milliseconds, not minutes.

### Transactions on the raw-KV surface

Not supported. The transaction surface is for typed collections only. If you need atomicity on raw-KV writes, layer your own write-ahead log on top, or model the data as a typed collection.

## Performance notes

- **Read-set tracking costs ~1 KiB / doc.** Reading many docs in one transaction inflates RAM proportionally.
- **Commit triggers one fsync** at most (the WAL). Multiple buffered writes share that fsync — the per-write cost is amortised.
- **Conflict probability scales with the doc-touch rate**, not the txn duration. A short txn that touches a hot key may still conflict; a long txn that touches cold keys won't.
- **Retry budget is yours to tune.** The platforms' examples use 5 retries with exponential backoff; for hot keys, give up after fewer retries and degrade to "best-effort" semantics.

## Related

- [Typed collections](collections.md) — the surface transactions operate on
- [Observation](observation.md) — listen for changes that might conflict
- [Errors](errors.md) — `TransactionConflict` (code 7) and `InvalidHandle` (code 8)
