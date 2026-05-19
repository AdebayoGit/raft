# Errors

[← Back to docs index](README.md)

The Rust core returns a single `RftError` enum across the C ABI. Each platform maps it to an idiomatic error type. Codes are stable across versions; meanings don't change.

## The error table

| Code | Symbol | Meaning | Typical cause |
|---|---|---|---|
| 0 | `Ok` | Success — not surfaced to the platform | — |
| 1 | `NullPointer` | An FFI argument was null | Bug in the platform binding; not user-facing |
| 2 | `InvalidUtf8` | A string argument wasn't valid UTF-8 | Filesystem paths with broken encoding |
| 3 | `IoError` | Storage engine I/O failure | Disk full, permission denied, corrupted SSTable |
| 4 | `NotFound` | The requested doc / key doesn't exist | Translated to `null` returns; **never thrown** on read APIs |
| 5 | `BufferTooSmall` | Output buffer wasn't large enough | Internal; the platforms retry transparently |
| 6 | `InvalidJson` | A document or query JSON failed to parse | Malformed JSON, schema validation failure |
| 7 | `TransactionConflict` | A tracked document was modified concurrently | Optimistic-concurrency conflict — retry the transaction |
| 8 | `InvalidHandle` | A freed transaction / query-result / subscription was reused | Calling commit twice; using a closed handle |
| 9 | `UnknownSubscription` | `unobserve` called with an unknown id | Subscription already cancelled |

Codes 4 and 5 are "non-errors" — both are absorbed by the platform bindings into a `null` return or an internal retry. The codes you'll actually encounter in user code are 2, 3, 6, 7, and 8.

## Platform mappings

### Swift — `RaftError`

```swift
public enum RaftError: Error, Equatable {
    case nullPointer            // 1
    case invalidUtf8            // 2
    case ioError                // 3
    case notFound               // 4 (internal)
    case bufferTooSmall         // 5 (internal)
    case invalidJson            // 6
    case transactionConflict    // 7
    case invalidHandle          // 8
    case unknownSubscription    // 9
    case unknown(UInt32)
}
```

Usage:

```swift
do {
    try await db.withTransaction { txn in /* ... */ }
} catch RaftError.transactionConflict {
    // retry
} catch RaftError.invalidJson {
    // payload was malformed
} catch let RaftError.unknown(code) {
    print("Unexpected code \(code)")
}
```

### Kotlin — sealed `RaftError`

```kotlin
sealed class RaftError(val code: Int, message: String) : RuntimeException(message) {
    class NullPointer       : RaftError(1, "null pointer argument")
    class InvalidUtf8       : RaftError(2, "invalid UTF-8")
    class IoError           : RaftError(3, "I/O error")
    class NotFound          : RaftError(4, "not found")
    class BufferTooSmall    : RaftError(5, "buffer too small")
    class InvalidJson       : RaftError(6, "invalid JSON")
    class TransactionConflict : RaftError(7, "transaction conflict")
    class InvalidHandle     : RaftError(8, "invalid handle")
    class UnknownSubscription : RaftError(9, "unknown subscription")
    class Unknown(code: Int) : RaftError(code, "unknown error code $code")
    // ...
}
```

Usage:

```kotlin
try {
    db.withTransaction { txn -> /* ... */ }
} catch (e: RaftError.TransactionConflict) {
    // retry
} catch (e: RaftError.InvalidJson) {
    // payload malformed
}
```

### Dart — `RaftDbException`

```dart
class RaftDbException implements Exception {
    final String message;
    final int? code;
    // ...
}
```

Usage:

```dart
try {
    await db.withTransaction((txn) async { /* ... */ });
} on RaftDbException catch (e) {
    if (e.code == 7) {
        // retry
    } else {
        rethrow;
    }
}
```

The codes are stable, so `e.code == 7` is the supported way to branch. There's no enum on the Dart side today.

### TypeScript — `Error` with code in the message

```ts
try {
    await db.withTransaction(async (txn) => { /* ... */ })
} catch (e) {
    const msg = String(e)
    if (msg.includes('code 7')) {
        // retry
    } else if (msg.includes('code 6')) {
        // payload malformed
    } else {
        throw e
    }
}
```

RN exceptions don't carry typed codes today — the message format is `RaftDB: <op> failed (code N)`. A typed error class is a v0.2 candidate.

## When each code shows up

### `InvalidUtf8` (code 2)

You'll see this if a filesystem path contains non-UTF-8 bytes (rare; only on some legacy Android NDK paths). Always pass UTF-8 strings to `open()`.

### `IoError` (code 3)

Catch-all for storage failures: disk full, file permissions, corrupted SSTable, MemTable flush failure. The error message includes the underlying OS error.

```swift
do {
    let db = try await RaftDB.open(path: "/dev/null/raft")
} catch RaftError.ioError {
    // The OS rejected the path
}
```

Recovery: typically not transient. Inform the user and abort.

### `InvalidJson` (code 6)

The most common user-facing error. Triggered by:

- A document JSON that doesn't parse (trailing commas, NaN, non-UTF-8)
- A query JSON that doesn't parse
- A document that fails Rust-side schema validation (when a schema is declared)

Recovery: fix the JSON. There's no "best effort" path here — the engine is strict.

### `TransactionConflict` (code 7)

A tracked document was modified between your read and your commit. Always recoverable: catch and retry.

```kotlin
suspend fun safeIncrement(id: Long, retries: Int = 5) {
    repeat(retries) { attempt ->
        try {
            db.withTransaction { txn -> /* ... */ }
            return
        } catch (e: RaftError.TransactionConflict) {
            delay((50L shl attempt))
        }
    }
    throw RaftError.TransactionConflict()
}
```

If you exhaust retries, the contention is severe — consider lock-free designs (CRDT counters, per-user partitioning).

### `InvalidHandle` (code 8)

You used a transaction / query-result / subscription handle after it was freed. Common causes:

- Calling `txn.commit()` twice
- Using a transaction across an isolate boundary (don't — keep the handle on one isolate)
- Holding a `RaftQueryResult` after the database is closed

Recovery: re-create the handle. Don't try to "revive" the old one.

### `UnknownSubscription` (code 9)

`unobserve` was called with a subscription id the engine doesn't know — usually because cancel ran twice, or the subscription was auto-cancelled when the database closed.

This is **safe to ignore**. The platforms generally don't surface it; if they do, treat as no-op.

## Conventions

### Reads return `null`, not throw

A missing key / doc is **always** a `null` return:

- `db.get(missingKey)` → `null` / `nil`
- `db.collectionGet("c", missingId)` → `null` / `nil`
- `txn.get(...)` → `null` / `nil`

Never wrap reads in a `NotFound` catch. The platforms map code 4 internally.

### Internal codes never propagate

Code 5 (`BufferTooSmall`) is part of the two-phase read protocol. The platforms always retry with the exact required size. You'll never see this code in user code.

### Error context

Most errors carry a short string describing the failing operation. The string is informational — don't match on it. Match on the code.

## Patterns

### Wrap a write with structured retry

```swift
@discardableResult
func withRetry<T>(_ block: () async throws -> T, maxAttempts: Int = 5) async throws -> T {
    var attempt = 0
    while true {
        do {
            return try await block()
        } catch RaftError.transactionConflict {
            attempt += 1
            if attempt >= maxAttempts { throw RaftError.transactionConflict }
            try await Task.sleep(nanoseconds: UInt64(50_000_000) << attempt)
        }
    }
}

try await withRetry {
    try await db.withTransaction { txn in /* ... */ }
}
```

### Convert to a domain error

```kotlin
sealed class TransferError : Exception() {
    class Insufficient : TransferError()
    class AccountMissing : TransferError()
    class Database(val cause: RaftError) : TransferError()
}

suspend fun transfer(...) {
    try {
        db.withTransaction { txn -> /* ... */ }
    } catch (e: RaftError) {
        throw TransferError.Database(e)
    }
}
```

### Surface to the UI

```ts
try {
    await db.withTransaction(async (txn) => { /* ... */ })
} catch (e) {
    const msg = String(e)
    if (msg.includes('code 7')) {
        showToast('Try again — someone else updated this just now')
    } else if (msg.includes('code 3')) {
        showToast('Storage error — restart the app')
    } else {
        throw e   // unexpected; let it surface
    }
}
```

## Related

- [Transactions](transactions.md) — when and why `TransactionConflict` happens
- [Typed collections](collections.md) — what `InvalidJson` surfaces from
- [Observation](observation.md) — `UnknownSubscription` and cleanup timing
