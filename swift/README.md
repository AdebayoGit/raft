# RaftDB — Swift

> Mobile-native embedded database for iOS / macOS. Idiomatic Swift over the Raft Rust core.

[![Swift](https://img.shields.io/badge/swift-5.9%2B-orange.svg)](https://swift.org)
[![Platforms](https://img.shields.io/badge/platforms-iOS%2014%20%7C%20macOS%2012-lightgrey.svg)]()

Offline-first key-value and document storage with `async`/`await`, `AsyncStream` observers, and a static `xcframework` linked from the Raft Rust core.

## Install

Swift Package Manager — `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/yourusername/raft-db", from: "0.1.0"),
]
```

Or via Xcode: **File → Add Packages…** and paste the repository URL.

The package vendors a precompiled `RaftDB.xcframework` containing both device and simulator slices for iOS and macOS — no extra build steps required.

## Quickstart

```swift
import Foundation
import RaftDB

struct User: Codable, Sendable {
    let id: String
    let name: String
    let age: Int
}

@main
struct Example {
    static func main() async throws {
        let path = NSHomeDirectory() + "/Library/Application Support/raftdb"
        let db = try await RaftDB.open(path: path)
        defer { db.close() }

        let users = RaftCollection<User>(db: db, name: "users")

        try await users.put(id: "1", document: User(id: "1", name: "Alice", age: 30))

        if let alice = try await users.get(id: "1") {
            print("Loaded: \(alice)")
        }

        // Live observation — yields the current value, then every change
        Task {
            for await user in users.observe(id: "1") {
                print("Observed: \(String(describing: user))")
            }
        }

        try await users.delete(id: "1")
    }
}
```

All native calls are dispatched through `withCheckedThrowingContinuation` on a global concurrent queue. Safe to call from any actor / Task context.

## API

### `RaftDB.open(path:)`

```swift
let db = try await RaftDB.open(path: "/path/to/db")
```

Async. There is also `RaftDB.openSync(path:)` for tests / non-async contexts.

### `db.put(key:value:)` / `db.get(key:)` / `db.delete(key:)`

Low-level byte ops. Keys and values are `Data`. `get` returns `Data?`.

### `RaftCollection<T: Codable>`

Typed collection wrapper. Encodes via `JSONEncoder` / decodes via `JSONDecoder` by default — pass custom encoders for snake_case, ISO dates, etc.

```swift
let custom = JSONEncoder()
custom.dateEncodingStrategy = .iso8601
let users = RaftCollection<User>(db: db, name: "users", encoder: custom)
```

### `db.observe(prefix:)` → `AsyncStream<QueryDiff>`

A stream that yields the current snapshot first, then every subsequent change to a matching key.

```swift
for await diff in db.observe(prefix: Data("user:".utf8)) {
    // diff.key, diff.value
}
```

`RaftCollection` exposes `observe(id:)` and `observeAll()` for typed streams.

### `db.close()`

Idempotent. Also called automatically in `deinit`, so you can rely on RAII for short-lived databases.

## Errors

Native errors are thrown as `RaftError` — a typed Swift enum mirroring the C `RftError` codes.

```swift
do {
    try await db.put(key: key, value: value)
} catch RaftError.ioError {
    // storage failure
} catch RaftError.invalidUtf8 {
    // bad path
}
```

## Concurrency

`RaftDB` and `RaftCollection<T>` are `Sendable`. Strict concurrency is supported under the Swift 6 language mode. The handle is internally lock-protected so concurrent reads from multiple tasks are safe.

## Roadmap

The current Swift layer wraps the v0.1.0 KV surface. Bridging the document / query / transaction / observer FFI (already in `core/include/raft.h`) is the next milestone — bringing first-class `RaftCollection<T>`-backed predicate queries and proper live-query streams.

## License

Apache-2.0 OR MIT.
