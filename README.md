# Raft

> A mobile-native embedded database. Offline-first by design.

Raft is an embedded, offline-first database built in Rust, designed from the ground up for mobile clients. It is not an adaptation of a server-side database. Every architectural decision assumes the device is the primary compute, connectivity is unreliable, and sync is a feature — not a requirement.

By default, the local database is the source of truth. The network is a sync channel.

## Why Raft

Most embedded databases on mobile started life on a server. Their assumptions — fast disks, tens of cores, abundant memory, reliable I/O — leak through to mobile in a hundred small ways: bursty battery drain, unpredictable cold-start times, tail latency on flaky filesystems.

Raft was designed for the device first:

- **LSM-tree storage** optimised for mobile flash I/O patterns and write amplification
- **CRDT-backed fields** for automatic, deterministic conflict resolution on sync
- **Reactive queries** as a first-class primitive, not a bolt-on
- **Per-collection sync authority** — `LocalFirst`, `RemoteAuthority`, or `RemoteFirst` modes
- **Tiny binary** — no SQL parser, minimal dependencies, mobile-conscious linker settings

The core is one Rust crate. Each platform binding (Flutter, Android, Swift, React Native) is a thin wrapper over a stable C ABI.

## Architecture

```
┌─────────────────────────────────────────────┐
│            Application Layer                │
│   Dart  │  Kotlin  │  Swift  │  TypeScript  │
├─────────────────────────────────────────────┤
│              C ABI (rft_*)                  │
│        (cbindgen-generated raft.h)          │
├─────────────────────────────────────────────┤
│              Database runtime               │
│   collections · queries · transactions      │
│        · reactive observers                 │
├─────────────────────────────────────────────┤
│              Storage engine                 │
│  WAL → MemTable → SSTables → Compaction     │
│         (LSM-tree, mobile-tuned)            │
└─────────────────────────────────────────────┘
```

The Rust core in [`core/`](core/) compiles to a single static library. Platform bindings load it via FFI and expose idiomatic APIs in their native language.

## Install

| Platform     | Package                       | Quickstart                                    |
| ------------ | ----------------------------- | --------------------------------------------- |
| Flutter      | `raft_db` (pub.dev)           | [flutter/README.md](flutter/README.md)        |
| Android      | `com.raftdb:raftdb` (Maven)   | [android/README.md](android/README.md)        |
| Swift        | `RaftDB` (Swift Package Index)| [swift/README.md](swift/README.md)            |
| React Native | `react-native-raft` (npm)     | [rn/README.md](rn/README.md)                  |
| Rust         | `raft-db` (crates.io)         | See [Rust API](#rust-api) below               |

> **Status:** v0.1.0 ships the storage engine, document layer, queries, transactions, and live observers. Sync is targeted for v0.2.0 — see [WORK_REMAINING.md](WORK_REMAINING.md).

## Rust API

```rust
use raft_db::Database;
use raft_db::query::{Document, Query, Filter, Value};
use raft_db::index::DocId;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let db = Database::open("./data")?;

// Insert a document
let alice = Document::new(DocId(1))
    .with_field("name", Value::String("Alice".into()))
    .with_field("age", Value::Int(30));
db.put("users", alice)?;

// Query
let q = Query::collection("users")
    .filter(Filter::gte("age", Value::Int(18)))
    .limit(10);
let adults: Vec<Document> = db.query(&q);

// Transactions
let mut txn = db.begin_transaction();
let _ = txn.get("users", DocId(1))?;
txn.put("users", /* updated doc */ alice.clone())?;
txn.commit()?;
# Ok(()) }
```

See `core/src/database.rs` for the full API.

## Building from source

```bash
cd core
cargo test --features ffi          # 359 tests
cargo build --features ffi --release
```

To regenerate the C header after FFI changes:

```bash
cbindgen --config core/cbindgen.toml --crate raft-db --output core/include/raft.h
```

To build the mobile artifacts:

```bash
./build-mobile.sh
```

## What Raft is not

- Not a server database adapted for mobile
- Not a cloud product or vendor lock-in
- Not a SQL database
- Not a key-value store (though one powers it internally)
- Not a replacement for your backend — it is the client layer

## Naming

You are always on the raft (local, offline). The ocean (sync) is optional. The name evokes resilience, mobility, and the idea that the local node is complete and self-sufficient without a connection.

## License

Apache-2.0 OR MIT.
