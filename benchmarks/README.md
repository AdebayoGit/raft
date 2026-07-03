# raft-db benchmark suite (X9)

Reproducible, comparative benchmarks of raft-db against SQLite, with an
honest methodology. Full methodology notes and published results live in
[`../docs/BENCHMARKS.md`](../docs/BENCHMARKS.md).

## Run

```bash
cd benchmarks
cargo bench
```

Criterion writes HTML reports to `target/criterion/`.

## What is compared

| Group | Workload | raft-db | SQLite |
|---|---|---|---|
| `durable_inserts` | 500 writes, fsync per commit | `SyncMode::Always` | WAL + `synchronous=FULL`, autocommit |
| `batched_inserts` | 10 000 writes, one commit | `apply_batch` | WAL + `synchronous=FULL`, one transaction |
| `point_reads` | 10 000 keyed reads of 10 000 rows | `get` | `SELECT ... WHERE key = ?` |
| `range_scans` | 1 000-key ordered range of 10 000 rows | `scan_prefix` | `SELECT ... WHERE key >= ? AND key < ? ORDER BY key` |

## Why Realm and Isar are not in the harness

Realm and Isar ship as mobile SDKs (Swift/Kotlin and Dart respectively)
with no Rust API — they cannot be driven from this harness without
measuring an FFI bridge that neither vendor ships. Running them here
would benchmark the bridge, not the database. Instead, the workload
spec above is documented precisely so the same scenarios can be
reproduced in their native harnesses; see `docs/BENCHMARKS.md` for the
cross-language protocol.

## Honesty rules

- Durability is matched per group — never fsync-per-write vs buffered.
- Same logical dataset (fixed-width text keys/values) for both engines.
- Fresh store per timed iteration for writes; pre-loaded flushed store
  built outside the timed section for reads.
- SQLite is the `bundled` build of rusqlite (version pinned by
  `Cargo.lock`) — no system-library variance.
- This crate is standalone: rusqlite and criterion never enter
  raft-db's own dependency tree.
