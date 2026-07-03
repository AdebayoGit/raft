# Benchmarks — raft-db vs SQLite (X9)

Reproducible comparative benchmarks, published with an honest
methodology. The harness lives in [`../benchmarks/`](../benchmarks/)
and is a standalone crate — rusqlite and criterion never enter
raft-db's own dependency tree.

## Reproducing

```bash
cd benchmarks
cargo bench
# HTML reports: benchmarks/target/criterion/report/index.html
```

Criterion runs each group with `sample_size(10)`; each write sample
uses a fresh database directory (`BatchSize::PerIteration`) so no run
inherits another's compaction or page-cache state.

## Methodology

### Matched durability

Comparing an fsync-per-commit engine against a buffered one is the most
common way database benchmarks lie. Every group here pins both engines
to the same durability contract:

| Group | raft-db | SQLite |
|---|---|---|
| `durable_inserts` | `SyncMode::Always` — fsync per commit | WAL journal, `synchronous=FULL`, autocommit per statement |
| `batched_inserts` | `apply_batch` — one WAL write + fsync per batch | WAL journal, `synchronous=FULL`, one explicit transaction |
| `point_reads` | flushed store, `SyncMode::Off` (reads don't sync) | WAL journal, `synchronous=NORMAL` |
| `range_scans` | flushed store, `SyncMode::Off` | WAL journal, `synchronous=NORMAL` |

### Matched workload

- Keys: fixed-width `key-XXXXXXXX` text; values: `value-XXXXXXXX` text.
- SQLite schema: `CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT NOT
  NULL)` — its closest KV-store equivalent, reading/writing through the
  primary-key index exactly as raft reads/writes through its LSM tree.
- Reads use a deterministic prime-stride (7919) access pattern over
  10 000 pre-loaded rows, identical for both engines.
- Range scan: raft `scan_prefix` vs SQLite indexed `BETWEEN`-style range
  with `ORDER BY key` — both return the same 1 000 ordered rows.

### Environment pinning

- SQLite is rusqlite's `bundled` build — the SQLite version is pinned by
  `benchmarks/Cargo.lock`, not whatever the OS ships.
- raft-db is built from the sibling `core/` tree at the same commit.
- Numbers below are from a single machine and are indicative, not
  universal. Re-run on your target hardware; mobile I/O behaves
  differently from desktop NVMe.

## Results

<!-- RESULTS:BEGIN -->

Reference machine: macOS (Darwin), x86_64, local SSD. Criterion mean of
10 samples per group. Higher throughput is better.

| Group | raft-db | SQLite | Ratio |
|---|---|---|---|
| `durable_inserts` (500 × fsync/commit) | 9.69 s — **51.6 op/s** | 9.79 s — **51.0 op/s** | ~1.0× (parity) |
| `batched_inserts` (10 000, one commit) | 35.9 ms — **278.5 K op/s** | 66.4 ms — **150.7 K op/s** | raft **1.85×** faster |
| `point_reads` (10 000 keyed reads) | 12.5 ms — **802.6 K op/s** | 50.5 ms — **198.0 K op/s** | raft **4.1×** faster |
| `range_scans` (1 000-key ordered range) | 3.23 ms — **309.9 K op/s** | 474 µs — **2.11 M op/s** | SQLite **6.8×** faster |

Reading the numbers honestly:

- **Durable inserts are hardware-bound, not engine-bound.** At ~51
  commits/s both engines sit on the cost of a true flush to stable
  storage (`F_FULLFSYNC` on this machine's SSD). Any engine claiming
  thousands of *durable* single-commits per second on macOS is not
  actually flushing.
- **Batched writes and point reads favour raft's LSM design** — one WAL
  append + memtable insert per batch, and memtable/SSTable+bloom-filter
  reads without B-tree page traversal.
- **Range scans favour SQLite.** Its B-tree stores rows physically in
  key order, so a 1 000-row range is a sequential page walk; raft's
  `scan_prefix` merges across memtable and SSTable levels. This is a
  real, known LSM trade-off — we publish it rather than hide it.

### macOS durability footnote (why `PRAGMA fullfsync`)

The single most important methodology detail on macOS: raft's
`SyncMode::Always` uses Rust's `File::sync_all`, which issues
`F_FULLFSYNC` — a true flush to stable storage. SQLite's
`synchronous=FULL` issues a plain `fsync()`, which macOS is permitted
to satisfy from the drive's volatile cache. Without correcting for
this, SQLite appears ~100× faster at durable inserts (5.8 K/s vs 51/s)
while providing strictly weaker durability. The durable groups
therefore set `PRAGMA fullfsync=ON` and `PRAGMA
checkpoint_fullfsync=ON`, after which the two engines converge to the
same hardware-bound commit rate — evidence the comparison is finally
apples-to-apples.

<!-- RESULTS:END -->

## Why Realm and Isar are not in this harness

The review finding (X9) asks for comparison against Realm, Isar, and
SQLite. SQLite is embeddable from Rust; Realm and Isar are not:

- **Realm** ships as Swift/Kotlin (and .NET/JS) SDKs. Its core is C++,
  but the supported, documented surface — the one an app developer
  actually gets — is the SDK. Driving realm-core directly from Rust
  would benchmark an unsupported internal API.
- **Isar** ships as a Dart SDK backed by a native core. There is no
  supported Rust (or C) client API.

Benchmarking either through a hand-rolled bridge measures the bridge,
not the database — precisely the kind of dishonesty this methodology
exists to avoid. Instead, the workload spec above (dataset shape,
durability contract, access patterns, group sizes) is defined
language-neutrally so it can be reproduced in each vendor's native
harness:

1. Same dataset: fixed-width text keys/values, sizes as per group.
2. Same durability contract per group (each SDK's fsync-per-commit vs
   batched-transaction equivalents).
3. Same access patterns: prime-stride point reads, contiguous ordered
   range.
4. Fresh store per write sample; pre-loaded flushed store for reads.

A Flutter-based harness implementing this spec for Isar and Realm (and
raft-db via its Dart bindings, so all three pay the same platform-channel
cost) is the natural follow-up and will be published alongside the
bindings benchmarks.
