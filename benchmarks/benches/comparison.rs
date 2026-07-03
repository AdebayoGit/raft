//! raft-db vs SQLite — reproducible comparative benchmarks (X9).
//!
//! Methodology (see ../README.md and docs/BENCHMARKS.md for the full
//! honesty notes):
//!
//! - Both engines run with *matched durability* per group:
//!   - `durable_*`: raft `SyncMode::Always` (fsync per commit) vs SQLite
//!     WAL journal + `synchronous=FULL` with one implicit transaction
//!     per statement. Both fsync on every write.
//!   - `batched_*`: raft `apply_batch` (one WAL write + fsync per batch)
//!     vs SQLite one explicit transaction for the whole batch.
//!   - Reads: raft default config vs SQLite WAL + `synchronous=NORMAL`
//!     (durability is irrelevant for reads; both read from a flushed,
//!     fully persisted store).
//! - Same logical workload: fixed-width `key-XXXXXXXX` keys and
//!   `value-XXXXXXXX` values. SQLite uses a `(key TEXT PRIMARY KEY,
//!   value TEXT)` table — its closest equivalent to a KV store.
//! - Fresh database directory/file per timed iteration for write
//!   benches (`BatchSize::PerIteration`); reads share one pre-loaded,
//!   flushed store built outside the timed section.
//! - Realm and Isar cannot be driven from a Rust harness (they ship as
//!   mobile SDKs only). The workload spec is documented so the same
//!   scenarios can be reproduced in their native harnesses.

use std::fs;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use raftdb::wal::SyncMode;
use raftdb::{BatchOp, StorageConfig, StorageEngine};
use rusqlite::Connection;

fn bench_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join("raft_vs_sqlite").join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir).unwrap();
    }
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn key(i: usize) -> String {
    format!("key-{i:08}")
}

fn value(i: usize) -> String {
    format!("value-{i:08}")
}

fn raft_config(sync: SyncMode) -> StorageConfig {
    StorageConfig {
        wal_sync: sync,
        ..StorageConfig::default()
    }
}

/// SQLite connection tuned to the given durability profile.
///
/// `fullfsync` matters on macOS: raft's `SyncMode::Always` goes through
/// Rust's `File::sync_all`, which issues `F_FULLFSYNC` (a true flush to
/// stable storage). SQLite's `synchronous=FULL` only issues a plain
/// `fsync()` — which macOS is allowed to satisfy from the drive cache —
/// unless `PRAGMA fullfsync=ON`. Durable groups set it so both engines
/// pay for the same durability; anything else compares a real flush
/// against a lying one.
fn sqlite_conn(path: &std::path::Path, synchronous: &str, fullfsync: bool) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "synchronous", synchronous)
        .unwrap();
    if fullfsync {
        conn.pragma_update(None, "fullfsync", "ON").unwrap();
        conn.pragma_update(None, "checkpoint_fullfsync", "ON")
            .unwrap();
    }
    conn.execute(
        "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        [],
    )
    .unwrap();
    conn
}

/// Durable inserts: every write is individually committed and fsynced.
fn durable_inserts(c: &mut Criterion) {
    const N: usize = 500;
    let mut group = c.benchmark_group("durable_inserts");
    group.sample_size(10);
    group.throughput(Throughput::Elements(N as u64));

    group.bench_function(BenchmarkId::new("raft", N), |b| {
        b.iter_batched(
            || {
                let dir = bench_dir("raft_durable");
                StorageEngine::open(&dir, raft_config(SyncMode::Always)).unwrap()
            },
            |mut engine| {
                for i in 0..N {
                    engine
                        .put(key(i).into_bytes(), value(i).into_bytes())
                        .unwrap();
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function(BenchmarkId::new("sqlite", N), |b| {
        b.iter_batched(
            || {
                let dir = bench_dir("sqlite_durable");
                sqlite_conn(&dir.join("bench.db"), "FULL", true)
            },
            |conn| {
                let mut stmt = conn
                    .prepare("INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)")
                    .unwrap();
                for i in 0..N {
                    stmt.execute([key(i), value(i)]).unwrap();
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

/// Batched inserts: one commit (one fsync) for the whole batch.
fn batched_inserts(c: &mut Criterion) {
    const N: usize = 10_000;
    let mut group = c.benchmark_group("batched_inserts");
    group.sample_size(10);
    group.throughput(Throughput::Elements(N as u64));

    group.bench_function(BenchmarkId::new("raft", N), |b| {
        b.iter_batched(
            || {
                let dir = bench_dir("raft_batched");
                StorageEngine::open(&dir, raft_config(SyncMode::Always)).unwrap()
            },
            |mut engine| {
                let ops: Vec<BatchOp> = (0..N)
                    .map(|i| BatchOp::Put {
                        key: key(i).into_bytes(),
                        value: value(i).into_bytes(),
                    })
                    .collect();
                engine.apply_batch(ops).unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function(BenchmarkId::new("sqlite", N), |b| {
        b.iter_batched(
            || {
                let dir = bench_dir("sqlite_batched");
                sqlite_conn(&dir.join("bench.db"), "FULL", true)
            },
            |mut conn| {
                let tx = conn.transaction().unwrap();
                {
                    let mut stmt = tx
                        .prepare("INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)")
                        .unwrap();
                    for i in 0..N {
                        stmt.execute([key(i), value(i)]).unwrap();
                    }
                }
                tx.commit().unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

/// Point reads over a pre-loaded, flushed store of `LOADED` rows.
fn point_reads(c: &mut Criterion) {
    const LOADED: usize = 10_000;
    const READS: usize = 10_000;
    const STRIDE: usize = 7919; // prime — deterministic pseudo-random order

    let mut group = c.benchmark_group("point_reads");
    group.sample_size(10);
    group.throughput(Throughput::Elements(READS as u64));

    // raft: load, flush, reopen read-only pattern.
    let raft_dir = bench_dir("raft_reads");
    {
        let mut engine = StorageEngine::open(&raft_dir, raft_config(SyncMode::Off)).unwrap();
        for i in 0..LOADED {
            engine
                .put(key(i).into_bytes(), value(i).into_bytes())
                .unwrap();
        }
        engine.flush().unwrap();
    }
    group.bench_function(BenchmarkId::new("raft", READS), |b| {
        let engine = StorageEngine::open(&raft_dir, raft_config(SyncMode::Off)).unwrap();
        b.iter(|| {
            let mut idx = 0usize;
            for _ in 0..READS {
                idx = (idx + STRIDE) % LOADED;
                assert!(engine.get(key(idx).as_bytes()).unwrap().is_some());
            }
        });
    });

    // SQLite: same data, same access pattern.
    let sqlite_dir = bench_dir("sqlite_reads");
    let sqlite_path = sqlite_dir.join("bench.db");
    {
        let mut conn = sqlite_conn(&sqlite_path, "NORMAL", false);
        let tx = conn.transaction().unwrap();
        {
            let mut stmt = tx
                .prepare("INSERT INTO kv (key, value) VALUES (?1, ?2)")
                .unwrap();
            for i in 0..LOADED {
                stmt.execute([key(i), value(i)]).unwrap();
            }
        }
        tx.commit().unwrap();
    }
    group.bench_function(BenchmarkId::new("sqlite", READS), |b| {
        let conn = sqlite_conn(&sqlite_path, "NORMAL", false);
        let mut stmt = conn.prepare("SELECT value FROM kv WHERE key = ?1").unwrap();
        b.iter(|| {
            let mut idx = 0usize;
            for _ in 0..READS {
                idx = (idx + STRIDE) % LOADED;
                let v: String = stmt.query_row([key(idx)], |row| row.get(0)).unwrap();
                assert!(!v.is_empty());
            }
        });
    });

    group.finish();
}

/// Ordered range scan: read a contiguous 1k-key range out of 10k.
fn range_scans(c: &mut Criterion) {
    const LOADED: usize = 10_000;
    const RANGE: usize = 1_000;
    const START: usize = 4_000;

    let mut group = c.benchmark_group("range_scans");
    group.sample_size(10);
    group.throughput(Throughput::Elements(RANGE as u64));

    let raft_dir = bench_dir("raft_scan");
    {
        let mut engine = StorageEngine::open(&raft_dir, raft_config(SyncMode::Off)).unwrap();
        for i in 0..LOADED {
            engine
                .put(key(i).into_bytes(), value(i).into_bytes())
                .unwrap();
        }
        engine.flush().unwrap();
    }
    group.bench_function(BenchmarkId::new("raft", RANGE), |b| {
        let engine = StorageEngine::open(&raft_dir, raft_config(SyncMode::Off)).unwrap();
        b.iter(|| {
            // raft's scan surface is prefix-based; `key-00004` covers
            // exactly `key-00004000`..`key-00004999` → the 1000 keys
            // 4000..4999 under the fixed-width `key-XXXXXXXX` scheme.
            let entries = engine.scan_prefix(b"key-00004").unwrap();
            assert_eq!(entries.len(), RANGE);
        });
    });

    let sqlite_dir = bench_dir("sqlite_scan");
    let sqlite_path = sqlite_dir.join("bench.db");
    {
        let mut conn = sqlite_conn(&sqlite_path, "NORMAL", false);
        let tx = conn.transaction().unwrap();
        {
            let mut stmt = tx
                .prepare("INSERT INTO kv (key, value) VALUES (?1, ?2)")
                .unwrap();
            for i in 0..LOADED {
                stmt.execute([key(i), value(i)]).unwrap();
            }
        }
        tx.commit().unwrap();
    }
    group.bench_function(BenchmarkId::new("sqlite", RANGE), |b| {
        let conn = sqlite_conn(&sqlite_path, "NORMAL", false);
        let mut stmt = conn
            .prepare("SELECT key, value FROM kv WHERE key >= ?1 AND key < ?2 ORDER BY key")
            .unwrap();
        b.iter(|| {
            let rows: Vec<(String, String)> = stmt
                .query_map([key(START), key(START + RANGE)], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            assert_eq!(rows.len(), RANGE);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    durable_inserts,
    batched_inserts,
    point_reads,
    range_scans
);
criterion_main!(benches);
