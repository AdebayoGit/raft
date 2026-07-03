//! Group commit / WAL write batching (S7f).
//!
//! Concurrent `Database::put` callers are group-committed: one leader
//! flushes every staged write with a single WAL write + fsync, so
//! multi-writer throughput under `SyncMode::Always` is no longer bound
//! to one fsync per put. These tests verify the semantics (durability,
//! visibility, per-writer results) and — behind `#[ignore]` — the
//! throughput gain.
//!
//! Run the throughput gate explicitly, in release mode:
//!   cargo test --release --features ffi --test group_commit -- --ignored --nocapture
#![cfg(feature = "ffi")]

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use raftdb::index::DocId;
use raftdb::query::{Document, Value};
use raftdb::wal::SyncMode;
use raftdb::{Database, StorageConfig};

fn temp_dir(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "raft-group-commit-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn always_sync_config() -> StorageConfig {
    StorageConfig {
        wal_sync: SyncMode::Always,
        ..StorageConfig::default()
    }
}

/// N threads write disjoint documents concurrently under
/// `SyncMode::Always`. Every put must return a version, every document
/// must be visible immediately after, and every document must survive a
/// reopen (durability came from the WAL, not the memtable).
#[test]
fn concurrent_puts_all_durable_and_visible() {
    const WRITERS: u64 = 8;
    const DOCS_PER_WRITER: u64 = 25;

    let dir = temp_dir("concurrent");
    {
        let db = Arc::new(Database::open_with_config(&dir, always_sync_config()).unwrap());

        let handles: Vec<_> = (0..WRITERS)
            .map(|w| {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    for i in 0..DOCS_PER_WRITER {
                        let id = w * DOCS_PER_WRITER + i;
                        let doc = Document::new(DocId(id))
                            .with_field("writer", Value::Int(w as i64))
                            .with_field("seq", Value::Int(i as i64));
                        let version = db.put("events", doc).unwrap();
                        assert!(version >= 1);
                        // Visible-after-durable: our own write must be
                        // readable as soon as put returns.
                        let got = db.get("events", DocId(id)).expect("just-written doc");
                        assert_eq!(got.get("writer"), Some(&Value::Int(w as i64)));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(db.count("events") as u64, WRITERS * DOCS_PER_WRITER);
        // Drop without flushing — recovery must come from the WAL.
    }

    let db = Database::open_with_config(&dir, always_sync_config()).unwrap();
    assert_eq!(
        db.count("events") as u64,
        WRITERS * DOCS_PER_WRITER,
        "all group-committed writes must survive reopen"
    );
    for id in 0..WRITERS * DOCS_PER_WRITER {
        let doc = db.get("events", DocId(id)).expect("doc lost after reopen");
        assert_eq!(
            doc.get("writer"),
            Some(&Value::Int((id / DOCS_PER_WRITER) as i64))
        );
    }

    std::fs::remove_dir_all(&dir).ok();
}

/// Concurrent writers hammering the *same* document id: versions handed
/// back must be unique (each staged write got its own version bump).
#[test]
fn concurrent_puts_same_doc_get_distinct_versions() {
    const WRITERS: usize = 8;

    let dir = temp_dir("same-doc");
    let db = Arc::new(Database::open_with_config(&dir, always_sync_config()).unwrap());

    let handles: Vec<_> = (0..WRITERS)
        .map(|w| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                let doc = Document::new(DocId(1)).with_field("writer", Value::Int(w as i64));
                db.put("singleton", doc).unwrap()
            })
        })
        .collect();

    let mut versions: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    versions.sort_unstable();
    versions.dedup();
    assert_eq!(
        versions.len(),
        WRITERS,
        "every group-committed put must get its own version"
    );

    std::fs::remove_dir_all(&dir).ok();
}

/// A transaction touching many documents commits with one batch. All of
/// it must be durable across reopen.
#[test]
fn transaction_batch_durable_across_reopen() {
    const DOCS: u64 = 50;

    let dir = temp_dir("txn-batch");
    {
        let db = Database::open_with_config(&dir, always_sync_config()).unwrap();
        // Seed a doc the transaction will delete.
        db.put("items", Document::new(DocId(999))).unwrap();

        let mut txn = db.begin_transaction();
        for i in 0..DOCS {
            let doc = Document::new(DocId(i)).with_field("n", Value::Int(i as i64));
            txn.put("items", doc).unwrap();
        }
        txn.delete("items", DocId(999)).unwrap();
        txn.commit().unwrap();
    }

    let db = Database::open_with_config(&dir, always_sync_config()).unwrap();
    assert_eq!(db.count("items") as u64, DOCS);
    assert!(db.get("items", DocId(999)).is_none());
    for i in 0..DOCS {
        assert!(db.get("items", DocId(i)).is_some(), "doc {i} lost");
    }

    std::fs::remove_dir_all(&dir).ok();
}

// ── Throughput gate (work plan 3.3): multi-writer ≥ target under Always ─

/// Multi-writer group commit must beat sequential one-fsync-per-put
/// throughput. Loose ratio: the win comes from amortising fsyncs, so
/// even a conservative bound demonstrates batching is active.
#[test]
#[ignore = "timing-sensitive throughput bench; run in release mode explicitly"]
fn multi_writer_throughput_beats_sequential() {
    const WRITERS: u64 = 8;
    const DOCS_PER_WRITER: u64 = 100;
    const TOTAL: u64 = WRITERS * DOCS_PER_WRITER;

    // Sequential baseline: one thread, one fsync per put.
    let seq_dir = temp_dir("bench-seq");
    let seq_elapsed = {
        let db = Database::open_with_config(&seq_dir, always_sync_config()).unwrap();
        let start = Instant::now();
        for id in 0..TOTAL {
            let doc = Document::new(DocId(id)).with_field("n", Value::Int(id as i64));
            db.put("bench", doc).unwrap();
        }
        start.elapsed()
    };
    std::fs::remove_dir_all(&seq_dir).ok();

    // Concurrent: same total writes across WRITERS threads — group
    // commit amortises the fsyncs.
    let par_dir = temp_dir("bench-par");
    let par_elapsed = {
        let db = Arc::new(Database::open_with_config(&par_dir, always_sync_config()).unwrap());
        let start = Instant::now();
        let handles: Vec<_> = (0..WRITERS)
            .map(|w| {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    for i in 0..DOCS_PER_WRITER {
                        let id = w * DOCS_PER_WRITER + i;
                        let doc = Document::new(DocId(id)).with_field("n", Value::Int(id as i64));
                        db.put("bench", doc).unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        start.elapsed()
    };
    std::fs::remove_dir_all(&par_dir).ok();

    let seq_tput = TOTAL as f64 / seq_elapsed.as_secs_f64();
    let par_tput = TOTAL as f64 / par_elapsed.as_secs_f64();
    println!(
        "sequential: {TOTAL} puts in {seq_elapsed:?} ({seq_tput:.0}/s), \
         concurrent x{WRITERS}: {TOTAL} puts in {par_elapsed:?} ({par_tput:.0}/s), \
         speedup {:.2}x",
        par_tput / seq_tput
    );

    assert!(
        par_tput > seq_tput * 1.5,
        "group commit should beat sequential fsync-per-put throughput: \
         sequential {seq_tput:.0}/s vs concurrent {par_tput:.0}/s"
    );
}
