//! Phase 2 exit-gate verification (WORK_PLAN §Phase 2 Exit Gate).
//!
//! Three measurable gates live here:
//!   1. Indexed equality query ≥ 100× faster than a full scan on 100k docs
//!      (`#[ignore]` — timing-sensitive, run explicitly).
//!   2. Bounded RSS for reads and compaction on a ~1 GiB dataset
//!      (`#[ignore]` — heavy, run explicitly).
//!   3. 100 live-query subscribers under a write storm complete in time
//!      only possible with incremental (non-rescanning) evaluation.
//!
//! Run the ignored gates with:
//!   cargo test --features ffi --test phase2_exit_gate -- --ignored --nocapture
#![cfg(feature = "ffi")]

use std::time::{Duration, Instant};

use raftdb::index::DocId;
use raftdb::query::{Document, Filter, IndexKind, Query, ScanStrategy, Value};
use raftdb::wal::SyncMode;
use raftdb::{Database, StorageConfig, StorageEngine};

fn temp_dir(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "raft-p2-gate-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// Fast-load config: gates measure query/compaction behaviour, not fsync.
fn load_config() -> StorageConfig {
    StorageConfig {
        wal_sync: SyncMode::Off,
        ..StorageConfig::default()
    }
}

// ── Gate 1: indexed query ≥ 100× faster than full scan on 100k docs ────

#[test]
#[ignore = "timing-sensitive 100k-doc benchmark; run explicitly"]
fn indexed_query_100x_faster_than_full_scan_on_100k_docs() {
    const DOCS: u64 = 100_000;
    const BUCKETS: i64 = 1_000; // → ~100 matches per bucket
    const RUNS: u32 = 10;

    let dir = temp_dir("index-bench");
    let db = Database::open_with_config(&dir, load_config()).unwrap();

    for i in 0..DOCS {
        let doc = Document::new(DocId(i))
            .with_field("bucket", Value::Int((i as i64) % BUCKETS))
            .with_field("name", Value::String(format!("doc-{i}")));
        db.put("items", doc).unwrap();
    }

    let query = Query::collection("items").filter(Filter::eq("bucket", Value::Int(42)));

    // Baseline: planner must fall back to a full scan without an index.
    assert!(matches!(
        db.explain(&query).strategy,
        ScanStrategy::FullScan
    ));
    let full_scan = time_query(&db, &query, RUNS);

    // Indexed: planner must pick the index.
    db.create_index("items", "bucket", IndexKind::Hash).unwrap();
    assert!(!matches!(
        db.explain(&query).strategy,
        ScanStrategy::FullScan
    ));
    let indexed = time_query(&db, &query, RUNS);

    let speedup = full_scan.as_secs_f64() / indexed.as_secs_f64();
    println!(
        "full scan: {full_scan:?} / {RUNS} runs, indexed: {indexed:?} / {RUNS} runs, \
         speedup: {speedup:.1}x"
    );
    assert!(
        speedup >= 100.0,
        "exit gate requires >= 100x speedup, measured {speedup:.1}x"
    );

    std::fs::remove_dir_all(&dir).ok();
}

fn time_query(db: &Database, query: &Query, runs: u32) -> Duration {
    // Verify the result set once so both strategies are known-correct.
    let results = db.query(query);
    assert_eq!(results.len(), 100);

    let start = Instant::now();
    for _ in 0..runs {
        let results = db.query(query);
        std::hint::black_box(&results);
    }
    start.elapsed()
}

// ── Gate 2: bounded RSS on a ~1 GiB dataset (reads + compaction) ────────

#[test]
#[ignore = "writes ~1 GiB to disk; run explicitly"]
fn bounded_rss_for_reads_and_compaction_on_1gb_dataset() {
    const KEYS: u64 = 262_144; // × 4 KiB values ≈ 1 GiB
    const VALUE_SIZE: usize = 4096;
    const RSS_CAP_KB: u64 = 512 * 1024; // 512 MiB

    let dir = temp_dir("rss-1gb");
    let mut engine = StorageEngine::open(&dir, load_config()).unwrap();

    let mut value = vec![0xABu8; VALUE_SIZE];
    for i in 0..KEYS {
        value[..8].copy_from_slice(&i.to_be_bytes());
        engine
            .put(format!("key-{i:012}").into_bytes(), value.clone())
            .unwrap();
    }
    engine.flush().unwrap();
    println!("after load:       rss = {} KiB", current_rss_kb());

    engine.compact().unwrap();
    let rss_after_compact = current_rss_kb();
    println!("after compaction: rss = {rss_after_compact} KiB");
    assert!(
        rss_after_compact < RSS_CAP_KB,
        "compaction RSS {rss_after_compact} KiB exceeds cap {RSS_CAP_KB} KiB"
    );

    // Random point reads across the whole keyspace: block-level reads plus
    // the byte-capped block cache must keep RSS bounded.
    let stride: u64 = 7919;
    let mut idx: u64 = 0;
    for _ in 0..20_000 {
        idx = (idx + stride) % KEYS;
        let key = format!("key-{idx:012}");
        let got = engine.get(key.as_bytes()).unwrap();
        assert_eq!(got.expect("key must exist")[..8], idx.to_be_bytes());
    }
    let rss_after_reads = current_rss_kb();
    println!("after reads:      rss = {rss_after_reads} KiB");
    assert!(
        rss_after_reads < RSS_CAP_KB,
        "read RSS {rss_after_reads} KiB exceeds cap {RSS_CAP_KB} KiB"
    );

    drop(engine);
    std::fs::remove_dir_all(&dir).ok();
}

/// Current process RSS in KiB via `ps` (portable across macOS/Linux, no
/// extra dependency).
fn current_rss_kb() -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps must be available");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .expect("ps rss output must be numeric")
}

// ── Gate 3: 100 live-query subscribers, bounded CPU per write ──────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hundred_subscribers_survive_write_storm_within_bound() {
    const SEED_DOCS: u64 = 5_000;
    const SUBSCRIBERS: usize = 100;
    const WRITES: u64 = 1_000;
    const SENTINEL: DocId = DocId(9_999_999);
    // Incremental evaluation does SUBSCRIBERS × WRITES single-doc checks
    // (~1e5). O(subs × collection) rescans would need ~5e8 predicate
    // evaluations and blow far past this bound.
    const BOUND: Duration = Duration::from_secs(20);

    let dir = temp_dir("live-stress");
    let db = std::sync::Arc::new(Database::open_with_config(&dir, load_config()).unwrap());

    for i in 0..SEED_DOCS {
        let doc = Document::new(DocId(i))
            .with_field("bucket", Value::Int((i % 100) as i64))
            .with_field("n", Value::Int(0));
        db.put("items", doc).unwrap();
    }

    let start = Instant::now();

    // 100 subscribers, all filtered (no sort/limit → incremental path).
    let mut consumers = Vec::with_capacity(SUBSCRIBERS);
    for _ in 0..SUBSCRIBERS {
        let query = Query::collection("items").filter(Filter::eq("bucket", Value::Int(7)));
        let (_initial, mut live) = db.live_query(query);
        consumers.push(tokio::spawn(async move {
            let mut updates_seen = 0u64;
            while let Some(diff) = live.next_diff().await {
                updates_seen += diff.updated.len() as u64;
                if diff.added.iter().any(|d| d.id == SENTINEL) {
                    return updates_seen;
                }
            }
            panic!("bus closed before sentinel was observed");
        }));
    }

    // Write storm: repeatedly update one matching doc, then insert a
    // sentinel that also matches so every subscriber can terminate.
    let writer_db = std::sync::Arc::clone(&db);
    tokio::task::spawn_blocking(move || {
        for i in 0..WRITES {
            let doc = Document::new(DocId(7))
                .with_field("bucket", Value::Int(7))
                .with_field("n", Value::Int(i as i64 + 1));
            writer_db.put("items", doc).unwrap();
        }
        let sentinel = Document::new(SENTINEL)
            .with_field("bucket", Value::Int(7))
            .with_field("n", Value::Int(-1));
        writer_db.put("items", sentinel).unwrap();
    })
    .await
    .unwrap();

    for consumer in consumers {
        let updates_seen = consumer.await.unwrap();
        // Lag fallbacks may coalesce updates, but every subscriber must
        // have observed real progress before the sentinel.
        assert!(updates_seen > 0, "subscriber saw no updates");
    }

    let elapsed = start.elapsed();
    println!("100 subscribers × {WRITES} writes drained in {elapsed:?}");
    assert!(
        elapsed < BOUND,
        "stress run took {elapsed:?}, exceeding the {BOUND:?} bound"
    );

    std::fs::remove_dir_all(&dir).ok();
}
