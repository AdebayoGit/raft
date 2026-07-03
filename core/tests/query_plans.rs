//! Query planner v2 acceptance (Q6b, Q6c): golden plan snapshots against
//! a real `Database`, plus a regression bench behind `#[ignore]`.
//!
//! Run the regression bench explicitly, in release mode:
//!   cargo test --release --features ffi --test query_plans -- --ignored --nocapture
#![cfg(feature = "ffi")]

use std::time::Instant;

use raftdb::index::DocId;
use raftdb::query::{Document, Filter, IndexKind, Query, QueryPlan, ScanStrategy, Value};
use raftdb::Database;

fn temp_dir(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "raft-query-plans-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// A small, fully indexed collection: `status` (hash) and `age` (btree).
fn seed_db(dir: &std::path::Path, docs: u64) -> Database {
    let db = Database::open(dir).unwrap();
    for i in 0..docs {
        let status = if i % 3 == 0 { "active" } else { "inactive" };
        let doc = Document::new(DocId(i))
            .with_field("status", Value::String(status.into()))
            .with_field("age", Value::Int((18 + i % 60) as i64));
        db.put("users", doc).unwrap();
    }
    db.create_index("users", "status", IndexKind::Hash).unwrap();
    db.create_index("users", "age", IndexKind::BTree).unwrap();
    db
}

// ── Golden plan snapshots via Database::explain ─────────────────────────

#[test]
fn golden_plans_through_database_explain() {
    let dir = temp_dir("golden");
    let db = seed_db(&dir, 90);

    // Eq on the hash-indexed field → point lookup, cost from entry_count.
    let q = Query::collection("users").filter(Filter::eq("status", Value::String("active".into())));
    assert_eq!(
        db.explain(&q),
        QueryPlan {
            strategy: ScanStrategy::HashLookup {
                field: "status".into(),
                key: Value::String("active".into()).to_index_bytes(),
            },
            estimated_cost: 1, // eq_cost(90 entries)
        }
    );

    // Range on the btree-indexed field → range scan, entry_count-based.
    let q = Query::collection("users").filter(Filter::gte("age", Value::Int(40)));
    assert_eq!(
        db.explain(&q),
        QueryPlan {
            strategy: ScanStrategy::BTreeRange {
                field: "age".into(),
                start: Some(Value::Int(40).to_index_bytes()),
                start_inclusive: true,
                end: None,
                end_inclusive: false,
            },
            estimated_cost: 30, // 90 entries / RANGE_SELECTIVITY
        }
    );

    // Same-field Or of equalities → union of hash lookups (Q6c).
    let q = Query::collection("users").filter(Filter::or(vec![
        Filter::eq("status", Value::String("active".into())),
        Filter::eq("status", Value::String("inactive".into())),
    ]));
    assert_eq!(
        db.explain(&q),
        QueryPlan {
            strategy: ScanStrategy::HashUnion {
                field: "status".into(),
                keys: vec![
                    Value::String("active".into()).to_index_bytes(),
                    Value::String("inactive".into()).to_index_bytes(),
                ],
            },
            estimated_cost: 2,
        }
    );

    // Unindexed field → full scan costed at the collection size.
    let q = Query::collection("users").filter(Filter::eq("nickname", Value::Int(1)));
    assert_eq!(
        db.explain(&q),
        QueryPlan {
            strategy: ScanStrategy::FullScan,
            estimated_cost: 90,
        }
    );

    std::fs::remove_dir_all(&dir).ok();
}

/// The union plan must return exactly what the equivalent full scan
/// returns — plan choice must never change results.
#[test]
fn hash_union_results_match_full_scan_semantics() {
    let dir = temp_dir("union-semantics");
    let db = seed_db(&dir, 60);

    let filter = Filter::or(vec![
        Filter::eq("status", Value::String("active".into())),
        Filter::eq("status", Value::String("inactive".into())),
    ]);
    let q = Query::collection("users").filter(filter);
    assert!(matches!(
        db.explain(&q).strategy,
        ScanStrategy::HashUnion { .. }
    ));

    let mut got: Vec<u64> = db.query(&q).iter().map(|d| d.id.0).collect();
    got.sort_unstable();
    let expected: Vec<u64> = (0..60).collect();
    assert_eq!(got, expected, "union of both statuses covers every doc");

    std::fs::remove_dir_all(&dir).ok();
}

/// Sorted-with-limit results stay deterministic after the Q6b change
/// (ids now arrive ascending from the BTreeMap, not a per-query sort).
#[test]
fn sorted_limit_is_deterministic_across_runs() {
    let dir = temp_dir("determinism");
    let db = seed_db(&dir, 50);

    let q = Query::collection("users")
        .filter(Filter::gte("age", Value::Int(18)))
        .sort(raftdb::query::Sort::asc("age"))
        .limit(10);

    let first: Vec<u64> = db.query(&q).iter().map(|d| d.id.0).collect();
    for _ in 0..5 {
        let again: Vec<u64> = db.query(&q).iter().map(|d| d.id.0).collect();
        assert_eq!(first, again, "identical query must return identical order");
    }

    std::fs::remove_dir_all(&dir).ok();
}

// ── Regression bench (work plan 3.4 acceptance) ─────────────────────────

/// The Or-union plan must beat forcing a full scan on the same predicate.
#[test]
#[ignore = "timing-sensitive regression bench; run in release mode explicitly"]
fn or_union_beats_full_scan() {
    const DOCS: u64 = 20_000;
    const RUNS: u32 = 50;

    let dir = temp_dir("bench");
    let db = seed_db(&dir, DOCS);

    // Indexed: same-field Or served by the hash union plan.
    let indexed = Query::collection("users").filter(Filter::or(vec![
        Filter::eq("status", Value::String("active".into())),
        Filter::eq("status", Value::String("missing".into())),
    ]));
    assert!(matches!(
        db.explain(&indexed).strategy,
        ScanStrategy::HashUnion { .. }
    ));

    // Full scan: same selectivity, but on a field with no index.
    let scanned = Query::collection("users").filter(Filter::or(vec![
        Filter::eq("status2", Value::String("x".into())),
        Filter::contains("status", Value::String("activ".into())),
    ]));
    assert!(matches!(
        db.explain(&scanned).strategy,
        ScanStrategy::FullScan
    ));

    let start = Instant::now();
    for _ in 0..RUNS {
        std::hint::black_box(db.query(&indexed));
    }
    let union_elapsed = start.elapsed();

    let start = Instant::now();
    for _ in 0..RUNS {
        std::hint::black_box(db.query(&scanned));
    }
    let scan_elapsed = start.elapsed();

    println!("hash-union: {union_elapsed:?} vs full scan: {scan_elapsed:?} over {RUNS} runs");
    assert!(
        union_elapsed < scan_elapsed,
        "union plan should beat a full scan: {union_elapsed:?} vs {scan_elapsed:?}"
    );
}
