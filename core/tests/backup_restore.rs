//! Backup export / restore (X7).
//!
//! `Database::export_backup` writes a consistent logical snapshot of the
//! whole keyspace (documents, schemas, index definitions) while holding
//! the engine lock, so a backup taken under concurrent writers is a
//! point-in-time image — never a torn one. `Database::restore_backup`
//! rebuilds a fresh database directory from that snapshot, optionally
//! under a different storage config (e.g. re-encrypted).
#![cfg(feature = "ffi")]

use std::sync::Arc;
use std::thread;

use raftdb::index::DocId;
use raftdb::query::{Document, Filter, IndexKind, Query, Value};
use raftdb::{Database, EncryptionKey, StorageConfig};

fn temp_dir(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "raft-backup-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn seed_doc(i: u64) -> Document {
    Document::new(DocId(i))
        .with_field("n", Value::Int(i as i64))
        .with_field(
            "status",
            Value::String(if i % 2 == 0 { "even" } else { "odd" }.into()),
        )
}

/// Quiesced round-trip: documents, counts, and index definitions all
/// survive export → restore into a brand-new directory.
#[test]
fn quiesced_round_trip_preserves_docs_and_indexes() {
    const DOCS: u64 = 200;

    let src_dir = temp_dir("rt-src");
    let dst_dir = temp_dir("rt-dst");
    let backup = temp_dir("rt-file").with_extension("rftbkup");

    {
        let db = Database::open(&src_dir).unwrap();
        db.create_index("items", "status", IndexKind::Hash).unwrap();
        for i in 0..DOCS {
            db.put("items", seed_doc(i)).unwrap();
        }
        db.export_backup(&backup).unwrap();
    }

    let restored = Database::restore_backup(&backup, &dst_dir).unwrap();
    assert_eq!(restored.count("items") as u64, DOCS);
    for i in 0..DOCS {
        let doc = restored
            .get("items", DocId(i))
            .expect("doc lost in restore");
        assert_eq!(doc.get("n"), Some(&Value::Int(i as i64)));
    }

    // Index definitions come back too: an equality query on the indexed
    // field must plan a hash lookup, not a full scan.
    let plan = restored.explain(
        &Query::collection("items").filter(Filter::eq("status", Value::String("even".into()))),
    );
    assert!(
        format!("{plan:?}").contains("HashLookup"),
        "restored db should still use the hash index, got {plan:?}"
    );
    let evens = restored.query(
        &Query::collection("items").filter(Filter::eq("status", Value::String("even".into()))),
    );
    assert_eq!(evens.len() as u64, DOCS / 2);

    std::fs::remove_dir_all(&src_dir).ok();
    std::fs::remove_dir_all(&dst_dir).ok();
    std::fs::remove_file(&backup).ok();
}

/// Restore refuses to merge into a directory that already has data.
#[test]
fn restore_refuses_non_empty_target() {
    let src_dir = temp_dir("refuse-src");
    let dst_dir = temp_dir("refuse-dst");
    let backup = temp_dir("refuse-file").with_extension("rftbkup");

    {
        let db = Database::open(&src_dir).unwrap();
        db.put("items", seed_doc(1)).unwrap();
        db.export_backup(&backup).unwrap();
    }
    {
        // Occupy the target with an existing database.
        let db = Database::open(&dst_dir).unwrap();
        db.put("other", seed_doc(9)).unwrap();
    }

    let err = Database::restore_backup(&backup, &dst_dir);
    assert!(err.is_err(), "restore into non-empty dir must fail");

    std::fs::remove_dir_all(&src_dir).ok();
    std::fs::remove_dir_all(&dst_dir).ok();
    std::fs::remove_file(&backup).ok();
}

/// A plaintext snapshot restored with an encryption key produces an
/// encrypted database with identical logical contents.
#[test]
fn restore_into_encrypted_config() {
    const DOCS: u64 = 50;

    let src_dir = temp_dir("enc-src");
    let dst_dir = temp_dir("enc-dst");
    let backup = temp_dir("enc-file").with_extension("rftbkup");
    let key = EncryptionKey::from_bytes([7u8; 32]);
    let enc_config = StorageConfig {
        encryption_key: Some(key.clone()),
        ..StorageConfig::default()
    };

    {
        let db = Database::open(&src_dir).unwrap();
        for i in 0..DOCS {
            db.put("items", seed_doc(i)).unwrap();
        }
        db.export_backup(&backup).unwrap();
    }

    {
        let restored =
            Database::restore_backup_with_config(&backup, &dst_dir, enc_config.clone()).unwrap();
        assert_eq!(restored.count("items") as u64, DOCS);
    }
    // Reopen with the key: data must still be there (i.e. it really went
    // through the encrypted storage path).
    let reopened = Database::open_with_config(&dst_dir, enc_config).unwrap();
    assert_eq!(reopened.count("items") as u64, DOCS);
    assert_eq!(
        reopened.get("items", DocId(3)).unwrap().get("n"),
        Some(&Value::Int(3))
    );

    std::fs::remove_dir_all(&src_dir).ok();
    std::fs::remove_dir_all(&dst_dir).ok();
    std::fs::remove_file(&backup).ok();
}

/// Acceptance (work plan 3.5): snapshot restore round-trip under
/// concurrent writes. Writers hammer the database while a backup is
/// taken mid-flight. The restored database must be a consistent
/// point-in-time image: every restored document is complete and valid,
/// and the count lies between the writes known-complete before the
/// export and the total written.
#[test]
fn round_trip_under_concurrent_writes() {
    const WRITERS: u64 = 4;
    const DOCS_PER_WRITER: u64 = 200;
    const PRE_SEEDED: u64 = 100;

    let src_dir = temp_dir("conc-src");
    let dst_dir = temp_dir("conc-dst");
    let backup = temp_dir("conc-file").with_extension("rftbkup");

    let db = Arc::new(Database::open(&src_dir).unwrap());

    // Writes guaranteed complete before the backup starts.
    for i in 0..PRE_SEEDED {
        db.put("events", seed_doc(i)).unwrap();
    }

    let handles: Vec<_> = (0..WRITERS)
        .map(|w| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for i in 0..DOCS_PER_WRITER {
                    let id = PRE_SEEDED + w * DOCS_PER_WRITER + i;
                    db.put("events", seed_doc(id)).unwrap();
                }
            })
        })
        .collect();

    // Export while the writers are running.
    db.export_backup(&backup).unwrap();

    for h in handles {
        h.join().unwrap();
    }
    let total = PRE_SEEDED + WRITERS * DOCS_PER_WRITER;
    assert_eq!(db.count("events") as u64, total);

    let restored = Database::restore_backup(&backup, &dst_dir).unwrap();
    let restored_count = restored.count("events") as u64;
    assert!(
        (PRE_SEEDED..=total).contains(&restored_count),
        "restored count {restored_count} outside [{PRE_SEEDED}, {total}]"
    );

    // Every restored document must be complete — both fields intact and
    // consistent with its id (no torn/partial documents).
    let docs = restored.query(&Query::collection("events"));
    assert_eq!(docs.len() as u64, restored_count);
    for doc in &docs {
        let id = doc.id.0;
        assert_eq!(doc.get("n"), Some(&Value::Int(id as i64)), "torn doc {id}");
        let expected = if id % 2 == 0 { "even" } else { "odd" };
        assert_eq!(
            doc.get("status"),
            Some(&Value::String(expected.into())),
            "torn doc {id}"
        );
    }
    // All pre-seeded docs preceded the export, so they must be present.
    for i in 0..PRE_SEEDED {
        assert!(
            restored.get("events", DocId(i)).is_some(),
            "pre-seeded doc {i} missing from snapshot"
        );
    }

    std::fs::remove_dir_all(&src_dir).ok();
    std::fs::remove_dir_all(&dst_dir).ok();
    std::fs::remove_file(&backup).ok();
}
