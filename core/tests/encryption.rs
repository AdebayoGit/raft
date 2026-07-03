//! Integration tests for encryption at rest (F4).
//!
//! Verifies the review-report acceptance criteria: files are unreadable
//! without the key, corrupt ciphertext returns an integrity error, and
//! the write-path performance delta stays under 15%.

use raftdb::{EncryptionKey, StorageConfig, StorageEngine};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("raft_db_encryption_tests")
        .join(name);
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn key(byte: u8) -> EncryptionKey {
    EncryptionKey::from_bytes([byte; 32])
}

fn encrypted_config(byte: u8) -> StorageConfig {
    StorageConfig {
        encryption_key: Some(key(byte)),
        // Small memtable so the test exercises flush + sstable reads.
        memtable_size: 16 * 1024,
        ..Default::default()
    }
}

fn kv(i: usize) -> (Vec<u8>, Vec<u8>) {
    (
        format!("user/profile/{i:05}").into_bytes(),
        format!("secret-document-body-{i:05}").into_bytes(),
    )
}

/// Recursively collect every regular file under `dir`.
fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(files_under(&path));
        } else {
            out.push(path);
        }
    }
    out
}

#[test]
fn encrypted_engine_round_trips_through_flush_compact_and_reopen() {
    let dir = temp_dir("round_trip");

    {
        let mut engine = StorageEngine::open(&dir, encrypted_config(0x42)).unwrap();
        for i in 0..500 {
            let (k, v) = kv(i);
            engine.put(k, v).unwrap();
        }
        // Force everything to disk and run a compaction pass.
        engine.flush().unwrap();
        engine.compact().unwrap();
        for i in 0..500 {
            let (k, v) = kv(i);
            assert_eq!(engine.get(&k).unwrap(), Some(v));
        }
    }

    // Reopen with the same key: all data readable.
    let engine = StorageEngine::open(&dir, encrypted_config(0x42)).unwrap();
    for i in 0..500 {
        let (k, v) = kv(i);
        assert_eq!(engine.get(&k).unwrap(), Some(v));
    }
}

#[test]
fn encrypted_files_contain_no_plaintext() {
    let dir = temp_dir("no_plaintext");

    let mut engine = StorageEngine::open(&dir, encrypted_config(0x42)).unwrap();
    for i in 0..500 {
        let (k, v) = kv(i);
        engine.put(k, v).unwrap();
    }
    engine.flush().unwrap();
    drop(engine);

    let needles: [&[u8]; 2] = [b"user/profile/00000", b"secret-document-body"];
    for path in files_under(&dir) {
        let raw = fs::read(&path).unwrap();
        for needle in needles {
            assert!(
                !raw.windows(needle.len()).any(|w| w == needle),
                "plaintext found in {}",
                path.display()
            );
        }
    }
}

#[test]
fn reopen_without_key_fails() {
    let dir = temp_dir("no_key");

    {
        let mut engine = StorageEngine::open(&dir, encrypted_config(0x42)).unwrap();
        let (k, v) = kv(0);
        engine.put(k, v).unwrap();
    }

    assert!(StorageEngine::open(&dir, StorageConfig::default()).is_err());
}

#[test]
fn reopen_with_wrong_key_fails() {
    let dir = temp_dir("wrong_key");

    {
        let mut engine = StorageEngine::open(&dir, encrypted_config(0x01)).unwrap();
        let (k, v) = kv(0);
        engine.put(k, v).unwrap();
    }

    assert!(StorageEngine::open(&dir, encrypted_config(0x02)).is_err());
}

#[test]
fn corrupt_sstable_ciphertext_returns_integrity_error() {
    let dir = temp_dir("corrupt_sstable");

    let mut engine = StorageEngine::open(&dir, encrypted_config(0x42)).unwrap();
    for i in 0..500 {
        let (k, v) = kv(i);
        engine.put(k, v).unwrap();
    }
    engine.flush().unwrap();
    drop(engine);

    // Flip one byte inside the first data block of every SSTable.
    let sstables: Vec<PathBuf> = files_under(&dir)
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "sst"))
        .collect();
    assert!(
        !sstables.is_empty(),
        "expected at least one flushed sstable"
    );
    for path in &sstables {
        let mut raw = fs::read(path).unwrap();
        raw[16] ^= 0xFF;
        fs::write(path, &raw).unwrap();
    }

    let engine = StorageEngine::open(&dir, encrypted_config(0x42)).unwrap();
    // At least one key that lives in a flushed table must now fail with an
    // integrity error rather than returning silently-corrupted data.
    let mut saw_error = false;
    for i in 0..500 {
        let (k, v) = kv(i);
        match engine.get(&k) {
            Ok(Some(got)) => assert_eq!(got, v, "corrupted data returned as valid"),
            Ok(None) => {}
            Err(_) => saw_error = true,
        }
    }
    assert!(saw_error, "corrupted sstable block was not detected");
}

/// Perf criterion from the review report: encryption overhead < 15% on the
/// write path. Ignored by default — timing-sensitive and only meaningful
/// with optimizations (debug builds cripple AES throughput). Run with
/// `cargo test --release --test encryption -- --ignored`.
#[test]
#[ignore = "timing-sensitive perf check; run in release mode explicitly"]
fn encryption_write_overhead_under_15_percent() {
    fn write_workload(config: StorageConfig, dir: &Path) -> std::time::Duration {
        let mut engine = StorageEngine::open(dir, config).unwrap();
        let start = Instant::now();
        for i in 0..5_000 {
            let (k, v) = kv(i);
            engine.put(k, v).unwrap();
        }
        engine.flush().unwrap();
        start.elapsed()
    }

    // SyncMode::Off isolates encryption cost from fsync noise.
    let base_config = || StorageConfig {
        wal_sync: raftdb::wal::SyncMode::Off,
        memtable_size: 256 * 1024,
        ..Default::default()
    };

    // Warm-up pass to stabilise file-system caches.
    write_workload(base_config(), &temp_dir("perf_warmup"));

    let plain = write_workload(base_config(), &temp_dir("perf_plain"));
    let encrypted = write_workload(
        StorageConfig {
            encryption_key: Some(key(0x42)),
            ..base_config()
        },
        &temp_dir("perf_encrypted"),
    );

    let ratio = encrypted.as_secs_f64() / plain.as_secs_f64();
    assert!(
        ratio < 1.15,
        "encryption overhead {:.1}% exceeds 15% (plain {plain:?}, encrypted {encrypted:?})",
        (ratio - 1.0) * 100.0
    );
}
