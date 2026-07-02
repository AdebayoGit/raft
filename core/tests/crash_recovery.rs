//! Crash-injection recovery harness (X1).
//!
//! Simulates power loss deterministically: the WAL file is truncated at
//! every possible byte offset and corrupted at every byte position, and
//! the engine's data directory is snapshotted mid-workload ("kill") and
//! reopened ("restart"). The invariant under test is always the same:
//!
//! * every acknowledged (fsynced) write before the crash point is
//!   recovered intact — no lost writes;
//! * nothing beyond the valid prefix is ever resurrected — no phantom
//!   or corrupted entries;
//! * the recovered log/database accepts new writes cleanly.

use std::fs;
use std::path::{Path, PathBuf};

use raftdb::wal::{HlcTimestamp, SyncMode, Wal, WalEntry};
use raftdb::{StorageConfig, StorageEngine};

/// Fresh scratch directory for one test, wiped on entry.
fn scratch_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("raft_db_crash_tests")
        .join(format!("{}-{name}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn make_entry(physical: u64, logical: u16, payload: &[u8]) -> WalEntry {
    WalEntry::new(
        HlcTimestamp::new(physical, logical),
        0x0123_4567_89AB_CDEF_FEDC_BA98_7654_3210u128,
        payload.to_vec(),
    )
}

/// Recursively copy a database directory — the "kill -9" snapshot.
fn copy_dir_recursive(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for entry in fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let target = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir_recursive(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// Config used by the engine-level scenarios: fsync on every append, no
/// preallocation (so the WAL file contains exactly the written bytes).
fn crash_test_config() -> StorageConfig {
    StorageConfig {
        wal_sync: SyncMode::Always,
        wal_preallocate: 0,
        ..StorageConfig::default()
    }
}

// ── WAL-level fault injection ──────────────────────────────────────────

/// Write a small log and record the byte offset of every entry boundary.
/// Returns (entries, boundaries, full file bytes).
fn build_reference_log(dir: &Path) -> (Vec<WalEntry>, Vec<usize>, Vec<u8>) {
    let path = dir.join("reference.wal");
    let entries: Vec<WalEntry> = (0..6)
        .map(|i| {
            make_entry(
                1_000 + i,
                i as u16,
                format!("acknowledged-write-{i}").as_bytes(),
            )
        })
        .collect();

    let mut wal = Wal::open_with_mode(&path, SyncMode::Always).unwrap();
    for e in &entries {
        wal.append(e).unwrap();
    }

    // boundaries[k] = offset where entry k ends (boundaries[0] = 0).
    let mut boundaries = vec![0usize];
    for e in &entries {
        boundaries.push(boundaries.last().unwrap() + e.encoded_size());
    }

    let data = fs::read(&path).unwrap();
    assert_eq!(data.len(), *boundaries.last().unwrap());
    (entries, boundaries, data)
}

/// Number of complete entries that fit within `cut` bytes.
fn entries_within(boundaries: &[usize], cut: usize) -> usize {
    boundaries.iter().filter(|&&b| b <= cut).count() - 1
}

/// A crash can tear the last write at any byte. For every possible
/// truncation point, recovery must return exactly the fully-written
/// prefix and leave a log that accepts new appends cleanly.
#[test]
fn wal_recovers_from_truncation_at_every_byte_offset() {
    let dir = scratch_dir("wal_truncate_sweep");
    let (entries, boundaries, data) = build_reference_log(&dir);
    let crash_path = dir.join("crash.wal");

    for cut in 0..=data.len() {
        fs::write(&crash_path, &data[..cut]).unwrap();

        let mut wal = Wal::open_with_mode(&crash_path, SyncMode::Always).unwrap();
        let (recovered, stats) = wal.recover().unwrap();

        let expected = entries_within(&boundaries, cut);
        assert_eq!(
            recovered,
            entries[..expected],
            "cut at byte {cut}: recovered set must be exactly the durable prefix"
        );
        assert_eq!(stats.entries_recovered, expected, "cut at byte {cut}");
        if cut == boundaries[expected] {
            assert_eq!(
                stats.truncated_at, None,
                "cut at byte {cut}: clean boundary must not report damage"
            );
        }

        // The recovered log must accept new writes and replay cleanly.
        let fresh = make_entry(9_999, 0, b"post-recovery-write");
        wal.append(&fresh).unwrap();
        let replayed: Vec<WalEntry> = wal
            .replay()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|e| panic!("cut at byte {cut}: replay after recovery failed: {e}"));
        let mut expected_log = entries[..expected].to_vec();
        expected_log.push(fresh);
        assert_eq!(replayed, expected_log, "cut at byte {cut}");
    }
}

/// A single flipped bit anywhere in the log must never surface a phantom
/// or altered entry: recovery keeps exactly the entries preceding the
/// corrupted one.
#[test]
fn wal_recovers_from_single_bitflip_at_every_byte_offset() {
    let dir = scratch_dir("wal_bitflip_sweep");
    let (entries, boundaries, data) = build_reference_log(&dir);
    let crash_path = dir.join("crash.wal");

    for offset in 0..data.len() {
        let mut corrupted = data.clone();
        corrupted[offset] ^= 1 << (offset % 8);
        fs::write(&crash_path, &corrupted).unwrap();

        let mut wal = Wal::open_with_mode(&crash_path, SyncMode::Always).unwrap();
        let (recovered, _stats) = wal.recover().unwrap();

        // Index of the entry containing the flipped byte.
        let damaged_idx = entries_within(&boundaries, offset);
        assert_eq!(
            recovered,
            entries[..damaged_idx],
            "bitflip at byte {offset}: recovery must keep only the intact prefix"
        );

        // Damaged bytes are truncated away — the log stays writable.
        let fresh = make_entry(9_999, 0, b"post-corruption-write");
        wal.append(&fresh).unwrap();
        let replayed: Vec<WalEntry> = wal
            .replay()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|e| panic!("bitflip at byte {offset}: replay failed: {e}"));
        let mut expected_log = entries[..damaged_idx].to_vec();
        expected_log.push(fresh);
        assert_eq!(replayed, expected_log, "bitflip at byte {offset}");
    }
}

// ── Engine-level kill-and-reopen ───────────────────────────────────────

/// Kill-and-reopen loop: after every acknowledged put (and a mid-run
/// flush), snapshot the whole db directory and reopen it as if the
/// process had been killed. Every acknowledged write must be readable.
#[test]
fn engine_recovers_every_acknowledged_write_after_kill() {
    let live_dir = scratch_dir("engine_kill_loop");
    let mut engine = StorageEngine::open(&live_dir, crash_test_config()).unwrap();

    const WRITES: usize = 20;
    for i in 0..WRITES {
        let key = format!("key-{i:03}").into_bytes();
        let value = format!("value-{i:03}").into_bytes();
        engine.put(key, value).unwrap(); // acknowledged: fsynced (SyncMode::Always)

        // Exercise the SSTable + manifest path halfway through.
        if i == WRITES / 2 {
            engine.flush().unwrap();
        }

        // "kill -9": snapshot the directory exactly as it is on disk.
        let snapshot = live_dir.with_file_name(format!(
            "{}-snap-{i:03}",
            live_dir.file_name().unwrap().to_string_lossy()
        ));
        let _ = fs::remove_dir_all(&snapshot);
        copy_dir_recursive(&live_dir, &snapshot);

        // "restart": reopen and verify every acknowledged write.
        let reopened = StorageEngine::open(&snapshot, crash_test_config()).unwrap();
        for j in 0..=i {
            let key = format!("key-{j:03}").into_bytes();
            let got = reopened.get(&key).unwrap();
            assert_eq!(
                got,
                Some(format!("value-{j:03}").into_bytes()),
                "write {j} lost after crash at write {i}"
            );
        }
        fs::remove_dir_all(&snapshot).ok();
    }
}

/// Deletes are acknowledged writes too: a crash after a delete must not
/// resurrect the old value on restart.
#[test]
fn engine_does_not_resurrect_deleted_keys_after_kill() {
    let live_dir = scratch_dir("engine_delete_kill");
    let mut engine = StorageEngine::open(&live_dir, crash_test_config()).unwrap();

    engine
        .put(b"keep".to_vec(), b"kept-value".to_vec())
        .unwrap();
    engine
        .put(b"drop".to_vec(), b"doomed-value".to_vec())
        .unwrap();
    engine.flush().unwrap(); // old value now lives in an SSTable
    engine.delete(b"drop".to_vec()).unwrap(); // tombstone only in the WAL

    let snapshot = scratch_dir("engine_delete_kill_snap");
    copy_dir_recursive(&live_dir, &snapshot);

    let reopened = StorageEngine::open(&snapshot, crash_test_config()).unwrap();
    assert_eq!(reopened.get(b"keep").unwrap(), Some(b"kept-value".to_vec()));
    assert_eq!(
        reopened.get(b"drop").unwrap(),
        None,
        "crash after acknowledged delete must not resurrect the value"
    );
}

/// A torn write at the WAL tail (garbage bytes after the last fsynced
/// entry) must not take down the database or lose earlier writes.
#[test]
fn engine_survives_torn_wal_tail_after_kill() {
    let live_dir = scratch_dir("engine_torn_tail");
    let mut engine = StorageEngine::open(&live_dir, crash_test_config()).unwrap();

    for i in 0..5 {
        engine
            .put(
                format!("stable-{i}").into_bytes(),
                format!("payload-{i}").into_bytes(),
            )
            .unwrap();
    }

    let snapshot = scratch_dir("engine_torn_tail_snap");
    copy_dir_recursive(&live_dir, &snapshot);

    // Simulate a torn write in flight when power was cut: half an entry
    // plus junk appended past the last fsynced byte.
    let torn = make_entry(42, 0, b"never-acknowledged").encode_to_vec();
    let wal_path = snapshot.join("wal.log");
    let mut bytes = fs::read(&wal_path).unwrap();
    bytes.extend_from_slice(&torn[..torn.len() / 2]);
    bytes.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    fs::write(&wal_path, &bytes).unwrap();

    let mut reopened = StorageEngine::open(&snapshot, crash_test_config()).unwrap();
    for i in 0..5 {
        assert_eq!(
            reopened.get(format!("stable-{i}").as_bytes()).unwrap(),
            Some(format!("payload-{i}").into_bytes()),
            "acknowledged write {i} lost to torn tail"
        );
    }
    assert_eq!(
        reopened.get(b"never-acknowledged").unwrap(),
        None,
        "unacknowledged torn entry must not surface"
    );

    // The recovered database keeps working: write, kill again, reopen.
    reopened
        .put(b"after-recovery".to_vec(), b"still-works".to_vec())
        .unwrap();
    let second = scratch_dir("engine_torn_tail_snap2");
    copy_dir_recursive(&snapshot, &second);
    let reopened_twice = StorageEngine::open(&second, crash_test_config()).unwrap();
    assert_eq!(
        reopened_twice.get(b"after-recovery").unwrap(),
        Some(b"still-works".to_vec())
    );
}
