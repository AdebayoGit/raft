//! Fuzz the SSTable reader against arbitrary file contents (X2).
//!
//! A corrupt or truncated SSTable file must be rejected with an error —
//! never a panic, unbounded allocation, or garbage read.

#![no_main]

use std::fs;
use std::path::PathBuf;

use libfuzzer_sys::fuzz_target;
use raftdb::sstable::SSTableReader;

/// One scratch file per process; overwritten every iteration.
fn scratch_file() -> PathBuf {
    let dir = std::env::temp_dir().join("raft_db_fuzz_sstable");
    fs::create_dir_all(&dir).expect("create scratch dir");
    dir.join(format!("{}.sst", std::process::id()))
}

fuzz_target!(|data: &[u8]| {
    let path = scratch_file();
    fs::write(&path, data).expect("write scratch sstable");

    // Open may fail (expected for corrupt input) but must not panic.
    if let Ok(reader) = SSTableReader::open(&path) {
        // If the file parses, every read path must stay panic-free.
        let _ = reader.entry_count();
        let _ = reader.scan_all();
        let _ = reader.get(b"");
        let _ = reader.get(data.get(..data.len().min(16)).unwrap_or(b""));
        let _ = reader.scan(b"", None);
    }
});
