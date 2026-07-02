//! Fuzz the manifest record decoder (X2).
//!
//! Arbitrary bytes must never panic, over-allocate (bounded length
//! prefix), or loop without making progress.

#![no_main]

use libfuzzer_sys::fuzz_target;
use raftdb::manifest::ManifestRecord;

fuzz_target!(|data: &[u8]| {
    let mut cursor = data;
    let mut offset = 0u64;
    loop {
        let before = cursor.len();
        match ManifestRecord::decode(&mut cursor, offset) {
            Ok(Some(record)) => {
                let consumed = before - cursor.len();
                assert!(consumed > 0, "decode succeeded without consuming bytes");
                // Round-trip: re-encoding must reproduce the consumed bytes.
                let start = data.len() - before;
                assert_eq!(
                    record.encode(),
                    &data[start..start + consumed],
                    "decode/encode round-trip mismatch"
                );
                offset += consumed as u64;
            }
            Ok(None) | Err(_) => break,
        }
    }
});
