//! Fuzz the WAL entry decoder (X2).
//!
//! Arbitrary bytes must never panic, over-allocate, or decode into an
//! entry that doesn't re-encode to the exact bytes consumed.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use raftdb::wal::WalEntry;

fuzz_target!(|data: &[u8]| {
    // Chained decode, mirroring recovery: keep decoding entries until
    // EOF or the first error, checking cursor progress each step.
    let mut cursor = Bytes::copy_from_slice(data);
    let mut offset = 0u64;
    loop {
        let before = cursor.len();
        match WalEntry::decode(&mut cursor, offset) {
            Ok(Some(entry)) => {
                let consumed = before - cursor.len();
                assert_eq!(
                    consumed,
                    entry.encoded_size(),
                    "decode consumed a different length than encoded_size()"
                );
                // Round-trip: a decoded entry must re-encode to the
                // exact bytes it was decoded from.
                let start = data.len() - before;
                assert_eq!(
                    entry.encode_to_vec(),
                    &data[start..start + consumed],
                    "decode/encode round-trip mismatch"
                );
                offset += consumed as u64;
            }
            Ok(None) | Err(_) => break,
        }
    }
});
