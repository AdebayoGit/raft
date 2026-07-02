//! Property-based invariants for HLC timestamps and WAL entry encoding (X3).

use bytes::Bytes;
use proptest::prelude::*;

use raftdb::wal::{HlcTimestamp, WalEntry, MAX_PAYLOAD_LEN};

proptest! {
    /// The `Ord` derive must order timestamps lexicographically by
    /// (physical, logical) — causality first, tie-break second.
    #[test]
    fn hlc_ordering_is_lexicographic(
        p1 in any::<u64>(), l1 in any::<u16>(),
        p2 in any::<u64>(), l2 in any::<u16>(),
    ) {
        let a = HlcTimestamp::new(p1, l1);
        let b = HlcTimestamp::new(p2, l2);
        prop_assert_eq!(a.cmp(&b), (p1, l1).cmp(&(p2, l2)));
    }

    /// encode → decode is the identity for any timestamp.
    #[test]
    fn hlc_encode_decode_roundtrip(p in any::<u64>(), l in any::<u16>()) {
        let original = HlcTimestamp::new(p, l);
        let mut buf = Vec::new();
        original.encode(&mut buf);
        prop_assert_eq!(buf.len(), HlcTimestamp::ENCODED_SIZE);
        let decoded = HlcTimestamp::decode(&mut Bytes::from(buf));
        prop_assert_eq!(decoded, original);
    }

    /// encode → decode is the identity for any WAL entry, and the
    /// checksum computed on construction verifies on decode.
    #[test]
    fn wal_entry_encode_decode_roundtrip(
        p in any::<u64>(),
        l in any::<u16>(),
        device in any::<u128>(),
        payload in prop::collection::vec(any::<u8>(), 0..512),
    ) {
        let entry = WalEntry::new(HlcTimestamp::new(p, l), device, payload);
        let encoded = entry.encode_to_vec();
        prop_assert_eq!(encoded.len(), entry.encoded_size());

        let decoded = WalEntry::decode(&mut Bytes::from(encoded), 0)
            .expect("decode failed")
            .expect("decode returned None");
        prop_assert_eq!(decoded, entry);
    }

    /// A single flipped bit anywhere in the encoding must be rejected
    /// (checksum mismatch) or change the decoded entry — never silently
    /// produce the original entry from corrupt bytes.
    #[test]
    fn wal_entry_detects_single_bitflips(
        p in any::<u64>(),
        l in any::<u16>(),
        device in any::<u128>(),
        payload in prop::collection::vec(any::<u8>(), 0..64),
        flip_bit in any::<prop::sample::Index>(),
    ) {
        let entry = WalEntry::new(HlcTimestamp::new(p, l), device, payload);
        let mut encoded = entry.encode_to_vec();
        let bit = flip_bit.index(encoded.len() * 8);
        encoded[bit / 8] ^= 1 << (bit % 8);

        // Rejection (checksum/length/incomplete error, or None) is fine;
        // only a successful decode equal to the original is a violation.
        if let Ok(Some(decoded)) = WalEntry::decode(&mut Bytes::from(encoded), 0) {
            prop_assert_ne!(decoded, entry);
        }
    }

    /// A corrupted length prefix can never force an allocation larger
    /// than `MAX_PAYLOAD_LEN`.
    #[test]
    fn wal_entry_length_prefix_is_bounded(
        p in any::<u64>(),
        l in any::<u16>(),
        device in any::<u128>(),
        bogus_len in (MAX_PAYLOAD_LEN as u32 + 1)..=u32::MAX,
    ) {
        // Hand-craft a header claiming a huge payload.
        let mut buf = Vec::new();
        HlcTimestamp::new(p, l).encode(&mut buf);
        buf.extend_from_slice(&device.to_be_bytes());
        buf.extend_from_slice(&bogus_len.to_be_bytes());

        let result = WalEntry::decode(&mut Bytes::from(buf), 0);
        prop_assert!(result.is_err(), "oversized length prefix must be rejected");
    }
}
