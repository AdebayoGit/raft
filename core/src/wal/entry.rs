use bytes::{Buf, BufMut};

/// Hybrid Logical Clock timestamp providing causal ordering without clock sync.
///
/// Combines a physical wall-clock component (milliseconds since epoch) with a
/// logical counter to distinguish events that occur within the same millisecond.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub struct HlcTimestamp {
    /// Milliseconds since Unix epoch.
    pub physical: u64,
    /// Logical counter for ordering within the same physical tick.
    pub logical: u16,
}

impl HlcTimestamp {
    /// Encoded size in bytes: 8 (physical) + 2 (logical).
    pub const ENCODED_SIZE: usize = 8 + 2;

    pub fn new(physical: u64, logical: u16) -> Self {
        Self { physical, logical }
    }

    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u64(self.physical);
        buf.put_u16(self.logical);
    }

    pub fn decode(buf: &mut impl Buf) -> Self {
        let physical = buf.get_u64();
        let logical = buf.get_u16();
        Self { physical, logical }
    }
}

/// A single entry in the write-ahead log.
///
/// Binary layout (all fields big-endian):
/// ```text
/// ┌──────────┬─────────┬───────────┬──────────────┬─────────┬──────────┐
/// │ physical │ logical │ device_id │ payload_len  │ payload │ checksum │
/// │  8 bytes │ 2 bytes │ 16 bytes  │   4 bytes    │ N bytes │ 4 bytes  │
/// └──────────┴─────────┴───────────┴──────────────┴─────────┴──────────┘
/// ```
///
/// The checksum covers everything except itself (timestamp + device_id +
/// payload_len + payload).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalEntry {
    /// HLC timestamp for causal ordering.
    pub timestamp: HlcTimestamp,
    /// UUID of the originating device (128-bit, big-endian).
    pub device_id: u128,
    /// Opaque payload bytes (the mutation).
    pub payload: Vec<u8>,
    /// CRC-32 checksum of all preceding fields.
    pub checksum: u32,
}

/// Fixed overhead per entry excluding the variable-length payload:
/// HLC (10) + device_id (16) + payload_len (4) + checksum (4) = 34 bytes.
const ENTRY_OVERHEAD: usize = HlcTimestamp::ENCODED_SIZE + 16 + 4 + 4;

impl WalEntry {
    /// Creates a new entry, computing the checksum automatically.
    pub fn new(timestamp: HlcTimestamp, device_id: u128, payload: Vec<u8>) -> Self {
        let checksum = Self::compute_checksum(&timestamp, device_id, &payload);
        Self {
            timestamp,
            device_id,
            payload,
            checksum,
        }
    }

    /// Total encoded size in bytes.
    pub fn encoded_size(&self) -> usize {
        ENTRY_OVERHEAD + self.payload.len()
    }

    /// Encode this entry into the provided buffer.
    pub fn encode(&self, buf: &mut impl BufMut) {
        self.timestamp.encode(buf);
        buf.put_u128(self.device_id);
        buf.put_u32(self.payload.len() as u32);
        buf.put_slice(&self.payload);
        buf.put_u32(self.checksum);
    }

    /// Encode this entry into a new `Vec<u8>`.
    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_size());
        self.encode(&mut buf);
        buf
    }

    /// Decode an entry from a buffer, verifying the checksum.
    ///
    /// Returns `None` if the buffer doesn't contain enough bytes for the
    /// fixed header. Returns an error on checksum mismatch.
    pub fn decode(buf: &mut impl Buf, offset: u64) -> Result<Option<Self>, crate::wal::WalError> {
        // Need at least the fixed header to read payload_len.
        let header_size = HlcTimestamp::ENCODED_SIZE + 16 + 4;
        if buf.remaining() < header_size {
            if buf.remaining() == 0 {
                return Ok(None);
            }
            return Err(crate::wal::WalError::IncompleteEntry {
                offset,
                needed: header_size,
                available: buf.remaining(),
            });
        }

        let timestamp = HlcTimestamp::decode(buf);
        let device_id = buf.get_u128();
        let payload_len = buf.get_u32() as usize;

        // Now we need payload_len + 4 bytes for the checksum.
        let tail_size = payload_len + 4;
        if buf.remaining() < tail_size {
            return Err(crate::wal::WalError::IncompleteEntry {
                offset,
                needed: header_size + tail_size,
                available: header_size + buf.remaining(),
            });
        }

        let mut payload = vec![0u8; payload_len];
        buf.copy_to_slice(&mut payload);
        let stored_checksum = buf.get_u32();

        let computed = Self::compute_checksum(&timestamp, device_id, &payload);
        if stored_checksum != computed {
            return Err(crate::wal::WalError::ChecksumMismatch {
                offset,
                expected: stored_checksum,
                actual: computed,
            });
        }

        Ok(Some(Self {
            timestamp,
            device_id,
            payload,
            checksum: stored_checksum,
        }))
    }

    /// Compute CRC-32 over all fields except the checksum itself.
    fn compute_checksum(timestamp: &HlcTimestamp, device_id: u128, payload: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&timestamp.physical.to_be_bytes());
        hasher.update(&timestamp.logical.to_be_bytes());
        hasher.update(&device_id.to_be_bytes());
        hasher.update(&(payload.len() as u32).to_be_bytes());
        hasher.update(payload);
        hasher.finalize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry() -> WalEntry {
        WalEntry::new(
            HlcTimestamp::new(1_700_000_000_000, 1),
            0xDEAD_BEEF_CAFE_BABE_0123_4567_89AB_CDEFu128,
            b"hello raft".to_vec(),
        )
    }

    #[test]
    fn hlc_round_trip() {
        let ts = HlcTimestamp::new(42, 7);
        let mut buf = Vec::new();
        ts.encode(&mut buf);
        assert_eq!(buf.len(), HlcTimestamp::ENCODED_SIZE);
        let decoded = HlcTimestamp::decode(&mut &buf[..]);
        assert_eq!(ts, decoded);
    }

    #[test]
    fn hlc_ordering() {
        let a = HlcTimestamp::new(100, 0);
        let b = HlcTimestamp::new(100, 1);
        let c = HlcTimestamp::new(101, 0);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn entry_round_trip() {
        let entry = sample_entry();
        let bytes = entry.encode_to_vec();
        assert_eq!(bytes.len(), entry.encoded_size());

        let decoded = WalEntry::decode(&mut &bytes[..], 0)
            .expect("decode should succeed")
            .expect("should have an entry");
        assert_eq!(entry, decoded);
    }

    #[test]
    fn entry_checksum_is_deterministic() {
        let a = sample_entry();
        let b = sample_entry();
        assert_eq!(a.checksum, b.checksum);
    }

    #[test]
    fn entry_detects_corruption() {
        let entry = sample_entry();
        let mut bytes = entry.encode_to_vec();
        // Flip a byte in the payload region.
        let payload_offset = HlcTimestamp::ENCODED_SIZE + 16 + 4;
        bytes[payload_offset] ^= 0xFF;

        let result = WalEntry::decode(&mut &bytes[..], 0);
        assert!(matches!(result, Err(crate::wal::WalError::ChecksumMismatch { .. })));
    }

    #[test]
    fn entry_empty_payload() {
        let entry = WalEntry::new(HlcTimestamp::new(1, 0), 0, Vec::new());
        let bytes = entry.encode_to_vec();
        let decoded = WalEntry::decode(&mut &bytes[..], 0)
            .expect("decode should succeed")
            .expect("should have an entry");
        assert_eq!(entry, decoded);
        assert!(decoded.payload.is_empty());
    }

    #[test]
    fn decode_empty_buffer_returns_none() {
        let mut buf: &[u8] = &[];
        let result = WalEntry::decode(&mut buf, 0).expect("should not error");
        assert!(result.is_none());
    }

    #[test]
    fn decode_incomplete_header_returns_error() {
        let buf: &[u8] = &[0u8; 5]; // less than header
        let result = WalEntry::decode(&mut &buf[..], 42);
        assert!(matches!(
            result,
            Err(crate::wal::WalError::IncompleteEntry { offset: 42, .. })
        ));
    }
}
