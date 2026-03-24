/// Simple bloom filter for probabilistic key existence checks.
///
/// Uses double hashing (two independent hash functions combined) to
/// set/test `k` bits per key in a bit vector of `m` bits.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Bit vector stored as bytes.
    bits: Vec<u8>,
    /// Number of bits in the filter.
    num_bits: u32,
    /// Number of hash functions (probes) per key.
    num_hashes: u32,
}

impl BloomFilter {
    /// Create a bloom filter sized for `expected_keys` with the given
    /// false-positive rate (e.g. 0.01 for 1%).
    pub fn with_rate(expected_keys: usize, fp_rate: f64) -> Self {
        let expected = expected_keys.max(1) as f64;
        // m = -n * ln(p) / (ln(2)^2)
        let num_bits = ((-expected * fp_rate.ln()) / (2.0_f64.ln().powi(2)))
            .ceil()
            .max(8.0) as u32;
        // k = (m/n) * ln(2)
        let num_hashes = ((num_bits as f64 / expected) * 2.0_f64.ln())
            .ceil()
            .clamp(1.0, 30.0) as u32;
        let byte_len = num_bits.div_ceil(8) as usize;
        Self {
            bits: vec![0u8; byte_len],
            num_bits,
            num_hashes,
        }
    }

    /// Insert a key into the filter.
    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = self.hash_pair(key);
        for i in 0..self.num_hashes {
            let bit = self.probe(h1, h2, i);
            self.set_bit(bit);
        }
    }

    /// Check whether a key *might* be present.
    ///
    /// Returns `false` if the key is definitely absent.
    /// Returns `true` if the key is probably present (subject to false positives).
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.hash_pair(key);
        for i in 0..self.num_hashes {
            let bit = self.probe(h1, h2, i);
            if !self.get_bit(bit) {
                return false;
            }
        }
        true
    }

    /// Encode the bloom filter into bytes for on-disk storage.
    ///
    /// Layout: `[num_bits: u32][num_hashes: u32][bits: ...]`
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + self.bits.len());
        buf.extend_from_slice(&self.num_bits.to_be_bytes());
        buf.extend_from_slice(&self.num_hashes.to_be_bytes());
        buf.extend_from_slice(&self.bits);
        buf
    }

    /// Decode a bloom filter from bytes previously written by `encode()`.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        let num_bits = u32::from_be_bytes(data[0..4].try_into().ok()?);
        let num_hashes = u32::from_be_bytes(data[4..8].try_into().ok()?);
        let expected_byte_len = num_bits.div_ceil(8) as usize;
        let bits_data = &data[8..];
        if bits_data.len() < expected_byte_len {
            return None;
        }
        Some(Self {
            bits: bits_data[..expected_byte_len].to_vec(),
            num_bits,
            num_hashes,
        })
    }

    /// Two independent hashes via splitting a single 64-bit hash.
    /// Uses the FNV-1a family for simplicity and speed — no crypto needed.
    fn hash_pair(&self, key: &[u8]) -> (u32, u32) {
        // FNV-1a 64-bit
        let mut h: u64 = 0xcbf29ce484222325;
        for &b in key {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        let h1 = h as u32;
        let h2 = (h >> 32) as u32;
        (h1, h2)
    }

    /// Double hashing: position = (h1 + i*h2) mod m
    fn probe(&self, h1: u32, h2: u32, i: u32) -> u32 {
        h1.wrapping_add(i.wrapping_mul(h2)) % self.num_bits
    }

    fn set_bit(&mut self, bit: u32) {
        let byte_idx = (bit / 8) as usize;
        let bit_idx = bit % 8;
        self.bits[byte_idx] |= 1 << bit_idx;
    }

    fn get_bit(&self, bit: u32) -> bool {
        let byte_idx = (bit / 8) as usize;
        let bit_idx = bit % 8;
        (self.bits[byte_idx] >> bit_idx) & 1 == 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserted_keys_are_found() {
        let mut bf = BloomFilter::with_rate(100, 0.01);
        for i in 0u32..100 {
            bf.insert(&i.to_be_bytes());
        }
        for i in 0u32..100 {
            assert!(
                bf.may_contain(&i.to_be_bytes()),
                "key {i} should be found"
            );
        }
    }

    #[test]
    fn absent_keys_mostly_rejected() {
        let mut bf = BloomFilter::with_rate(1000, 0.01);
        for i in 0u32..1000 {
            bf.insert(&i.to_be_bytes());
        }
        // Check 1000 keys that were never inserted.
        let false_positives = (10_000u32..11_000)
            .filter(|i| bf.may_contain(&i.to_be_bytes()))
            .count();
        // With 1% FP rate, expect ~10 hits out of 1000. Allow generous margin.
        assert!(
            false_positives < 50,
            "too many false positives: {false_positives}/1000"
        );
    }

    #[test]
    fn empty_filter_rejects_everything() {
        let bf = BloomFilter::with_rate(100, 0.01);
        assert!(!bf.may_contain(b"anything"));
    }

    #[test]
    fn encode_decode_round_trip() {
        let mut bf = BloomFilter::with_rate(50, 0.01);
        for i in 0u32..50 {
            bf.insert(&i.to_be_bytes());
        }
        let encoded = bf.encode();
        let decoded = BloomFilter::decode(&encoded).expect("should decode");

        assert_eq!(decoded.num_bits, bf.num_bits);
        assert_eq!(decoded.num_hashes, bf.num_hashes);
        assert_eq!(decoded.bits, bf.bits);

        // Verify membership still works after round-trip.
        for i in 0u32..50 {
            assert!(decoded.may_contain(&i.to_be_bytes()));
        }
    }

    #[test]
    fn decode_too_short_returns_none() {
        assert!(BloomFilter::decode(&[0; 4]).is_none());
    }
}
