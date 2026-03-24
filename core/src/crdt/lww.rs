//! Last-Write-Wins Register — scalar CRDT resolved by HLC timestamp.
//!
//! Used for scalar fields (string, number, bool). Concurrent writes are
//! resolved deterministically: the write with the higher HLC wins. If
//! timestamps are identical (extremely unlikely with HLC), the write with
//! the higher device ID wins as a tiebreaker.

use serde::{Deserialize, Serialize};

use crate::wal::HlcTimestamp;

use super::Merge;

/// A last-write-wins register holding a value of type `T`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LwwRegister<T> {
    value: T,
    timestamp: HlcTimestamp,
    device_id: u128,
}

impl<T> LwwRegister<T> {
    /// Creates a new register with the given value, timestamp, and originating device.
    pub fn new(value: T, timestamp: HlcTimestamp, device_id: u128) -> Self {
        Self {
            value,
            timestamp,
            device_id,
        }
    }

    /// Returns a reference to the current value.
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Returns the timestamp of the most recent write.
    pub fn timestamp(&self) -> HlcTimestamp {
        self.timestamp
    }

    /// Returns the device that performed the most recent write.
    pub fn device_id(&self) -> u128 {
        self.device_id
    }

    /// Updates the register if the new write has a strictly higher
    /// `(timestamp, device_id)` pair.
    pub fn set(&mut self, value: T, timestamp: HlcTimestamp, device_id: u128) {
        if (timestamp, device_id) > (self.timestamp, self.device_id) {
            self.value = value;
            self.timestamp = timestamp;
            self.device_id = device_id;
        }
    }
}

impl<T: Clone> Merge for LwwRegister<T> {
    /// Merges another register into this one. The write with the higher
    /// `(timestamp, device_id)` pair wins.
    fn merge(&mut self, other: &Self) {
        if (other.timestamp, other.device_id) > (self.timestamp, self.device_id) {
            self.value = other.value.clone();
            self.timestamp = other.timestamp;
            self.device_id = other.device_id;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(physical: u64, logical: u16) -> HlcTimestamp {
        HlcTimestamp::new(physical, logical)
    }

    const DEVICE_A: u128 = 1;
    const DEVICE_B: u128 = 2;

    #[test]
    fn new_register_holds_value() {
        let reg = LwwRegister::new("hello", ts(100, 0), DEVICE_A);
        assert_eq!(*reg.value(), "hello");
        assert_eq!(reg.timestamp(), ts(100, 0));
        assert_eq!(reg.device_id(), DEVICE_A);
    }

    #[test]
    fn set_with_higher_timestamp_wins() {
        let mut reg = LwwRegister::new("old", ts(100, 0), DEVICE_A);
        reg.set("new", ts(200, 0), DEVICE_B);
        assert_eq!(*reg.value(), "new");
    }

    #[test]
    fn set_with_lower_timestamp_is_ignored() {
        let mut reg = LwwRegister::new("current", ts(200, 0), DEVICE_A);
        reg.set("stale", ts(100, 0), DEVICE_B);
        assert_eq!(*reg.value(), "current");
    }

    #[test]
    fn set_with_same_timestamp_uses_device_id_tiebreaker() {
        let mut reg = LwwRegister::new("from_a", ts(100, 0), DEVICE_A);
        reg.set("from_b", ts(100, 0), DEVICE_B);
        // DEVICE_B > DEVICE_A, so DEVICE_B wins
        assert_eq!(*reg.value(), "from_b");
    }

    #[test]
    fn merge_higher_timestamp_wins() {
        let mut a = LwwRegister::new("a_val", ts(100, 0), DEVICE_A);
        let b = LwwRegister::new("b_val", ts(200, 0), DEVICE_B);
        a.merge(&b);
        assert_eq!(*a.value(), "b_val");
    }

    #[test]
    fn merge_lower_timestamp_is_ignored() {
        let mut a = LwwRegister::new("a_val", ts(200, 0), DEVICE_A);
        let b = LwwRegister::new("b_val", ts(100, 0), DEVICE_B);
        a.merge(&b);
        assert_eq!(*a.value(), "a_val");
    }

    #[test]
    fn merge_is_commutative() {
        let a = LwwRegister::new("a_val", ts(100, 0), DEVICE_A);
        let b = LwwRegister::new("b_val", ts(200, 0), DEVICE_B);

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(*ab.value(), *ba.value());
        assert_eq!(ab.timestamp(), ba.timestamp());
    }

    #[test]
    fn merge_is_idempotent() {
        let mut a = LwwRegister::new("a_val", ts(100, 0), DEVICE_A);
        let b = LwwRegister::new("b_val", ts(200, 0), DEVICE_B);

        a.merge(&b);
        let after_first = a.clone();
        a.merge(&b);
        assert_eq!(a, after_first);
    }

    #[test]
    fn merge_concurrent_same_timestamp_deterministic() {
        let a = LwwRegister::new("a_val", ts(100, 0), DEVICE_A);
        let b = LwwRegister::new("b_val", ts(100, 0), DEVICE_B);

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        // Both must converge — higher device_id wins
        assert_eq!(*ab.value(), "b_val");
        assert_eq!(*ba.value(), "b_val");
    }

    #[test]
    fn merge_three_way_associative() {
        let a = LwwRegister::new("a", ts(100, 0), DEVICE_A);
        let b = LwwRegister::new("b", ts(200, 0), DEVICE_B);
        let c = LwwRegister::new("c", ts(150, 0), 3);

        // (a merge b) merge c
        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        // a merge (b merge c)
        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        assert_eq!(*ab_c.value(), *a_bc.value());
    }

    #[test]
    fn serde_round_trip() {
        let reg = LwwRegister::new(42i64, ts(100, 5), DEVICE_A);
        let json = serde_json::to_string(&reg).expect("serialize");
        let decoded: LwwRegister<i64> = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(reg, decoded);
    }
}
