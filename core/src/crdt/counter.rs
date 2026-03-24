//! Grow/shrink counter CRDT — merge by taking the max per-device delta.
//!
//! Each device maintains its own running delta (positive or negative).
//! The global value is the sum of all per-device deltas. Merging takes
//! the max delta per device, which is correct because deltas are monotonically
//! increasing within a single device's timeline (each operation adds to the
//! running total).
//!
//! This is a PN-Counter variant where each device tracks a single signed
//! delta rather than separate positive/negative counters.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::Merge;

/// A replicated counter supporting increment and decrement.
///
/// Internally stores per-device deltas. The counter's value is the sum of
/// all device deltas.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counter {
    /// Per-device cumulative delta. Each device only ever increases its own
    /// entry (in absolute terms of operations applied), so taking `max` on
    /// merge is safe.
    deltas: HashMap<u128, i64>,
}

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

impl Counter {
    pub fn new() -> Self {
        Self {
            deltas: HashMap::new(),
        }
    }

    /// Returns the current counter value (sum of all device deltas).
    pub fn value(&self) -> i64 {
        self.deltas.values().sum()
    }

    /// Increments the counter by `amount` on behalf of `device_id`.
    pub fn increment(&mut self, device_id: u128, amount: i64) {
        let entry = self.deltas.entry(device_id).or_insert(0);
        *entry += amount;
    }

    /// Decrements the counter by `amount` on behalf of `device_id`.
    pub fn decrement(&mut self, device_id: u128, amount: i64) {
        self.increment(device_id, -amount);
    }

    /// Returns the delta contributed by a specific device.
    pub fn device_delta(&self, device_id: u128) -> i64 {
        self.deltas.get(&device_id).copied().unwrap_or(0)
    }
}

impl Merge for Counter {
    /// Merges another counter by taking the max delta per device.
    ///
    /// This is correct because a device's delta is the cumulative result of
    /// all its operations. A higher delta means more operations have been
    /// applied, so taking max incorporates all known operations.
    fn merge(&mut self, other: &Self) {
        for (&device_id, &other_delta) in &other.deltas {
            let local = self.deltas.entry(device_id).or_insert(0);
            if other_delta > *local {
                *local = other_delta;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEVICE_A: u128 = 1;
    const DEVICE_B: u128 = 2;
    const DEVICE_C: u128 = 3;

    #[test]
    fn new_counter_is_zero() {
        let c = Counter::new();
        assert_eq!(c.value(), 0);
    }

    #[test]
    fn increment_adds_to_value() {
        let mut c = Counter::new();
        c.increment(DEVICE_A, 5);
        assert_eq!(c.value(), 5);
        c.increment(DEVICE_A, 3);
        assert_eq!(c.value(), 8);
    }

    #[test]
    fn decrement_subtracts_from_value() {
        let mut c = Counter::new();
        c.increment(DEVICE_A, 10);
        c.decrement(DEVICE_A, 3);
        assert_eq!(c.value(), 7);
    }

    #[test]
    fn multiple_devices_contribute_independently() {
        let mut c = Counter::new();
        c.increment(DEVICE_A, 5);
        c.increment(DEVICE_B, 10);
        assert_eq!(c.value(), 15);
        assert_eq!(c.device_delta(DEVICE_A), 5);
        assert_eq!(c.device_delta(DEVICE_B), 10);
    }

    #[test]
    fn device_delta_returns_zero_for_unknown() {
        let c = Counter::new();
        assert_eq!(c.device_delta(DEVICE_A), 0);
    }

    #[test]
    fn merge_takes_max_per_device() {
        // Device A has applied +10, Device B has applied +5
        let mut a = Counter::new();
        a.increment(DEVICE_A, 10);

        let mut b = Counter::new();
        b.increment(DEVICE_B, 5);

        a.merge(&b);
        assert_eq!(a.value(), 15);
        assert_eq!(a.device_delta(DEVICE_A), 10);
        assert_eq!(a.device_delta(DEVICE_B), 5);
    }

    #[test]
    fn merge_picks_higher_delta_when_both_have_same_device() {
        let mut a = Counter::new();
        a.increment(DEVICE_A, 10);

        // b has a more recent view — device A applied more ops
        let mut b = Counter::new();
        b.increment(DEVICE_A, 15);

        a.merge(&b);
        assert_eq!(a.device_delta(DEVICE_A), 15);
        assert_eq!(a.value(), 15);
    }

    #[test]
    fn merge_does_not_regress_local_delta() {
        let mut a = Counter::new();
        a.increment(DEVICE_A, 20);

        let mut b = Counter::new();
        b.increment(DEVICE_A, 10); // stale view

        a.merge(&b);
        assert_eq!(a.device_delta(DEVICE_A), 20); // stays at 20
    }

    #[test]
    fn merge_is_commutative() {
        let mut a = Counter::new();
        a.increment(DEVICE_A, 10);
        a.increment(DEVICE_B, 3);

        let mut b = Counter::new();
        b.increment(DEVICE_B, 7);
        b.increment(DEVICE_C, 5);

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab.value(), ba.value());
        assert_eq!(ab.device_delta(DEVICE_A), ba.device_delta(DEVICE_A));
        assert_eq!(ab.device_delta(DEVICE_B), ba.device_delta(DEVICE_B));
        assert_eq!(ab.device_delta(DEVICE_C), ba.device_delta(DEVICE_C));
    }

    #[test]
    fn merge_is_idempotent() {
        let mut a = Counter::new();
        a.increment(DEVICE_A, 10);
        a.increment(DEVICE_B, 5);

        let snapshot = a.clone();
        a.merge(&snapshot);
        assert_eq!(a, snapshot);
    }

    #[test]
    fn merge_is_associative() {
        let mut a = Counter::new();
        a.increment(DEVICE_A, 10);

        let mut b = Counter::new();
        b.increment(DEVICE_B, 20);

        let mut c = Counter::new();
        c.increment(DEVICE_C, 30);

        // (a merge b) merge c
        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        // a merge (b merge c)
        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        assert_eq!(ab_c.value(), a_bc.value());
    }

    #[test]
    fn concurrent_increments_both_reflected_after_merge() {
        // Simulates two devices incrementing independently then merging.
        let mut replica_a = Counter::new();
        replica_a.increment(DEVICE_A, 5); // A does +5

        let mut replica_b = Counter::new();
        replica_b.increment(DEVICE_B, 3); // B does +3

        // Both replicas sync
        replica_a.merge(&replica_b);
        replica_b.merge(&replica_a);

        assert_eq!(replica_a.value(), 8);
        assert_eq!(replica_b.value(), 8);
    }

    #[test]
    fn negative_deltas_work() {
        let mut c = Counter::new();
        c.decrement(DEVICE_A, 5);
        assert_eq!(c.value(), -5);

        c.increment(DEVICE_A, 3);
        assert_eq!(c.value(), -2);
    }

    #[test]
    fn default_is_zero() {
        let c = Counter::default();
        assert_eq!(c.value(), 0);
    }

    #[test]
    fn serde_round_trip() {
        let mut c = Counter::new();
        c.increment(DEVICE_A, 42);
        c.decrement(DEVICE_B, 10);

        let json = serde_json::to_string(&c).expect("serialize");
        let decoded: Counter = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(c, decoded);
    }
}
