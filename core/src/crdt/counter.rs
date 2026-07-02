//! PN-Counter CRDT — merge by taking the max per-device increment and
//! decrement totals independently.
//!
//! Each device maintains two monotonically increasing totals: everything it
//! has ever added (`inc`) and everything it has ever subtracted (`dec`).
//! The global value is `Σ inc − Σ dec`. Because both totals only ever grow
//! within a single device's timeline, taking the per-device `max` on merge
//! is a correct join — unlike a single signed delta, which stops being
//! monotonic the moment a device decrements.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::Merge;

/// Per-device state: monotonically increasing increment/decrement totals.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
struct DeviceTotals {
    inc: u64,
    dec: u64,
}

/// A replicated counter supporting increment and decrement (PN-Counter).
///
/// The counter's value is the sum of all device increments minus the sum
/// of all device decrements.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counter {
    /// Per-device totals. Each device only ever grows its own entry, so
    /// taking the field-wise `max` on merge is safe.
    totals: HashMap<u128, DeviceTotals>,
}

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

impl Counter {
    pub fn new() -> Self {
        Self {
            totals: HashMap::new(),
        }
    }

    /// Returns the current counter value (Σ increments − Σ decrements).
    pub fn value(&self) -> i64 {
        self.totals
            .values()
            .map(|t| t.inc as i64 - t.dec as i64)
            .sum()
    }

    /// Increments the counter by `amount` on behalf of `device_id`.
    /// Negative amounts are routed to the decrement total so both totals
    /// stay monotonic.
    pub fn increment(&mut self, device_id: u128, amount: i64) {
        let entry = self.totals.entry(device_id).or_default();
        if amount >= 0 {
            entry.inc = entry.inc.saturating_add(amount as u64);
        } else {
            entry.dec = entry.dec.saturating_add(amount.unsigned_abs());
        }
    }

    /// Decrements the counter by `amount` on behalf of `device_id`.
    pub fn decrement(&mut self, device_id: u128, amount: i64) {
        self.increment(device_id, amount.checked_neg().unwrap_or(i64::MAX));
    }

    /// Returns the net delta contributed by a specific device.
    pub fn device_delta(&self, device_id: u128) -> i64 {
        self.totals
            .get(&device_id)
            .map(|t| t.inc as i64 - t.dec as i64)
            .unwrap_or(0)
    }
}

impl Merge for Counter {
    /// Merges another counter by taking the max increment and decrement
    /// totals per device, independently.
    ///
    /// Both totals are monotonically increasing within a device's
    /// timeline, so a higher total always means "more operations
    /// observed" — the max incorporates all known operations without
    /// double-counting or resurrecting undone decrements.
    fn merge(&mut self, other: &Self) {
        for (&device_id, other_totals) in &other.totals {
            let local = self.totals.entry(device_id).or_default();
            local.inc = local.inc.max(other_totals.inc);
            local.dec = local.dec.max(other_totals.dec);
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
    fn merge_picks_higher_totals_when_both_have_same_device() {
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
    fn merge_does_not_resurrect_decrements() {
        // Regression for the single-signed-delta bug: device A does +10
        // then −3. A replica holding the stale +10 view must not undo
        // the decrement on merge.
        let mut a = Counter::new();
        a.increment(DEVICE_A, 10);

        let stale = a.clone(); // sees only +10

        a.decrement(DEVICE_A, 3);
        assert_eq!(a.value(), 7);

        a.merge(&stale);
        assert_eq!(a.value(), 7, "stale merge must not undo the decrement");
    }

    #[test]
    fn concurrent_increment_and_decrement_converge() {
        let mut a = Counter::new();
        a.increment(DEVICE_A, 10);
        let mut b = a.clone();

        a.increment(DEVICE_A, 5); // A: +15 total
        b.decrement(DEVICE_B, 4); // B: −4 from another device

        let mut ab = a.clone();
        ab.merge(&b);
        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab.value(), 11);
        assert_eq!(ba.value(), 11);
    }

    #[test]
    fn merge_is_commutative() {
        let mut a = Counter::new();
        a.increment(DEVICE_A, 10);
        a.decrement(DEVICE_B, 3);

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
        a.decrement(DEVICE_B, 5);

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
        c.decrement(DEVICE_C, 30);

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
        let mut replica_a = Counter::new();
        replica_a.increment(DEVICE_A, 5);

        let mut replica_b = Counter::new();
        replica_b.increment(DEVICE_B, 3);

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
    fn negative_increment_amount_routes_to_decrement() {
        let mut c = Counter::new();
        c.increment(DEVICE_A, -4);
        assert_eq!(c.value(), -4);
        c.decrement(DEVICE_A, -6); // double negative → +6
        assert_eq!(c.value(), 2);
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
