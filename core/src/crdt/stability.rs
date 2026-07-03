//! Causal-stability frontier for CRDT garbage collection.
//!
//! A mutation is *causally stable* once every replica has observed it —
//! after that point no future merge can arrive that is concurrent with it,
//! so metadata that exists solely to win such races (OR-Set tombstones)
//! can be discarded.
//!
//! Raft owns no network layer, so it cannot learn replica acknowledgements
//! by itself. Whatever plugs into the merge surface (Relay, or a
//! developer's own backend) feeds acks into a [`StabilityFrontier`] via
//! [`observe`](StabilityFrontier::observe) and passes the resulting
//! [`frontier`](StabilityFrontier::frontier) to `OrSet::gc`.
//!
//! Safety rests on one invariant the caller must uphold: **every replica
//! that may ever merge again is tracked**. If a device is registered, its
//! lagging ack holds the frontier back, so GC never outruns it. A device
//! that is retired must be removed from the tracked set out-of-band
//! (by rebuilding the frontier without it).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::wal::HlcTimestamp;

/// Tracks, per device, the newest HLC timestamp that device has
/// acknowledged as fully applied.
///
/// The causal-stability frontier is the *minimum* of those maxima: every
/// mutation at or below it has been observed by every tracked device.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StabilityFrontier {
    /// device_id → highest timestamp that device has acknowledged.
    acked: HashMap<u128, HlcTimestamp>,
}

impl StabilityFrontier {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records that `device_id` has applied all mutations up to and
    /// including `ts`. Acks are monotonic: an older timestamp than the
    /// one already recorded is ignored.
    pub fn observe(&mut self, device_id: u128, ts: HlcTimestamp) {
        self.acked
            .entry(device_id)
            .and_modify(|current| {
                if ts > *current {
                    *current = ts;
                }
            })
            .or_insert(ts);
    }

    /// Returns the causal-stability frontier: the highest timestamp that
    /// *every* tracked device has acknowledged.
    ///
    /// Returns `None` when no device has been observed yet — with zero
    /// participants nothing can be proven stable, so callers must not GC.
    pub fn frontier(&self) -> Option<HlcTimestamp> {
        self.acked.values().min().copied()
    }

    /// Number of devices currently tracked.
    pub fn device_count(&self) -> usize {
        self.acked.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(physical: u64, logical: u16) -> HlcTimestamp {
        HlcTimestamp::new(physical, logical)
    }

    #[test]
    fn empty_frontier_is_none() {
        let f = StabilityFrontier::new();
        assert_eq!(f.frontier(), None);
        assert_eq!(f.device_count(), 0);
    }

    #[test]
    fn single_device_frontier_is_its_ack() {
        let mut f = StabilityFrontier::new();
        f.observe(1, ts(100, 0));
        assert_eq!(f.frontier(), Some(ts(100, 0)));
    }

    #[test]
    fn frontier_is_minimum_across_devices() {
        let mut f = StabilityFrontier::new();
        f.observe(1, ts(500, 0));
        f.observe(2, ts(100, 3));
        f.observe(3, ts(300, 0));
        assert_eq!(f.frontier(), Some(ts(100, 3)));
    }

    #[test]
    fn acks_are_monotonic() {
        let mut f = StabilityFrontier::new();
        f.observe(1, ts(200, 0));
        // A stale, out-of-order ack must not move the device backwards.
        f.observe(1, ts(100, 0));
        assert_eq!(f.frontier(), Some(ts(200, 0)));

        f.observe(1, ts(300, 5));
        assert_eq!(f.frontier(), Some(ts(300, 5)));
    }

    #[test]
    fn lagging_device_holds_frontier_back() {
        let mut f = StabilityFrontier::new();
        f.observe(1, ts(100, 0));
        f.observe(2, ts(100, 0));
        assert_eq!(f.frontier(), Some(ts(100, 0)));

        // Device 1 races ahead; device 2 is offline. Frontier stays put.
        f.observe(1, ts(9_999, 0));
        assert_eq!(f.frontier(), Some(ts(100, 0)));

        // Device 2 catches up.
        f.observe(2, ts(9_999, 0));
        assert_eq!(f.frontier(), Some(ts(9_999, 0)));
    }

    #[test]
    fn logical_component_breaks_ties() {
        let mut f = StabilityFrontier::new();
        f.observe(1, ts(100, 2));
        f.observe(2, ts(100, 1));
        assert_eq!(f.frontier(), Some(ts(100, 1)));
    }

    #[test]
    fn serde_round_trip() {
        let mut f = StabilityFrontier::new();
        f.observe(1, ts(100, 0));
        f.observe(2, ts(200, 4));

        let json = serde_json::to_string(&f).expect("serialize");
        let decoded: StabilityFrontier = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(f, decoded);
    }
}
