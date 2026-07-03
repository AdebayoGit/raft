//! Observed-Remove Set (OR-Set) — add/remove CRDT for collections.
//!
//! Each `add` generates a globally unique tag (device_id + HLC). A `remove`
//! tombstones only the tags it has *observed*. Concurrent adds always win
//! over concurrent removes (add-wins semantics).

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::wal::HlcTimestamp;

use super::Merge;

/// A unique tag identifying a specific add operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Tag {
    pub device_id: u128,
    pub timestamp: HlcTimestamp,
}

/// An observed-remove set where concurrent adds win over removes.
///
/// Internally tracks every live `(element → {tags})` mapping plus a
/// tombstone set of removed tags. Removing an element tombstones only the
/// tags that were visible at the time of removal, so a concurrent add with
/// a new tag survives — and, crucially, a merge with a stale replica that
/// still carries a tombstoned tag cannot resurrect the element.
///
/// Tombstones grow with the number of removes; tombstones that every peer
/// has observed (per a causal-stability frontier) can be pruned with
/// [`gc`](OrSet::gc).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrSet<T: Eq + std::hash::Hash> {
    /// Maps each element to the set of tags that assert its presence.
    entries: HashMap<T, HashSet<Tag>>,
    /// Tags whose adds have been observed and removed, mapped to the HLC
    /// timestamp of the remove. The remove timestamp — not the add tag's —
    /// is what [`gc`](OrSet::gc) compares against the stability frontier:
    /// a replica may have acked past the *add* long before it ever saw
    /// the *remove*.
    #[serde(default, with = "tombstone_serde")]
    tombstones: HashMap<Tag, HlcTimestamp>,
}

/// JSON objects require string keys, so serialize the tombstone map as a
/// sequence of `(tag, removed_at)` pairs.
mod tombstone_serde {
    use super::{HashMap, HlcTimestamp, Tag};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        map: &HashMap<Tag, HlcTimestamp>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let pairs: Vec<(&Tag, &HlcTimestamp)> = map.iter().collect();
        pairs.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<HashMap<Tag, HlcTimestamp>, D::Error> {
        let pairs = Vec::<(Tag, HlcTimestamp)>::deserialize(deserializer)?;
        Ok(pairs.into_iter().collect())
    }
}

impl<T: Eq + std::hash::Hash> Default for OrSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Eq + std::hash::Hash> OrSet<T> {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            tombstones: HashMap::new(),
        }
    }

    /// Adds an element, returning the generated tag.
    ///
    /// If the tag is already tombstoned (this exact `(device, timestamp)`
    /// add was previously observed and removed), the add is a no-op:
    /// re-inserting a dead tag would create a non-canonical state that
    /// `merge` prunes, breaking merge idempotence. Re-adding a removed
    /// element requires a fresh tag (a new timestamp).
    pub fn add(&mut self, element: T, device_id: u128, timestamp: HlcTimestamp) -> Tag {
        let tag = Tag {
            device_id,
            timestamp,
        };
        if !self.tombstones.contains_key(&tag) {
            self.entries.entry(element).or_default().insert(tag);
        }
        tag
    }

    /// Removes an element by tombstoning all currently-observed tags,
    /// recording `removed_at` (the HLC timestamp of this remove) on each
    /// tombstone so [`gc`](OrSet::gc) can later judge its stability.
    ///
    /// Returns `true` if the element was present and removed.
    pub fn remove(&mut self, element: &T, removed_at: HlcTimestamp) -> bool {
        match self.entries.remove(element) {
            Some(tags) => {
                for tag in tags {
                    self.record_tombstone(tag, removed_at);
                }
                true
            }
            None => false,
        }
    }

    /// Inserts or raises a tombstone, keeping the *maximum* removed_at.
    ///
    /// Max is the join on timestamps, which keeps merge commutative,
    /// associative, and idempotent, and is conservative for GC (a
    /// tombstone is never considered stable earlier than any replica
    /// believes it was removed).
    fn record_tombstone(&mut self, tag: Tag, removed_at: HlcTimestamp) {
        self.tombstones
            .entry(tag)
            .and_modify(|current| {
                if removed_at > *current {
                    *current = removed_at;
                }
            })
            .or_insert(removed_at);
    }

    /// Returns `true` if the element is in the set (has at least one live tag).
    pub fn contains(&self, element: &T) -> bool {
        self.entries
            .get(element)
            .is_some_and(|tags| !tags.is_empty())
    }

    /// Returns the number of distinct elements in the set.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns an iterator over references to the elements.
    pub fn elements(&self) -> impl Iterator<Item = &T> {
        self.entries.keys()
    }

    /// Returns the number of tombstoned tags currently retained.
    pub fn tombstone_count(&self) -> usize {
        self.tombstones.len()
    }

    /// Garbage-collects tombstones that are causally stable, returning
    /// the number pruned.
    ///
    /// A tombstone whose `removed_at <= frontier` is dropped. This is safe
    /// **only** when `frontier` is a true causal-stability frontier (see
    /// [`StabilityFrontier`](super::StabilityFrontier)): every replica has
    /// already observed the remove, merged it, and purged the dead tag
    /// from its own `entries` — so no future merge can carry that tag
    /// back in, and the tombstone no longer has a race to win.
    ///
    /// If the caller supplies a frontier that some replica has *not*
    /// reached, that replica's stale live tag would resurrect the element
    /// on merge. Convergence is still guaranteed (all replicas would
    /// resurrect identically); only remove-durability is at risk — which
    /// is exactly why the frontier must come from all-replica acks.
    pub fn gc(&mut self, frontier: HlcTimestamp) -> usize {
        let before = self.tombstones.len();
        self.tombstones
            .retain(|_, removed_at| *removed_at > frontier);
        before - self.tombstones.len()
    }
}

impl<T: Eq + std::hash::Hash + Clone> Merge for OrSet<T> {
    /// Merges another OR-Set into this one.
    ///
    /// Tombstones are unioned first; each element's resulting tag set is
    /// then the union of tags from both sides minus every tombstoned tag.
    /// A remove observed on either side therefore sticks, while a
    /// concurrent add (whose fresh tag no remove has observed) survives.
    fn merge(&mut self, other: &Self) {
        for (tag, removed_at) in &other.tombstones {
            self.record_tombstone(*tag, *removed_at);
        }

        for (element, other_tags) in &other.entries {
            let live: Vec<Tag> = other_tags
                .iter()
                .filter(|t| !self.tombstones.contains_key(t))
                .copied()
                .collect();
            if !live.is_empty() {
                self.entries
                    .entry(element.clone())
                    .or_default()
                    .extend(live);
            }
        }

        // Purge local tags that the other side has removed.
        let tombstones = &self.tombstones;
        self.entries.retain(|_, tags| {
            tags.retain(|t| !tombstones.contains_key(t));
            !tags.is_empty()
        });
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
    fn add_and_contains() {
        let mut set = OrSet::new();
        set.add("apple", DEVICE_A, ts(100, 0));
        assert!(set.contains(&"apple"));
        assert!(!set.contains(&"banana"));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn remove_observed_element() {
        let mut set = OrSet::new();
        set.add("apple", DEVICE_A, ts(100, 0));
        assert!(set.remove(&"apple", ts(101, 0)));
        assert!(!set.contains(&"apple"));
        assert!(set.is_empty());
    }

    #[test]
    fn remove_nonexistent_returns_false() {
        let mut set: OrSet<&str> = OrSet::new();
        assert!(!set.remove(&"ghost", ts(1, 0)));
    }

    #[test]
    fn concurrent_adds_both_survive_merge() {
        let mut a = OrSet::new();
        a.add("apple", DEVICE_A, ts(100, 0));

        let mut b = OrSet::new();
        b.add("apple", DEVICE_B, ts(100, 0));
        b.add("banana", DEVICE_B, ts(101, 0));

        a.merge(&b);
        assert!(a.contains(&"apple"));
        assert!(a.contains(&"banana"));
        // apple should have 2 tags (one from each device)
        assert_eq!(a.entries.get(&"apple").unwrap().len(), 2);
    }

    #[test]
    fn add_wins_over_concurrent_remove() {
        // Device A has {apple} with tag_a
        let mut a = OrSet::new();
        let _tag_a = a.add("apple", DEVICE_A, ts(100, 0));

        // Device B independently adds apple with a different tag, then we
        // simulate that device A removed apple (only seeing its own tag).
        let mut b = OrSet::new();
        b.add("apple", DEVICE_B, ts(101, 0));

        // Device A removes apple — only its local tag is tombstoned.
        a.remove(&"apple", ts(102, 0));
        assert!(!a.contains(&"apple"));

        // Merge: device B's concurrent add should resurrect apple.
        a.merge(&b);
        assert!(a.contains(&"apple"));
    }

    #[test]
    fn merge_with_stale_replica_does_not_resurrect_removed_element() {
        // A adds apple; B syncs (sees the same tag); A removes apple.
        // Merging B's stale state back into A must not resurrect apple.
        let mut a = OrSet::new();
        a.add("apple", DEVICE_A, ts(100, 0));

        let b = a.clone(); // stale replica still holds the tag

        a.remove(&"apple", ts(101, 0));
        assert!(!a.contains(&"apple"));

        a.merge(&b);
        assert!(
            !a.contains(&"apple"),
            "stale merge must not resurrect a removed element"
        );
    }

    #[test]
    fn remove_propagates_through_merge() {
        // A adds apple; B syncs; B removes apple; A merges from B.
        let mut a = OrSet::new();
        a.add("apple", DEVICE_A, ts(100, 0));

        let mut b = a.clone();
        b.remove(&"apple", ts(101, 0));

        a.merge(&b);
        assert!(!a.contains(&"apple"), "remove must propagate via merge");
    }

    #[test]
    fn merge_is_commutative() {
        let mut a = OrSet::new();
        a.add("x", DEVICE_A, ts(100, 0));
        a.add("y", DEVICE_A, ts(101, 0));

        let mut b = OrSet::new();
        b.add("y", DEVICE_B, ts(102, 0));
        b.add("z", DEVICE_B, ts(103, 0));

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        // Same elements present
        let mut ab_elems: Vec<_> = ab.elements().collect();
        ab_elems.sort();
        let mut ba_elems: Vec<_> = ba.elements().collect();
        ba_elems.sort();
        assert_eq!(ab_elems, ba_elems);
    }

    #[test]
    fn merge_is_idempotent() {
        let mut a = OrSet::new();
        a.add("x", DEVICE_A, ts(100, 0));

        let b = a.clone();
        a.merge(&b);
        assert_eq!(a.len(), 1);
        assert_eq!(a.entries.get(&"x").unwrap().len(), 1);
    }

    #[test]
    fn merge_is_associative() {
        let mut a = OrSet::new();
        a.add("x", DEVICE_A, ts(100, 0));

        let mut b = OrSet::new();
        b.add("y", DEVICE_B, ts(101, 0));

        let mut c = OrSet::new();
        c.add("z", 3, ts(102, 0));

        // (a merge b) merge c
        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        // a merge (b merge c)
        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        let mut ab_c_elems: Vec<_> = ab_c.elements().collect();
        ab_c_elems.sort();
        let mut a_bc_elems: Vec<_> = a_bc.elements().collect();
        a_bc_elems.sort();
        assert_eq!(ab_c_elems, a_bc_elems);
    }

    #[test]
    fn multiple_adds_same_element_accumulate_tags() {
        let mut set = OrSet::new();
        set.add("x", DEVICE_A, ts(100, 0));
        set.add("x", DEVICE_A, ts(101, 0));
        set.add("x", DEVICE_B, ts(100, 0));

        assert_eq!(set.entries.get(&"x").unwrap().len(), 3);

        // Remove clears all tags
        set.remove(&"x", ts(102, 0));
        assert!(!set.contains(&"x"));
    }

    #[test]
    fn readd_with_tombstoned_tag_is_a_no_op() {
        // Found by the X3 proptest suite: add → remove → add with the
        // identical (device, timestamp) tag used to resurrect the dead
        // tag in `entries` while it stayed tombstoned, so self-merge
        // pruned it and idempotence broke.
        let mut set = OrSet::new();
        set.add("x", DEVICE_A, ts(100, 0));
        set.remove(&"x", ts(100, 1));
        set.add("x", DEVICE_A, ts(100, 0)); // same tag — must not resurrect
        assert!(!set.contains(&"x"));

        // Self-merge must be a no-op on the canonical state.
        let copy = set.clone();
        set.merge(&copy);
        assert_eq!(set, copy);

        // A fresh tag re-adds normally.
        set.add("x", DEVICE_A, ts(101, 0));
        assert!(set.contains(&"x"));
    }

    #[test]
    fn elements_iterator() {
        let mut set = OrSet::new();
        set.add("a", DEVICE_A, ts(100, 0));
        set.add("b", DEVICE_A, ts(101, 0));
        set.add("c", DEVICE_B, ts(102, 0));

        let mut elems: Vec<_> = set.elements().collect();
        elems.sort();
        assert_eq!(elems, vec![&"a", &"b", &"c"]);
    }

    #[test]
    fn default_is_empty() {
        let set: OrSet<String> = OrSet::default();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn gc_prunes_only_stable_tombstones() {
        let mut set = OrSet::new();
        set.add("old", DEVICE_A, ts(100, 0));
        set.add("new", DEVICE_A, ts(100, 1));
        set.remove(&"old", ts(150, 0));
        set.remove(&"new", ts(550, 0));
        assert_eq!(set.tombstone_count(), 2);

        // Frontier covers the first remove but not the second.
        let pruned = set.gc(ts(200, 0));
        assert_eq!(pruned, 1);
        assert_eq!(set.tombstone_count(), 1);
        assert!(!set.contains(&"old"));
        assert!(!set.contains(&"new"));
    }

    #[test]
    fn gc_frontier_boundary_is_inclusive() {
        let mut set = OrSet::new();
        set.add("x", DEVICE_A, ts(100, 0));
        set.remove(&"x", ts(100, 5));

        // frontier == removed_at → pruned.
        assert_eq!(set.gc(ts(100, 5)), 1);
        assert_eq!(set.tombstone_count(), 0);
    }

    #[test]
    fn gc_compares_remove_time_not_add_time() {
        // The add is ancient, but the remove is recent. A frontier that
        // covers the add but not the remove must NOT prune the tombstone
        // — a replica may have acked past the add without ever seeing
        // the remove.
        let mut set = OrSet::new();
        set.add("x", DEVICE_A, ts(100, 0));
        set.remove(&"x", ts(900, 0));

        assert_eq!(set.gc(ts(500, 0)), 0, "remove not yet stable");
        assert_eq!(set.tombstone_count(), 1);
        assert_eq!(set.gc(ts(900, 0)), 1);
    }

    #[test]
    fn merge_keeps_latest_remove_time_for_same_tag() {
        // Two replicas independently remove the same observed tag at
        // different times. The merged tombstone must carry the maximum
        // (conservative for GC) in both merge orders.
        let mut a = OrSet::new();
        a.add("apple", DEVICE_A, ts(100, 0));
        let mut b = a.clone();

        a.remove(&"apple", ts(200, 0));
        b.remove(&"apple", ts(300, 0));

        let mut ab = a.clone();
        ab.merge(&b);
        let mut ba = b.clone();
        ba.merge(&a);
        assert_eq!(ab, ba, "merge must be commutative on remove times");

        // Frontier past the earlier remove only: tombstone must survive.
        assert_eq!(ab.gc(ts(200, 0)), 0);
        assert_eq!(ab.gc(ts(300, 0)), 1);
    }

    #[test]
    fn merge_after_gc_converges_when_all_replicas_purged() {
        // Both replicas fully synced: the removed tag is gone from
        // everyone's entries, so dropping the tombstone is safe.
        let mut a = OrSet::new();
        a.add("apple", DEVICE_A, ts(100, 0));
        let mut b = a.clone();

        a.remove(&"apple", ts(200, 0));
        b.merge(&a); // b observes the remove and purges the tag
        a.merge(&b);

        a.gc(ts(200, 0));
        b.gc(ts(200, 0));
        assert_eq!(a.tombstone_count(), 0);
        assert_eq!(b.tombstone_count(), 0);

        // Post-GC merges in both directions stay converged.
        let mut ab = a.clone();
        ab.merge(&b);
        let mut ba = b.clone();
        ba.merge(&a);
        assert_eq!(ab, ba);
        assert!(!ab.contains(&"apple"));
    }

    #[test]
    fn tombstones_beyond_frontier_still_block_stale_replicas() {
        // b is a stale replica still holding the live tag. The frontier
        // (correctly computed) has not advanced past the remove, so the
        // tombstone survives GC and continues to block resurrection.
        let mut a = OrSet::new();
        a.add("apple", DEVICE_A, ts(100, 0));
        let b = a.clone();

        a.remove(&"apple", ts(200, 0));
        // The laggard has only acked up to ts(150) — below the remove.
        a.gc(ts(150, 0));
        assert_eq!(a.tombstone_count(), 1);

        a.merge(&b);
        assert!(
            !a.contains(&"apple"),
            "tombstone above the frontier must keep blocking stale tags"
        );
    }

    #[test]
    fn serde_round_trip() {
        let mut set = OrSet::new();
        set.add("hello".to_string(), DEVICE_A, ts(100, 0));
        set.add("world".to_string(), DEVICE_B, ts(101, 0));

        let json = serde_json::to_string(&set).expect("serialize");
        let decoded: OrSet<String> = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(set, decoded);
    }
}
