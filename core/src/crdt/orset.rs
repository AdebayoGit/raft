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
/// Internally tracks every live `(element → {tags})` mapping. Removing an
/// element only removes the tags that were visible at the time of removal;
/// a concurrent add with a new tag survives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrSet<T: Eq + std::hash::Hash> {
    /// Maps each element to the set of tags that assert its presence.
    entries: HashMap<T, HashSet<Tag>>,
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
        }
    }

    /// Adds an element, returning the generated tag.
    pub fn add(&mut self, element: T, device_id: u128, timestamp: HlcTimestamp) -> Tag {
        let tag = Tag {
            device_id,
            timestamp,
        };
        self.entries.entry(element).or_default().insert(tag);
        tag
    }

    /// Removes an element by tombstoning all currently-observed tags.
    ///
    /// Returns `true` if the element was present and removed.
    pub fn remove(&mut self, element: &T) -> bool {
        self.entries.remove(element).is_some()
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
}

impl<T: Eq + std::hash::Hash + Clone> Merge for OrSet<T> {
    /// Merges another OR-Set into this one.
    ///
    /// For each element, the resulting tag set is the union of tags from both
    /// sides. An element present in only one side keeps its tags (add-wins).
    fn merge(&mut self, other: &Self) {
        for (element, other_tags) in &other.entries {
            let local_tags = self.entries.entry(element.clone()).or_default();
            for tag in other_tags {
                local_tags.insert(*tag);
            }
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
        assert!(set.remove(&"apple"));
        assert!(!set.contains(&"apple"));
        assert!(set.is_empty());
    }

    #[test]
    fn remove_nonexistent_returns_false() {
        let mut set: OrSet<&str> = OrSet::new();
        assert!(!set.remove(&"ghost"));
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
        a.remove(&"apple");
        assert!(!a.contains(&"apple"));

        // Merge: device B's concurrent add should resurrect apple.
        a.merge(&b);
        assert!(a.contains(&"apple"));
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
        set.remove(&"x");
        assert!(!set.contains(&"x"));
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
    fn serde_round_trip() {
        let mut set = OrSet::new();
        set.add("hello".to_string(), DEVICE_A, ts(100, 0));
        set.add("world".to_string(), DEVICE_B, ts(101, 0));

        let json = serde_json::to_string(&set).expect("serialize");
        let decoded: OrSet<String> = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(set, decoded);
    }
}
