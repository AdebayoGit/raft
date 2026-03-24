//! B-tree–based secondary index for range queries and equality lookups.
//!
//! Uses `BTreeMap<Vec<u8>, BTreeSet<DocId>>`. Both exact-match and range
//! scans are supported with O(log n) access.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeBounds;

use super::{DocId, Index};

/// A B-tree–based index supporting both equality and range lookups.
#[derive(Debug, Clone)]
pub struct BTreeIndex {
    map: BTreeMap<Vec<u8>, BTreeSet<DocId>>,
    len: usize,
}

impl Default for BTreeIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl BTreeIndex {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            len: 0,
        }
    }
}

impl Index for BTreeIndex {
    fn insert(&mut self, key: &[u8], doc_id: DocId) {
        let set = self.map.entry(key.to_vec()).or_default();
        if set.insert(doc_id) {
            self.len += 1;
        }
    }

    fn remove(&mut self, key: &[u8], doc_id: DocId) -> bool {
        let Some(set) = self.map.get_mut(key) else {
            return false;
        };
        if !set.remove(&doc_id) {
            return false;
        }
        self.len -= 1;
        if set.is_empty() {
            self.map.remove(key);
        }
        true
    }

    fn lookup(&self, key: &[u8]) -> Vec<DocId> {
        self.map
            .get(key)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Returns all document IDs whose keys fall within `range`, sorted by
    /// `(key, DocId)`.
    fn range(&self, range: impl RangeBounds<Vec<u8>>) -> Vec<DocId> {
        self.map
            .range(range)
            .flat_map(|(_, set)| set.iter().copied())
            .collect()
    }

    fn len(&self) -> usize {
        self.len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(id: u64) -> DocId {
        DocId(id)
    }

    // ── Equality (lookup) ──────────────────────────────────────────────

    #[test]
    fn insert_and_lookup() {
        let mut idx = BTreeIndex::new();
        idx.insert(b"alice", d(1));
        idx.insert(b"alice", d(2));
        idx.insert(b"bob", d(3));

        assert_eq!(idx.lookup(b"alice"), vec![d(1), d(2)]);
        assert_eq!(idx.lookup(b"bob"), vec![d(3)]);
        assert_eq!(idx.len(), 3);
    }

    #[test]
    fn lookup_missing_returns_empty() {
        let idx = BTreeIndex::new();
        assert!(idx.lookup(b"ghost").is_empty());
    }

    #[test]
    fn duplicate_insert_is_idempotent() {
        let mut idx = BTreeIndex::new();
        idx.insert(b"k", d(1));
        idx.insert(b"k", d(1));
        assert_eq!(idx.lookup(b"k"), vec![d(1)]);
        assert_eq!(idx.len(), 1);
    }

    // ── Remove ─────────────────────────────────────────────────────────

    #[test]
    fn remove_existing_entry() {
        let mut idx = BTreeIndex::new();
        idx.insert(b"k", d(1));
        idx.insert(b"k", d(2));

        assert!(idx.remove(b"k", d(1)));
        assert_eq!(idx.lookup(b"k"), vec![d(2)]);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn remove_last_entry_cleans_up_key() {
        let mut idx = BTreeIndex::new();
        idx.insert(b"k", d(1));
        assert!(idx.remove(b"k", d(1)));
        assert!(idx.lookup(b"k").is_empty());
        assert!(idx.is_empty());
    }

    #[test]
    fn remove_nonexistent_returns_false() {
        let mut idx = BTreeIndex::new();
        assert!(!idx.remove(b"k", d(1)));

        idx.insert(b"k", d(1));
        assert!(!idx.remove(b"k", d(99)));
    }

    // ── Range ──────────────────────────────────────────────────────────

    #[test]
    fn range_inclusive() {
        let mut idx = BTreeIndex::new();
        idx.insert(b"a", d(1));
        idx.insert(b"b", d(2));
        idx.insert(b"c", d(3));
        idx.insert(b"d", d(4));

        let result = idx.range(b"b".to_vec()..=b"c".to_vec());
        assert_eq!(result, vec![d(2), d(3)]);
    }

    #[test]
    fn range_exclusive_end() {
        let mut idx = BTreeIndex::new();
        idx.insert(b"a", d(1));
        idx.insert(b"b", d(2));
        idx.insert(b"c", d(3));
        idx.insert(b"d", d(4));

        let result = idx.range(b"b".to_vec()..b"d".to_vec());
        assert_eq!(result, vec![d(2), d(3)]);
    }

    #[test]
    fn range_unbounded_start() {
        let mut idx = BTreeIndex::new();
        idx.insert(b"a", d(1));
        idx.insert(b"b", d(2));
        idx.insert(b"c", d(3));

        let result = idx.range(..b"c".to_vec());
        assert_eq!(result, vec![d(1), d(2)]);
    }

    #[test]
    fn range_unbounded_end() {
        let mut idx = BTreeIndex::new();
        idx.insert(b"a", d(1));
        idx.insert(b"b", d(2));
        idx.insert(b"c", d(3));

        let result = idx.range(b"b".to_vec()..);
        assert_eq!(result, vec![d(2), d(3)]);
    }

    #[test]
    fn range_full() {
        let mut idx = BTreeIndex::new();
        idx.insert(b"x", d(1));
        idx.insert(b"y", d(2));

        let all: std::ops::RangeFrom<Vec<u8>> = vec![]..;
        let result = idx.range(all);
        assert_eq!(result, vec![d(1), d(2)]);
    }

    #[test]
    fn range_empty_result() {
        let mut idx = BTreeIndex::new();
        idx.insert(b"a", d(1));
        idx.insert(b"z", d(2));

        let result = idx.range(b"m".to_vec()..b"n".to_vec());
        assert!(result.is_empty());
    }

    #[test]
    fn range_multiple_docs_per_key() {
        let mut idx = BTreeIndex::new();
        idx.insert(b"a", d(1));
        idx.insert(b"a", d(2));
        idx.insert(b"b", d(3));
        idx.insert(b"b", d(4));
        idx.insert(b"c", d(5));

        let result = idx.range(b"a".to_vec()..=b"b".to_vec());
        assert_eq!(result, vec![d(1), d(2), d(3), d(4)]);
    }

    #[test]
    fn range_preserves_key_order() {
        let mut idx = BTreeIndex::new();
        // Insert out of order
        idx.insert(b"c", d(3));
        idx.insert(b"a", d(1));
        idx.insert(b"b", d(2));

        let all: std::ops::RangeFrom<Vec<u8>> = vec![]..;
        let result = idx.range(all);
        assert_eq!(result, vec![d(1), d(2), d(3)]);
    }

    // ── Edge cases ─────────────────────────────────────────────────────

    #[test]
    fn empty_key_works() {
        let mut idx = BTreeIndex::new();
        idx.insert(b"", d(1));
        assert_eq!(idx.lookup(b""), vec![d(1)]);
    }

    #[test]
    fn is_empty_and_len() {
        let mut idx = BTreeIndex::new();
        assert!(idx.is_empty());
        assert_eq!(idx.len(), 0);

        idx.insert(b"k", d(1));
        assert!(!idx.is_empty());
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn default_is_empty() {
        let idx = BTreeIndex::default();
        assert!(idx.is_empty());
    }

    #[test]
    fn many_docs_per_key() {
        let mut idx = BTreeIndex::new();
        for i in 0..100 {
            idx.insert(b"shared", d(i));
        }
        assert_eq!(idx.len(), 100);
        let results = idx.lookup(b"shared");
        assert_eq!(results.len(), 100);
        for (i, doc) in results.iter().enumerate() {
            assert_eq!(doc.0, i as u64);
        }
    }

    #[test]
    fn interleaved_insert_remove_range() {
        let mut idx = BTreeIndex::new();
        idx.insert(b"a", d(1));
        idx.insert(b"b", d(2));
        idx.insert(b"c", d(3));
        idx.remove(b"b", d(2));

        let result = idx.range(b"a".to_vec()..=b"c".to_vec());
        assert_eq!(result, vec![d(1), d(3)]);
        assert_eq!(idx.len(), 2);
    }

    #[test]
    fn numeric_key_ordering() {
        // Big-endian encoded u32 keys preserve numeric order
        let mut idx = BTreeIndex::new();
        idx.insert(&100u32.to_be_bytes(), d(1));
        idx.insert(&200u32.to_be_bytes(), d(2));
        idx.insert(&150u32.to_be_bytes(), d(3));

        let result = idx.range(100u32.to_be_bytes().to_vec()..=200u32.to_be_bytes().to_vec());
        assert_eq!(result, vec![d(1), d(3), d(2)]); // 100, 150, 200
    }
}
