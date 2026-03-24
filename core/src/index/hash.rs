//! Hash-based secondary index for equality lookups.
//!
//! Uses `HashMap<Vec<u8>, BTreeSet<DocId>>` under the hood. Equality
//! lookups are O(1) amortised. Range queries are not natively supported
//! and return an empty result — use [`BTreeIndex`](super::BTreeIndex)
//! for range scans.

use std::collections::{BTreeSet, HashMap};
use std::ops::RangeBounds;

use super::{DocId, Index};

/// A hash-based index optimised for equality lookups.
#[derive(Debug, Clone)]
pub struct HashIndex {
    map: HashMap<Vec<u8>, BTreeSet<DocId>>,
    len: usize,
}

impl Default for HashIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl HashIndex {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            len: 0,
        }
    }
}

impl Index for HashIndex {
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

    /// Hash indexes do not support ordered range scans. Always returns
    /// an empty vector — use [`BTreeIndex`](super::BTreeIndex) instead.
    fn range(&self, _range: impl RangeBounds<Vec<u8>>) -> Vec<DocId> {
        Vec::new()
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

    #[test]
    fn insert_and_lookup() {
        let mut idx = HashIndex::new();
        idx.insert(b"alice", d(1));
        idx.insert(b"alice", d(2));
        idx.insert(b"bob", d(3));

        assert_eq!(idx.lookup(b"alice"), vec![d(1), d(2)]);
        assert_eq!(idx.lookup(b"bob"), vec![d(3)]);
        assert_eq!(idx.len(), 3);
    }

    #[test]
    fn lookup_missing_key_returns_empty() {
        let idx = HashIndex::new();
        assert!(idx.lookup(b"ghost").is_empty());
    }

    #[test]
    fn duplicate_insert_is_idempotent() {
        let mut idx = HashIndex::new();
        idx.insert(b"k", d(1));
        idx.insert(b"k", d(1));
        assert_eq!(idx.lookup(b"k"), vec![d(1)]);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn remove_existing_entry() {
        let mut idx = HashIndex::new();
        idx.insert(b"k", d(1));
        idx.insert(b"k", d(2));

        assert!(idx.remove(b"k", d(1)));
        assert_eq!(idx.lookup(b"k"), vec![d(2)]);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn remove_last_entry_cleans_up_key() {
        let mut idx = HashIndex::new();
        idx.insert(b"k", d(1));
        assert!(idx.remove(b"k", d(1)));
        assert!(idx.lookup(b"k").is_empty());
        assert!(idx.is_empty());
    }

    #[test]
    fn remove_nonexistent_returns_false() {
        let mut idx = HashIndex::new();
        assert!(!idx.remove(b"k", d(1)));

        idx.insert(b"k", d(1));
        assert!(!idx.remove(b"k", d(99)));
    }

    #[test]
    fn range_returns_empty() {
        let mut idx = HashIndex::new();
        idx.insert(b"a", d(1));
        idx.insert(b"z", d(2));
        let result = idx.range(b"a".to_vec()..=b"z".to_vec());
        assert!(result.is_empty());
    }

    #[test]
    fn is_empty_and_len() {
        let mut idx = HashIndex::new();
        assert!(idx.is_empty());
        assert_eq!(idx.len(), 0);

        idx.insert(b"k", d(1));
        assert!(!idx.is_empty());
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn empty_key_works() {
        let mut idx = HashIndex::new();
        idx.insert(b"", d(1));
        assert_eq!(idx.lookup(b""), vec![d(1)]);
    }

    #[test]
    fn default_is_empty() {
        let idx = HashIndex::default();
        assert!(idx.is_empty());
    }

    #[test]
    fn many_docs_per_key() {
        let mut idx = HashIndex::new();
        for i in 0..100 {
            idx.insert(b"shared", d(i));
        }
        assert_eq!(idx.len(), 100);
        let results = idx.lookup(b"shared");
        assert_eq!(results.len(), 100);
        // BTreeSet guarantees sorted order
        for (i, doc) in results.iter().enumerate() {
            assert_eq!(doc.0, i as u64);
        }
    }
}
