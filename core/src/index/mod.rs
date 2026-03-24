//! Secondary indexes — hash (equality) and B-tree (range) lookups.
//!
//! Both index types map opaque byte keys (`Vec<u8>`) to sets of [`DocId`]s.
//! The [`Index`] trait provides a uniform interface for insert, remove,
//! exact-match lookup, and range scan.

mod btree;
mod hash;

pub use btree::BTreeIndex;
pub use hash::HashIndex;

use std::ops::RangeBounds;

use serde::{Deserialize, Serialize};

/// Unique document identifier within a collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DocId(pub u64);

/// Uniform interface for secondary indexes.
pub trait Index {
    /// Associates `doc_id` with `key`. Duplicate `(key, doc_id)` pairs are
    /// silently ignored (set semantics).
    fn insert(&mut self, key: &[u8], doc_id: DocId);

    /// Removes the association between `key` and `doc_id`.
    ///
    /// Returns `true` if the entry existed and was removed.
    fn remove(&mut self, key: &[u8], doc_id: DocId) -> bool;

    /// Returns all document IDs associated with the exact `key`, sorted.
    fn lookup(&self, key: &[u8]) -> Vec<DocId>;

    /// Returns all document IDs whose keys fall within `range`, sorted.
    ///
    /// Implementations that do not support ordered traversal (e.g. hash)
    /// may return an empty result or fall back to a full scan.
    fn range(&self, range: impl RangeBounds<Vec<u8>>) -> Vec<DocId>;

    /// Returns the total number of `(key, doc_id)` entries in the index.
    fn len(&self) -> usize;

    /// Returns `true` if the index contains no entries.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
