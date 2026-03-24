//! Versioned document store trait and in-memory implementation.
//!
//! The [`VersionedStore`] trait extends the concept of a document store
//! with per-document version tracking, enabling optimistic concurrency
//! control in [`Transaction`](super::Transaction).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::index::DocId;
use crate::query::Document;

use super::error::TransactionError;

/// A versioned document entry in the store.
#[derive(Debug, Clone)]
pub struct VersionedDocument {
    pub document: Document,
    pub version: u64,
}

/// Trait for a document store that tracks per-document versions.
///
/// Transactions read through this trait (capturing versions) and commit
/// by calling [`apply_batch`](Self::apply_batch) with version validation.
pub trait VersionedStore {
    /// Read a document and its current version.
    fn get_versioned(&self, id: DocId) -> Option<VersionedDocument>;

    /// Read the current version of a document without fetching its content.
    fn current_version(&self, id: DocId) -> Option<u64>;

    /// Atomically apply a batch of writes and deletes, but only if every
    /// `read_set` entry still matches the current version.
    ///
    /// `read_set`: `(DocId, version_at_read_time)` — the versions the
    /// transaction observed.
    ///
    /// `puts`: documents to upsert (version is bumped by the store).
    ///
    /// `deletes`: document IDs to remove.
    ///
    /// Returns `Ok(())` on success, or the first conflict found.
    fn apply_batch(
        &self,
        read_set: &[(DocId, u64)],
        puts: Vec<Document>,
        deletes: &[DocId],
    ) -> Result<(), TransactionError>;
}

/// An in-memory [`VersionedStore`] for testing and lightweight use.
///
/// Thread-safe via internal `Mutex`. Versions start at 1 and increment
/// on each write.
#[derive(Debug, Clone)]
pub struct MemVersionedStore {
    inner: Arc<Mutex<StoreInner>>,
}

#[derive(Debug)]
struct StoreInner {
    docs: HashMap<DocId, VersionedDocument>,
    /// Monotonically increasing version counter.
    next_version: u64,
}

impl Default for MemVersionedStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemVersionedStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(StoreInner {
                docs: HashMap::new(),
                next_version: 1,
            })),
        }
    }

    /// Insert a document directly (outside of a transaction), for test setup.
    pub fn insert(&self, doc: Document) {
        let mut inner = self.inner.lock().unwrap();
        let version = inner.next_version;
        inner.next_version += 1;
        inner.docs.insert(
            doc.id,
            VersionedDocument {
                document: doc,
                version,
            },
        );
    }

    /// Return all documents (unversioned, for assertions).
    pub fn all_documents(&self) -> Vec<Document> {
        let inner = self.inner.lock().unwrap();
        inner.docs.values().map(|vd| vd.document.clone()).collect()
    }

    /// Number of documents in the store.
    pub fn count(&self) -> usize {
        self.inner.lock().unwrap().docs.len()
    }
}

impl VersionedStore for MemVersionedStore {
    fn get_versioned(&self, id: DocId) -> Option<VersionedDocument> {
        self.inner.lock().unwrap().docs.get(&id).cloned()
    }

    fn current_version(&self, id: DocId) -> Option<u64> {
        self.inner.lock().unwrap().docs.get(&id).map(|vd| vd.version)
    }

    fn apply_batch(
        &self,
        read_set: &[(DocId, u64)],
        puts: Vec<Document>,
        deletes: &[DocId],
    ) -> Result<(), TransactionError> {
        let mut inner = self.inner.lock().unwrap();

        // Phase 1: Validate read set.
        for &(doc_id, read_version) in read_set {
            let current = inner.docs.get(&doc_id).map(|vd| vd.version).unwrap_or(0);
            if current != read_version {
                return Err(TransactionError::Conflict {
                    doc_id,
                    read_version,
                    current_version: current,
                });
            }
        }

        // Phase 2: Apply writes.
        for doc in puts {
            let version = inner.next_version;
            inner.next_version += 1;
            inner.docs.insert(
                doc.id,
                VersionedDocument {
                    document: doc,
                    version,
                },
            );
        }

        // Phase 3: Apply deletes.
        for id in deletes {
            inner.docs.remove(id);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Value;

    fn doc(id: u64, name: &str) -> Document {
        Document::new(DocId(id)).with_field("name", Value::String(name.into()))
    }

    #[test]
    fn insert_and_get() {
        let store = MemVersionedStore::new();
        store.insert(doc(1, "Alice"));

        let vd = store.get_versioned(DocId(1)).unwrap();
        assert_eq!(vd.document.id, DocId(1));
        assert_eq!(vd.version, 1);
    }

    #[test]
    fn versions_increment() {
        let store = MemVersionedStore::new();
        store.insert(doc(1, "Alice"));
        store.insert(doc(2, "Bob"));

        assert_eq!(store.get_versioned(DocId(1)).unwrap().version, 1);
        assert_eq!(store.get_versioned(DocId(2)).unwrap().version, 2);
    }

    #[test]
    fn apply_batch_succeeds() {
        let store = MemVersionedStore::new();
        store.insert(doc(1, "Alice"));

        let result = store.apply_batch(
            &[(DocId(1), 1)],
            vec![doc(1, "Alice Updated")],
            &[],
        );
        assert!(result.is_ok());

        let vd = store.get_versioned(DocId(1)).unwrap();
        assert_eq!(
            vd.document.get("name"),
            Some(&Value::String("Alice Updated".into()))
        );
        assert!(vd.version > 1);
    }

    #[test]
    fn apply_batch_conflict() {
        let store = MemVersionedStore::new();
        store.insert(doc(1, "Alice"));

        // Stale version (0 ≠ 1).
        let result = store.apply_batch(
            &[(DocId(1), 0)],
            vec![doc(1, "Alice Updated")],
            &[],
        );
        assert!(matches!(
            result,
            Err(TransactionError::Conflict {
                doc_id: DocId(1),
                read_version: 0,
                current_version: 1,
            })
        ));
    }

    #[test]
    fn apply_batch_deletes() {
        let store = MemVersionedStore::new();
        store.insert(doc(1, "Alice"));
        store.insert(doc(2, "Bob"));

        let result = store.apply_batch(
            &[(DocId(2), 2)],
            vec![],
            &[DocId(2)],
        );
        assert!(result.is_ok());
        assert!(store.get_versioned(DocId(2)).is_none());
        assert_eq!(store.count(), 1);
    }

    #[test]
    fn get_missing_returns_none() {
        let store = MemVersionedStore::new();
        assert!(store.get_versioned(DocId(99)).is_none());
        assert!(store.current_version(DocId(99)).is_none());
    }

    #[test]
    fn default_creates_empty_store() {
        let store = MemVersionedStore::default();
        assert_eq!(store.count(), 0);
    }
}
