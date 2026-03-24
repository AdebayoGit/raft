//! Transaction implementation — optimistic concurrency with versioned reads.

use std::collections::{HashMap, HashSet};

use crate::index::DocId;
use crate::query::Document;

use super::error::TransactionError;
use super::store::VersionedStore;

/// Transaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnState {
    Active,
    Committed,
    RolledBack,
}

/// An optimistic concurrency transaction.
///
/// Reads go through the transaction to record the version of each
/// document at read time. Writes and deletes are buffered locally.
/// On [`commit`](Self::commit), the transaction validates that all
/// read versions are still current, then atomically applies the writes.
///
/// If any version has changed, commit returns
/// [`TransactionError::Conflict`] and no writes are applied.
pub struct Transaction<'s, S: VersionedStore> {
    store: &'s S,
    state: TxnState,
    /// Documents read during the transaction: `DocId → version at read time`.
    read_set: HashMap<DocId, u64>,
    /// Documents to write on commit.
    write_set: HashMap<DocId, Document>,
    /// Documents to delete on commit.
    delete_set: HashSet<DocId>,
}

impl<'s, S: VersionedStore> Transaction<'s, S> {
    /// Begin a new transaction against `store`.
    pub fn begin(store: &'s S) -> Self {
        Self {
            store,
            state: TxnState::Active,
            read_set: HashMap::new(),
            write_set: HashMap::new(),
            delete_set: HashSet::new(),
        }
    }

    /// Read a document, recording its version for conflict detection.
    ///
    /// Returns the document if it exists, or `None`. The version is
    /// tracked either way (a read of a missing doc records version 0,
    /// so an insert by another transaction before commit is detected).
    pub fn read(&mut self, id: DocId) -> Result<Option<Document>, TransactionError> {
        self.ensure_active()?;

        // Check local write buffer first — read-your-own-writes.
        if let Some(doc) = self.write_set.get(&id) {
            return Ok(Some(doc.clone()));
        }
        if self.delete_set.contains(&id) {
            return Ok(None);
        }

        match self.store.get_versioned(id) {
            Some(vd) => {
                self.read_set.insert(id, vd.version);
                Ok(Some(vd.document))
            }
            None => {
                // Record version 0 so we detect if someone inserts this doc.
                self.read_set.insert(id, 0);
                Ok(None)
            }
        }
    }

    /// Buffer a document write. Applied atomically on commit.
    ///
    /// If the document was previously scheduled for deletion in this
    /// transaction, the delete is cancelled and replaced by this write.
    pub fn put(&mut self, doc: Document) -> Result<(), TransactionError> {
        self.ensure_active()?;
        self.delete_set.remove(&doc.id);
        self.write_set.insert(doc.id, doc);
        Ok(())
    }

    /// Buffer multiple document writes. Applied atomically on commit.
    pub fn put_batch(&mut self, docs: Vec<Document>) -> Result<(), TransactionError> {
        self.ensure_active()?;
        for doc in docs {
            self.delete_set.remove(&doc.id);
            self.write_set.insert(doc.id, doc);
        }
        Ok(())
    }

    /// Buffer a document deletion. Applied atomically on commit.
    ///
    /// If the document was previously scheduled for writing in this
    /// transaction, the write is cancelled.
    pub fn delete(&mut self, id: DocId) -> Result<(), TransactionError> {
        self.ensure_active()?;
        self.write_set.remove(&id);
        self.delete_set.insert(id);
        Ok(())
    }

    /// Validate the read set and atomically apply all buffered writes
    /// and deletes.
    ///
    /// Returns `Ok(())` on success. On conflict, returns
    /// [`TransactionError::Conflict`] and no writes are applied.
    pub fn commit(mut self) -> Result<(), TransactionError> {
        self.ensure_active()?;
        self.state = TxnState::Committed;

        let read_set: Vec<(DocId, u64)> = self.read_set.into_iter().collect();
        let puts: Vec<Document> = self.write_set.into_values().collect();
        let deletes: Vec<DocId> = self.delete_set.into_iter().collect();

        self.store.apply_batch(&read_set, puts, &deletes)
    }

    /// Discard all buffered writes and deletes without applying them.
    pub fn rollback(mut self) {
        self.state = TxnState::RolledBack;
        // Buffers are dropped when `self` goes out of scope.
    }

    /// Returns `true` if the transaction is still active (not committed
    /// or rolled back).
    pub fn is_active(&self) -> bool {
        self.state == TxnState::Active
    }

    /// Number of documents in the write buffer.
    pub fn pending_writes(&self) -> usize {
        self.write_set.len()
    }

    /// Number of documents in the delete buffer.
    pub fn pending_deletes(&self) -> usize {
        self.delete_set.len()
    }

    fn ensure_active(&self) -> Result<(), TransactionError> {
        if self.state != TxnState::Active {
            return Err(TransactionError::AlreadyFinalised);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Value;
    use crate::transaction::MemVersionedStore;

    fn doc(id: u64, name: &str) -> Document {
        Document::new(DocId(id)).with_field("name", Value::String(name.into()))
    }

    fn setup_store() -> MemVersionedStore {
        let store = MemVersionedStore::new();
        store.insert(doc(1, "Alice"));
        store.insert(doc(2, "Bob"));
        store.insert(doc(3, "Charlie"));
        store
    }

    // ── Basic read / write / commit ──

    #[test]
    fn begin_and_read() {
        let store = setup_store();
        let mut txn = Transaction::begin(&store);

        let alice = txn.read(DocId(1)).unwrap().unwrap();
        assert_eq!(alice.get("name"), Some(&Value::String("Alice".into())));
    }

    #[test]
    fn read_missing_returns_none() {
        let store = setup_store();
        let mut txn = Transaction::begin(&store);

        let result = txn.read(DocId(99)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn put_and_commit() {
        let store = setup_store();
        let mut txn = Transaction::begin(&store);

        let _alice = txn.read(DocId(1)).unwrap();
        txn.put(doc(1, "Alice Updated")).unwrap();
        assert_eq!(txn.pending_writes(), 1);

        txn.commit().unwrap();

        let vd = store.get_versioned(DocId(1)).unwrap();
        assert_eq!(
            vd.document.get("name"),
            Some(&Value::String("Alice Updated".into()))
        );
    }

    #[test]
    fn delete_and_commit() {
        let store = setup_store();
        let mut txn = Transaction::begin(&store);

        let _bob = txn.read(DocId(2)).unwrap();
        txn.delete(DocId(2)).unwrap();
        assert_eq!(txn.pending_deletes(), 1);

        txn.commit().unwrap();

        assert!(store.get_versioned(DocId(2)).is_none());
        assert_eq!(store.count(), 2);
    }

    #[test]
    fn put_batch() {
        let store = setup_store();
        let mut txn = Transaction::begin(&store);

        txn.put_batch(vec![
            doc(1, "Alice V2"),
            doc(2, "Bob V2"),
            doc(4, "Diana"),
        ])
        .unwrap();
        assert_eq!(txn.pending_writes(), 3);

        txn.commit().unwrap();

        assert_eq!(store.count(), 4);
        assert_eq!(
            store
                .get_versioned(DocId(4))
                .unwrap()
                .document
                .get("name"),
            Some(&Value::String("Diana".into()))
        );
    }

    // ── Read-your-own-writes ──

    #[test]
    fn read_own_writes() {
        let store = setup_store();
        let mut txn = Transaction::begin(&store);

        txn.put(doc(10, "NewDoc")).unwrap();
        let result = txn.read(DocId(10)).unwrap().unwrap();
        assert_eq!(result.get("name"), Some(&Value::String("NewDoc".into())));
    }

    #[test]
    fn read_after_local_delete_returns_none() {
        let store = setup_store();
        let mut txn = Transaction::begin(&store);

        txn.delete(DocId(1)).unwrap();
        let result = txn.read(DocId(1)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn put_cancels_pending_delete() {
        let store = setup_store();
        let mut txn = Transaction::begin(&store);

        txn.delete(DocId(1)).unwrap();
        assert_eq!(txn.pending_deletes(), 1);

        txn.put(doc(1, "Alice Revived")).unwrap();
        assert_eq!(txn.pending_deletes(), 0);
        assert_eq!(txn.pending_writes(), 1);

        txn.commit().unwrap();

        assert_eq!(
            store
                .get_versioned(DocId(1))
                .unwrap()
                .document
                .get("name"),
            Some(&Value::String("Alice Revived".into()))
        );
    }

    #[test]
    fn delete_cancels_pending_write() {
        let store = setup_store();
        let mut txn = Transaction::begin(&store);

        txn.put(doc(10, "Temp")).unwrap();
        assert_eq!(txn.pending_writes(), 1);

        txn.delete(DocId(10)).unwrap();
        assert_eq!(txn.pending_writes(), 0);
        assert_eq!(txn.pending_deletes(), 1);
    }

    // ── Rollback ──

    #[test]
    fn rollback_discards_writes() {
        let store = setup_store();
        let mut txn = Transaction::begin(&store);

        let _alice = txn.read(DocId(1)).unwrap();
        txn.put(doc(1, "Alice Rolled Back")).unwrap();
        txn.rollback();

        // Store unchanged.
        assert_eq!(
            store
                .get_versioned(DocId(1))
                .unwrap()
                .document
                .get("name"),
            Some(&Value::String("Alice".into()))
        );
    }

    #[test]
    fn rollback_discards_deletes() {
        let store = setup_store();
        let mut txn = Transaction::begin(&store);

        txn.delete(DocId(2)).unwrap();
        txn.rollback();

        assert!(store.get_versioned(DocId(2)).is_some());
    }

    #[test]
    fn drop_without_commit_is_implicit_rollback() {
        let store = setup_store();
        {
            let mut txn = Transaction::begin(&store);
            txn.put(doc(1, "Never Committed")).unwrap();
            // txn dropped here — no commit, no rollback
        }

        assert_eq!(
            store
                .get_versioned(DocId(1))
                .unwrap()
                .document
                .get("name"),
            Some(&Value::String("Alice".into()))
        );
    }

    // ── Conflict detection ──

    #[test]
    fn conflict_on_concurrent_write() {
        let store = setup_store();

        // Transaction 1 reads Alice.
        let mut txn1 = Transaction::begin(&store);
        let _alice = txn1.read(DocId(1)).unwrap();
        txn1.put(doc(1, "Alice from Txn1")).unwrap();

        // Transaction 2 reads and commits first.
        let mut txn2 = Transaction::begin(&store);
        let _alice2 = txn2.read(DocId(1)).unwrap();
        txn2.put(doc(1, "Alice from Txn2")).unwrap();
        txn2.commit().unwrap();

        // Transaction 1 tries to commit — should fail.
        let result = txn1.commit();
        assert!(matches!(
            result,
            Err(TransactionError::Conflict {
                doc_id: DocId(1),
                ..
            })
        ));

        // Store has txn2's value.
        assert_eq!(
            store
                .get_versioned(DocId(1))
                .unwrap()
                .document
                .get("name"),
            Some(&Value::String("Alice from Txn2".into()))
        );
    }

    #[test]
    fn conflict_on_concurrent_delete() {
        let store = setup_store();

        // Txn1 reads Bob.
        let mut txn1 = Transaction::begin(&store);
        let _bob = txn1.read(DocId(2)).unwrap();
        txn1.put(doc(2, "Bob Updated")).unwrap();

        // Txn2 deletes Bob and commits.
        let mut txn2 = Transaction::begin(&store);
        let _bob2 = txn2.read(DocId(2)).unwrap();
        txn2.delete(DocId(2)).unwrap();
        txn2.commit().unwrap();

        // Txn1 commit fails — Bob's version changed (deleted = version 0).
        let result = txn1.commit();
        assert!(matches!(result, Err(TransactionError::Conflict { .. })));
    }

    #[test]
    fn conflict_on_insert_of_previously_absent_doc() {
        let store = setup_store();

        // Txn1 reads DocId(99) — not found (version 0).
        let mut txn1 = Transaction::begin(&store);
        let missing = txn1.read(DocId(99)).unwrap();
        assert!(missing.is_none());

        // Another writer inserts DocId(99).
        store.insert(doc(99, "Surprise"));

        // Txn1 tries to commit — conflicts because DocId(99) now exists.
        txn1.put(doc(99, "From Txn1")).unwrap();
        let result = txn1.commit();
        assert!(matches!(
            result,
            Err(TransactionError::Conflict {
                doc_id: DocId(99),
                read_version: 0,
                ..
            })
        ));
    }

    #[test]
    fn no_conflict_on_non_overlapping_writes() {
        let store = setup_store();

        // Txn1 reads and writes Alice.
        let mut txn1 = Transaction::begin(&store);
        let _alice = txn1.read(DocId(1)).unwrap();
        txn1.put(doc(1, "Alice V2")).unwrap();

        // Txn2 reads and writes Bob (different doc).
        let mut txn2 = Transaction::begin(&store);
        let _bob = txn2.read(DocId(2)).unwrap();
        txn2.put(doc(2, "Bob V2")).unwrap();

        // Both commit successfully — no overlap.
        txn1.commit().unwrap();
        txn2.commit().unwrap();
    }

    #[test]
    fn write_without_read_has_no_conflict() {
        let store = setup_store();

        // Txn1 writes without reading — no read set, so no conflict check.
        let mut txn1 = Transaction::begin(&store);
        txn1.put(doc(1, "Blind Write")).unwrap();

        // Another writer modifies the same doc.
        store.insert(doc(1, "Concurrent Write"));

        // Txn1 commits — no read set means no conflict.
        txn1.commit().unwrap();

        // Last writer wins.
        let vd = store.get_versioned(DocId(1)).unwrap();
        assert_eq!(
            vd.document.get("name"),
            Some(&Value::String("Blind Write".into()))
        );
    }

    // ── Already-finalised checks ──

    #[test]
    fn operations_after_rollback_fail() {
        let store = setup_store();
        let mut txn = Transaction::begin(&store);
        txn.put(doc(1, "X")).unwrap();
        assert!(txn.is_active());
        txn.rollback();

        // Can't start a new txn from the rolled back one — it's consumed.
        // The API enforces this via move semantics (rollback takes self).
    }

    // ── Batch + conflict ──

    #[test]
    fn batch_write_with_conflict() {
        let store = setup_store();

        let mut txn = Transaction::begin(&store);
        let _alice = txn.read(DocId(1)).unwrap();
        let _bob = txn.read(DocId(2)).unwrap();

        // Concurrent write to Bob.
        store.insert(doc(2, "Bob Concurrent"));

        txn.put_batch(vec![doc(1, "Alice V2"), doc(2, "Bob V2")])
            .unwrap();

        // Commit fails because Bob's version changed.
        let result = txn.commit();
        assert!(matches!(result, Err(TransactionError::Conflict { .. })));

        // Neither write was applied.
        assert_eq!(
            store
                .get_versioned(DocId(1))
                .unwrap()
                .document
                .get("name"),
            Some(&Value::String("Alice".into()))
        );
    }

    #[test]
    fn batch_write_succeeds_when_no_conflict() {
        let store = setup_store();

        let mut txn = Transaction::begin(&store);
        let _alice = txn.read(DocId(1)).unwrap();

        txn.put_batch(vec![doc(1, "Alice V2"), doc(4, "Diana")])
            .unwrap();
        txn.commit().unwrap();

        assert_eq!(store.count(), 4);
    }

    // ── Edge cases ──

    #[test]
    fn empty_transaction_commits_successfully() {
        let store = setup_store();
        let txn = Transaction::begin(&store);
        txn.commit().unwrap();
    }

    #[test]
    fn read_only_transaction_commits() {
        let store = setup_store();
        let mut txn = Transaction::begin(&store);
        let _alice = txn.read(DocId(1)).unwrap();
        let _bob = txn.read(DocId(2)).unwrap();
        txn.commit().unwrap();
    }

    #[test]
    fn multiple_reads_of_same_doc() {
        let store = setup_store();
        let mut txn = Transaction::begin(&store);

        let a1 = txn.read(DocId(1)).unwrap().unwrap();
        let a2 = txn.read(DocId(1)).unwrap().unwrap();
        assert_eq!(a1, a2);
    }

    #[test]
    fn insert_new_doc_via_transaction() {
        let store = setup_store();
        let mut txn = Transaction::begin(&store);

        txn.put(doc(10, "NewUser")).unwrap();
        txn.commit().unwrap();

        let vd = store.get_versioned(DocId(10)).unwrap();
        assert_eq!(
            vd.document.get("name"),
            Some(&Value::String("NewUser".into()))
        );
    }

    #[test]
    fn is_active_flag() {
        let store = setup_store();
        let txn = Transaction::begin(&store);
        assert!(txn.is_active());
    }
}
