//! Mutation events emitted by the pub/sub bus.

use crate::index::DocId;

/// The kind of mutation that occurred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MutationType {
    /// A new document was inserted.
    Insert,
    /// An existing document was updated.
    Update,
    /// A document was deleted.
    Delete,
}

/// A single mutation event broadcast to subscribers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationEvent {
    /// The collection that was mutated.
    pub collection: String,
    /// The document that was affected.
    pub doc_id: DocId,
    /// What kind of mutation occurred.
    pub mutation_type: MutationType,
}

impl MutationEvent {
    pub fn insert(collection: impl Into<String>, doc_id: DocId) -> Self {
        Self {
            collection: collection.into(),
            doc_id,
            mutation_type: MutationType::Insert,
        }
    }

    pub fn update(collection: impl Into<String>, doc_id: DocId) -> Self {
        Self {
            collection: collection.into(),
            doc_id,
            mutation_type: MutationType::Update,
        }
    }

    pub fn delete(collection: impl Into<String>, doc_id: DocId) -> Self {
        Self {
            collection: collection.into(),
            doc_id,
            mutation_type: MutationType::Delete,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_event() {
        let e = MutationEvent::insert("users", DocId(1));
        assert_eq!(e.collection, "users");
        assert_eq!(e.doc_id, DocId(1));
        assert_eq!(e.mutation_type, MutationType::Insert);
    }

    #[test]
    fn update_event() {
        let e = MutationEvent::update("orders", DocId(42));
        assert_eq!(e.mutation_type, MutationType::Update);
    }

    #[test]
    fn delete_event() {
        let e = MutationEvent::delete("sessions", DocId(99));
        assert_eq!(e.mutation_type, MutationType::Delete);
    }

    #[test]
    fn clone_and_eq() {
        let a = MutationEvent::insert("x", DocId(1));
        let b = a.clone();
        assert_eq!(a, b);
    }
}
