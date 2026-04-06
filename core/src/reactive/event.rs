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

/// Whether a mutation originated locally or from a remote sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum MutationOrigin {
    /// The mutation was performed by the local device.
    #[default]
    Local,
    /// The mutation was applied from a remote sync.
    Remote,
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
    /// Whether this mutation originated locally or from a remote sync.
    pub origin: MutationOrigin,
}

impl MutationEvent {
    pub fn insert(collection: impl Into<String>, doc_id: DocId) -> Self {
        Self {
            collection: collection.into(),
            doc_id,
            mutation_type: MutationType::Insert,
            origin: MutationOrigin::Local,
        }
    }

    pub fn update(collection: impl Into<String>, doc_id: DocId) -> Self {
        Self {
            collection: collection.into(),
            doc_id,
            mutation_type: MutationType::Update,
            origin: MutationOrigin::Local,
        }
    }

    pub fn delete(collection: impl Into<String>, doc_id: DocId) -> Self {
        Self {
            collection: collection.into(),
            doc_id,
            mutation_type: MutationType::Delete,
            origin: MutationOrigin::Local,
        }
    }

    /// Returns a copy of this event with the given origin.
    pub fn with_origin(mut self, origin: MutationOrigin) -> Self {
        self.origin = origin;
        self
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
