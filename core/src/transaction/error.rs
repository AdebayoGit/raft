//! Transaction error types.

use crate::index::DocId;

/// Errors that can occur during transaction commit.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransactionError {
    /// A document read during the transaction was modified by another
    /// writer before this transaction could commit.
    #[error("conflict: document {doc_id:?} version changed (read {read_version}, current {current_version})")]
    Conflict {
        doc_id: DocId,
        read_version: u64,
        current_version: u64,
    },

    /// The transaction was already committed or rolled back.
    #[error("transaction already finalised")]
    AlreadyFinalised,

    /// The document was not found in the store.
    #[error("document {0:?} not found")]
    NotFound(DocId),
}
