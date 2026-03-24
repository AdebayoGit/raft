//! Optimistic concurrency transactions.
//!
//! A [`Transaction`] accumulates reads and writes in a local buffer.
//! On [`commit`](Transaction::commit), the transaction validates that
//! every document read during the transaction still has the same version
//! in the store. If any version has changed (concurrent write), the
//! commit fails with [`TransactionError::Conflict`] and no writes are
//! applied.
//!
//! ```text
//! let mut txn = Transaction::begin(&store);
//! let doc = txn.read(DocId(1))?;
//! txn.put(updated_doc);
//! txn.commit()?;       // fails if DocId(1) was written by another txn
//! ```

mod error;
mod store;
mod txn;

pub use error::TransactionError;
pub use store::{MemVersionedStore, VersionedStore};
pub use txn::Transaction;
