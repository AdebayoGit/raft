//! CRDT primitives for conflict-free replicated data.
//!
//! Every document field is backed by one of these CRDT types. Merging is
//! deterministic: two devices that have seen the same set of mutations will
//! always converge to the identical state, regardless of the order they
//! received those mutations.

mod counter;
mod lww;
mod orset;

pub use counter::Counter;
pub use lww::LwwRegister;
pub use orset::OrSet;

/// Deterministic merge of two replicas of the same logical value.
///
/// After `a.merge(&b)`, `a` must contain the union of information from both
/// `a` and `b`. The operation is idempotent, commutative, and associative.
pub trait Merge {
    fn merge(&mut self, other: &Self);
}
