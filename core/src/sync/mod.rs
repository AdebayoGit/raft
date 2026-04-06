//! Sync primitives — authority configuration and conflict resolution.
//!
//! The [`SyncAuthority`] enum controls per-collection conflict resolution
//! strategy. The [`ConflictResolver`] dispatches merge operations based on
//! the configured authority mode without modifying the pure CRDT [`Merge`]
//! trait.

mod authority;
mod resolver;

pub use authority::{MergeContext, SyncAuthority};
pub use resolver::ConflictResolver;
