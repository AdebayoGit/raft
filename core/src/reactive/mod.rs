//! Internal pub/sub bus and live query subscriptions.
//!
//! Built on [`tokio::sync::broadcast`]. The [`EventBus`] emits
//! [`MutationEvent`]s whenever a document is inserted, updated, or deleted.
//! [`LiveQuery`] subscribes to the bus, re-evaluates a query on each
//! relevant mutation, and emits a [`QueryDiff`] when results change.
//!
//! Gated behind the `async` feature flag.

mod bus;
mod event;
mod live_query;

pub use bus::EventBus;
pub use event::{MutationEvent, MutationOrigin, MutationType};
pub use live_query::{LiveQuery, QueryDiff, QueryRunner};
