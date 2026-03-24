//! Compaction — levelled merge strategy for SSTables.
//!
//! SSTables accumulate at each level. When the count at a level exceeds a
//! configurable threshold, all tables at that level are merged into a single
//! SSTable promoted to the next level. During the merge:
//!
//! - Duplicate keys are resolved by keeping the entry from the **newest**
//!   (highest-index) table — the one flushed most recently.
//! - Tombstones are preserved (they must propagate to deeper levels).
//! - The merge produces sorted output written via `SSTableWriter`.
//!
//! The scheduler is idle-aware: callers invoke `run_if_idle()` when the
//! device is idle. No background threads are spawned (that comes with
//! the `async` feature flag).

mod error;
mod merge;
mod scheduler;

pub use error::CompactionError;
pub use scheduler::{CompactionConfig, CompactionScheduler, CompactionStats};
