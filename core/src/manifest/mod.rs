//! Manifest — tracks live SSTables, their levels, key ranges, and DB state.
//!
//! The manifest is a binary append-only log of version edits. On open it
//! replays the log to reconstruct the current `DbVersion`. Each edit is
//! checksummed so corruption is detected on recovery.
//!
//! ```text
//! ┌─────────────────────────┐
//! │  Record 0 (snapshot)    │  ← full version after last compaction
//! │  Record 1 (add table)   │
//! │  Record 2 (remove table)│
//! │  ...                    │
//! └─────────────────────────┘
//! ```

mod error;
mod record;
mod version;

pub use error::ManifestError;
pub use record::{ManifestRecord, SSTableMeta, TableId};
pub use version::Manifest;
