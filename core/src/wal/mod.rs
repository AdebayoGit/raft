//! Write-Ahead Log — append-only, HLC timestamps, crc32 checksums.
//!
//! The WAL is the foundation of the storage engine. Every mutation is first
//! written here before being applied to the memtable. Entries are binary-encoded
//! using the `bytes` crate and protected by crc32 checksums.

mod entry;
mod error;
mod writer;

pub use entry::{HlcTimestamp, WalEntry};
pub use error::WalError;
pub use writer::Wal;
