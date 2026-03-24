//! SSTable — immutable sorted string table files.
//!
//! An SSTable is the on-disk representation of a frozen memtable. Data is
//! written once in sorted key order and never modified. The file layout:
//!
//! ```text
//! ┌───────────────────────┐
//! │   Data Block 0        │  ← key-value pairs, length-prefixed
//! │   Data Block 1        │
//! │   ...                 │
//! │   Data Block N        │
//! ├───────────────────────┤
//! │   Bloom Filter        │  ← bit vector for membership checks
//! ├───────────────────────┤
//! │   Index Block         │  ← first_key → (block_offset, block_len) per block
//! ├───────────────────────┤
//! │   Footer (32 bytes)   │  ← bloom offset, index offset, entry count, magic
//! └───────────────────────┘
//! ```

mod bloom;
mod error;
mod reader;
mod writer;

pub use bloom::BloomFilter;
pub use error::SSTableError;
pub use reader::SSTableReader;
pub use writer::SSTableWriter;

/// Magic bytes written at the end of every SSTable footer.
const SSTABLE_MAGIC: [u8; 4] = *b"RFST";

/// Default target size for a single data block (4 KiB).
const DEFAULT_BLOCK_SIZE: usize = 4096;
