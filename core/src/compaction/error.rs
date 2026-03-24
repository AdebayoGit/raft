use crate::sstable::SSTableError;

/// Errors that can occur during compaction.
#[derive(Debug, thiserror::Error)]
pub enum CompactionError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("SSTable error: {0}")]
    SSTable(#[from] SSTableError),
}
