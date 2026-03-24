/// Errors that can occur during SSTable operations.
#[derive(Debug, thiserror::Error)]
pub enum SSTableError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid SSTable: bad magic bytes")]
    BadMagic,

    #[error("corrupt data block at offset {offset}: {reason}")]
    CorruptBlock { offset: u64, reason: String },

    #[error("corrupt index block: {0}")]
    CorruptIndex(String),

    #[error("empty iterator — nothing to write")]
    EmptyInput,
}
