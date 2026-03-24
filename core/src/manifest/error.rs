/// Errors that can occur during manifest operations.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("checksum mismatch at offset {offset}: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch {
        offset: u64,
        expected: u32,
        actual: u32,
    },

    #[error("corrupt record at offset {offset}: {reason}")]
    CorruptRecord { offset: u64, reason: String },

    #[error("unknown record tag {0} at offset {1}")]
    UnknownTag(u8, u64),

    #[error("duplicate table id {0}")]
    DuplicateTable(u64),

    #[error("table id {0} not found")]
    TableNotFound(u64),
}
