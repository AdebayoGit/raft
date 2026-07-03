/// Errors that can occur during WAL operations.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("checksum mismatch at offset {offset}: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch {
        offset: u64,
        expected: u32,
        actual: u32,
    },

    #[error("incomplete entry at offset {offset}: needed {needed} bytes, got {available}")]
    IncompleteEntry {
        offset: u64,
        needed: usize,
        available: usize,
    },

    #[error("payload length {len} at offset {offset} exceeds maximum {max}")]
    PayloadTooLarge { offset: u64, len: usize, max: usize },

    #[error(
        "encrypted entry failed authentication at offset {offset} (wrong key or corrupted data)"
    )]
    BadCiphertext { offset: u64 },

    #[error("encryption failed: {0}")]
    Encryption(String),
}
