//! C-compatible error enum for FFI.

/// Error codes returned by all `rft_*` functions.
///
/// Represented as a `#[repr(u32)]` enum for stable C ABI. Zero means
/// success; non-zero values indicate specific error conditions.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RftError {
    /// Operation succeeded.
    Ok = 0,
    /// A required pointer argument was null.
    NullPointer = 1,
    /// A string argument was not valid UTF-8.
    InvalidUtf8 = 2,
    /// An I/O or storage engine error occurred.
    IoError = 3,
    /// The requested key/document was not found.
    NotFound = 4,
    /// The caller-provided buffer is too small. Check `out_len` for the
    /// required size.
    BufferTooSmall = 5,
    /// A document or filter passed via JSON failed to parse.
    InvalidJson = 6,
    /// A transaction commit failed because a tracked document was modified
    /// concurrently.
    TransactionConflict = 7,
    /// A handle (transaction, query result, subscription) is invalid —
    /// already consumed, freed, or never created.
    InvalidHandle = 8,
    /// A subscription id passed to [`rft_unobserve`](super::rft_unobserve)
    /// is not registered.
    UnknownSubscription = 9,
    /// An internal panic was caught at the FFI boundary. The database
    /// may be in an inconsistent in-memory state; the caller should
    /// close and reopen the handle.
    InternalPanic = 10,
    /// A database path failed validation: empty, contains `..`
    /// components, or escapes the confinement root passed to
    /// [`rft_open_at`](super::rft_open_at) (including via symlinks).
    InvalidPath = 11,
    /// A `rft_observe_*_dart_port` function was called before
    /// [`rft_dart_init`](super::rft_dart_init) registered the Dart VM's
    /// `Dart_PostCObject_DL` function.
    DartApiNotInitialized = 12,
    /// A JSON envelope exceeds its size cap
    /// ([`RFT_MAX_DOC_JSON_LEN`](super::RFT_MAX_DOC_JSON_LEN) for
    /// documents, [`RFT_MAX_QUERY_JSON_LEN`](super::RFT_MAX_QUERY_JSON_LEN)
    /// for query specs).
    PayloadTooLarge = 13,
    /// A JSON envelope declared a `"v"` schema version this build does
    /// not support.
    UnsupportedVersion = 14,
}
