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
    /// The requested key was not found.
    NotFound = 4,
    /// The caller-provided buffer is too small. Check `out_len` for the
    /// required size.
    BufferTooSmall = 5,
}
