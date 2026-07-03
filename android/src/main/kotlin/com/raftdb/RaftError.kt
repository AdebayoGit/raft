package com.raftdb

/**
 * Maps the C `RftError` enum (uint32_t) to Kotlin sealed exceptions.
 *
 * Error codes match `core/include/raft.h`:
 * - 0 = OK (no exception)
 * - 1 = NullPointer
 * - 2 = InvalidUtf8
 * - 3 = IoError
 * - 4 = NotFound
 * - 5 = BufferTooSmall
 * - 6 = InvalidJson
 * - 7 = TransactionConflict
 * - 8 = InvalidHandle
 * - 9 = UnknownSubscription
 * - 10 = InternalPanic
 * - 11 = InvalidPath
 * - 12 = DartApiNotInitialized
 * - 13 = PayloadTooLarge
 * - 14 = UnsupportedVersion
 */
sealed class RaftError(message: String) : Exception(message) {

    /** A required pointer argument was null (code 1). */
    class NullPointer : RaftError("A required pointer argument was null")

    /** A string argument was not valid UTF-8 (code 2). */
    class InvalidUtf8 : RaftError("A string argument was not valid UTF-8")

    /** An I/O or storage engine error occurred (code 3). */
    class IoError : RaftError("An I/O or storage engine error occurred")

    /** The requested key was not found (code 4). */
    class NotFound : RaftError("The requested key was not found")

    /** The caller-provided buffer is too small (code 5). */
    class BufferTooSmall : RaftError("The caller-provided buffer is too small")

    /** A document or filter passed via JSON failed to parse (code 6). */
    class InvalidJson : RaftError("A document or filter passed via JSON failed to parse")

    /** A transaction commit failed because a tracked document was modified concurrently (code 7). */
    class TransactionConflict : RaftError(
        "Transaction commit conflicted with a concurrent write",
    )

    /** A handle (transaction, query result, subscription) is invalid (code 8). */
    class InvalidHandle : RaftError("Native handle is invalid or already consumed")

    /** A subscription id passed to unobserve is not registered (code 9). */
    class UnknownSubscription : RaftError("Subscription id is not registered")

    /** The native core panicked; close and reopen the database (code 10). */
    class InternalPanic : RaftError(
        "Internal panic in native core; close and reopen the database",
    )

    /** The database path is invalid or escapes the confinement root (code 11). */
    class InvalidPath : RaftError(
        "Invalid database path (empty, contains \"..\", or escapes the confinement root)",
    )

    /** The Dart API was used before rft_dart_init — not applicable on Android (code 12). */
    class DartApiNotInitialized : RaftError(
        "Dart API not initialized (rft_dart_init was not called)",
    )

    /** A JSON payload exceeds its size cap (code 13). */
    class PayloadTooLarge : RaftError("JSON payload exceeds its size cap")

    /** A JSON envelope declared an unsupported schema version (code 14). */
    class UnsupportedVersion : RaftError(
        "JSON envelope declared an unsupported schema version",
    )

    /** An unknown error code was returned (defensive). */
    class Unknown(code: Int) : RaftError("Unknown raft error code: $code")

    companion object {
        /** Convert a raw C error code to the corresponding [RaftError], or null for OK. */
        fun fromCode(code: Int): RaftError? = when (code) {
            0 -> null
            1 -> NullPointer()
            2 -> InvalidUtf8()
            3 -> IoError()
            4 -> NotFound()
            5 -> BufferTooSmall()
            6 -> InvalidJson()
            7 -> TransactionConflict()
            8 -> InvalidHandle()
            9 -> UnknownSubscription()
            10 -> InternalPanic()
            11 -> InvalidPath()
            12 -> DartApiNotInitialized()
            13 -> PayloadTooLarge()
            14 -> UnsupportedVersion()
            else -> Unknown(code)
        }

        /**
         * Throw if [code] is non-zero.
         *
         * @throws RaftError subclass matching the error code.
         */
        fun check(code: Int) {
            fromCode(code)?.let { throw it }
        }
    }
}
