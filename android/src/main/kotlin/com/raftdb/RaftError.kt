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
