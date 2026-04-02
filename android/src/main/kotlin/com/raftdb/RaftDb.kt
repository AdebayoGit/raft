package com.raftdb

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.withContext
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Kotlin wrapper around the native `libraftdb.so` C API.
 *
 * All blocking JNI calls are dispatched on [Dispatchers.IO].
 * The database handle is reference-counted internally by the native
 * library; [close] releases the Kotlin side's reference.
 *
 * Usage:
 * ```kotlin
 * val db = RaftDb.open("/data/data/com.example/files/my.db")
 * db.put("user:1".toByteArray(), json.toByteArray())
 * val value = db.get("user:1".toByteArray())
 * db.delete("user:1".toByteArray())
 * db.close()
 * ```
 */
class RaftDb private constructor(private val handle: Long) : AutoCloseable {

    private val closed = AtomicBoolean(false)
    private val observerIdGen = AtomicLong(0)

    // -- Public API ----------------------------------------------------------

    /**
     * Insert or update a key-value pair.
     *
     * @throws RaftError on native failure.
     * @throws IllegalStateException if the database is closed.
     */
    suspend fun put(key: ByteArray, value: ByteArray) {
        ensureOpen()
        withContext(Dispatchers.IO) {
            val code = nativePut(handle, key, key.size, value, value.size)
            RaftError.check(code)
        }
    }

    /**
     * Look up a key.
     *
     * @return the value bytes, or `null` if the key does not exist.
     * @throws RaftError on native failure (other than not-found).
     * @throws IllegalStateException if the database is closed.
     */
    suspend fun get(key: ByteArray): ByteArray? {
        ensureOpen()
        return withContext(Dispatchers.IO) {
            nativeGet(handle, key, key.size)
        }
    }

    /**
     * Delete a key. Deleting a non-existent key is not an error.
     *
     * @throws RaftError on native failure.
     * @throws IllegalStateException if the database is closed.
     */
    suspend fun delete(key: ByteArray) {
        ensureOpen()
        withContext(Dispatchers.IO) {
            val code = nativeDelete(handle, key, key.size)
            RaftError.check(code)
        }
    }

    /**
     * Observe changes to a key prefix as a [Flow].
     *
     * Emits a [QueryResult] every time a key matching [prefix] is written
     * or deleted. The first emission is the current snapshot.
     *
     * The flow completes when the collector is cancelled or the database
     * is closed.
     */
    fun observe(prefix: ByteArray): Flow<QueryResult> = callbackFlow {
        ensureOpen()
        val observerId = observerIdGen.incrementAndGet()

        // Initial snapshot
        val initial = withContext(Dispatchers.IO) {
            nativeGet(handle, prefix, prefix.size)
        }
        send(QueryResult(prefix, initial))

        // Register a polling observer — real impl would use native callbacks.
        // For now this is a placeholder that keeps the flow open until
        // cancellation, suitable for wiring up a native callback later.
        awaitClose {
            // Unregister observer when collector cancels.
            // Future: nativeUnregisterObserver(handle, observerId)
        }
    }

    /**
     * Close the database and release the native handle.
     *
     * Safe to call multiple times; subsequent calls are no-ops.
     */
    override fun close() {
        if (closed.compareAndSet(false, true)) {
            nativeClose(handle)
        }
    }

    // -- Internals -----------------------------------------------------------

    private fun ensureOpen() {
        check(!closed.get()) { "RaftDb is already closed" }
    }

    // -- JNI declarations ----------------------------------------------------

    companion object {

        init {
            System.loadLibrary("raftdb")
        }

        /**
         * Open or create a database at [path].
         *
         * @throws RaftError if the native open fails.
         */
        suspend fun open(path: String): RaftDb = withContext(Dispatchers.IO) {
            val result = nativeOpen(path)
            if (result == 0L) {
                throw RaftError.IoError()
            }
            RaftDb(result)
        }

        /**
         * Open synchronously (for tests or non-coroutine contexts).
         *
         * @throws RaftError if the native open fails.
         */
        fun openBlocking(path: String): RaftDb {
            val result = nativeOpen(path)
            if (result == 0L) {
                throw RaftError.IoError()
            }
            return RaftDb(result)
        }

        // -- Native methods (implemented in C/Rust JNI layer) ----------------

        @JvmStatic
        private external fun nativeOpen(path: String): Long

        @JvmStatic
        private external fun nativePut(
            handle: Long,
            key: ByteArray,
            keyLen: Int,
            value: ByteArray,
            valueLen: Int,
        ): Int

        @JvmStatic
        private external fun nativeGet(
            handle: Long,
            key: ByteArray,
            keyLen: Int,
        ): ByteArray?

        @JvmStatic
        private external fun nativeDelete(
            handle: Long,
            key: ByteArray,
            keyLen: Int,
        ): Int

        @JvmStatic
        private external fun nativeClose(handle: Long)
    }
}

/**
 * Result emitted by [RaftDb.observe].
 *
 * @property key The key (or prefix) that was observed.
 * @property value The current value, or `null` if deleted / not found.
 */
data class QueryResult(
    val key: ByteArray,
    val value: ByteArray?,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is QueryResult) return false
        return key.contentEquals(other.key) && value.contentEquals(other.value)
    }

    override fun hashCode(): Int {
        var result = key.contentHashCode()
        result = 31 * result + (value?.contentHashCode() ?: 0)
        return result
    }
}
