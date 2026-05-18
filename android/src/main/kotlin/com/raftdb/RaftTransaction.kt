package com.raftdb

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.util.concurrent.atomic.AtomicBoolean

/**
 * An optimistic-concurrency transaction over a [RaftDb].
 *
 * Begin via [RaftDb.beginTransaction]; read and buffer writes; then
 * call [commit] or [rollback]. The handle is consumed by either
 * terminator and must not be reused.
 *
 * At commit time, the engine validates that every document read inside
 * the transaction has the same version it had when read. If any
 * tracked document was modified concurrently, [commit] throws
 * [RaftError.TransactionConflict] and no writes are applied.
 *
 * Use [RaftDb.withTransaction] for the scoped, auto-cleanup form:
 *
 * ```kotlin
 * db.withTransaction { txn ->
 *     val raw = txn.get("users", 42L)
 *     // mutate raw
 *     txn.put("users", mutated)
 * }
 * ```
 */
class RaftTransaction internal constructor(private val handle: Long) {

    private val consumed = AtomicBoolean(false)

    /**
     * Read a document by id. The version is recorded for conflict
     * detection at commit time. Returns `null` if not found (the read
     * is still tracked).
     */
    suspend fun get(collection: String, docId: Long): ByteArray? {
        ensureActive()
        return withContext(Dispatchers.IO) {
            val outCode = IntArray(1)
            val bytes = RaftDb.nativeTransactionGet(handle, collection, docId, outCode)
            RaftError.check(outCode[0])
            bytes
        }
    }

    /**
     * Buffer a write inside the transaction. Applied atomically on commit.
     * The serialized JSON's `id` field must equal the storage doc id.
     */
    suspend fun put(collection: String, documentJson: ByteArray) {
        ensureActive()
        withContext(Dispatchers.IO) {
            val code = RaftDb.nativeTransactionPut(handle, collection, documentJson)
            RaftError.check(code)
        }
    }

    /**
     * Buffer a delete inside the transaction.
     */
    suspend fun delete(collection: String, docId: Long) {
        ensureActive()
        withContext(Dispatchers.IO) {
            val code = RaftDb.nativeTransactionDelete(handle, collection, docId)
            RaftError.check(code)
        }
    }

    /**
     * Validate the read set and atomically apply all buffered writes.
     * Consumes the handle — calling [commit] or [rollback] again is a
     * no-op (and the transaction is then unusable).
     *
     * Throws [RaftError.TransactionConflict] if a tracked document was
     * modified concurrently. No writes are applied in that case.
     */
    suspend fun commit() {
        if (!consumed.compareAndSet(false, true)) {
            throw RaftError.InvalidHandle()
        }
        withContext(Dispatchers.IO) {
            val code = RaftDb.nativeTransactionCommit(handle)
            RaftError.check(code)
        }
    }

    /**
     * Discard the transaction. Consumes the handle. Safe to call after
     * commit (no-op).
     */
    suspend fun rollback() {
        if (!consumed.compareAndSet(false, true)) return
        withContext(Dispatchers.IO) {
            RaftDb.nativeTransactionRollback(handle)
        }
    }

    private fun ensureActive() {
        check(!consumed.get()) {
            "RaftTransaction has already been committed or rolled back"
        }
    }
}
