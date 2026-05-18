package com.raftdb

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.withContext
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Kotlin wrapper around the native `libraftdb.so` C API.
 *
 * Two complementary surfaces are exposed:
 *
 * 1. **Raw KV** — [put]/[get]/[delete]/[observe] take `ByteArray` keys
 *    and values. The store is byte-addressed; callers manage their
 *    own key namespacing.
 *
 * 2. **Typed collections** — [collectionPut]/[collectionPutAuto]/
 *    [collectionGet]/[collectionDelete]/[collectionCount]/
 *    [collectionListIds] address documents by `Long` (uint64) id in
 *    Raft's typed document store. [executeQuery] / [observeCollection] /
 *    [observeQuery] / [beginTransaction] / [withTransaction] complete
 *    the typed surface.
 *
 * The two surfaces address different storage namespaces. Pick one per
 * collection.
 *
 * All blocking JNI calls are dispatched on [Dispatchers.IO].
 *
 * ```kotlin
 * val db = RaftDb.open("/data/data/com.example/files/my.db")
 *
 * // Raw KV
 * db.put("user:1".toByteArray(), json.toByteArray())
 *
 * // Typed
 * val id = db.collectionPutAuto("users", userJson)
 * db.observeCollection("users").collect { event -> /* MutationEvent */ }
 *
 * db.close()
 * ```
 */
class RaftDb private constructor(private val handle: Long) : AutoCloseable {

    private val closed = AtomicBoolean(false)

    // ── Raw KV ─────────────────────────────────────────────────────────

    /**
     * Insert or update a raw-KV pair.
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
     * Look up a raw-KV key.
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
     * Delete a raw-KV key. Deleting a non-existent key is not an error.
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
     * Emits the current snapshot once, then completes when the
     * collector is cancelled. Backed by raw `rft_get` — this stub
     * predates the proper callback-based observer and remains for
     * back-compat. Use [observeCollection] for typed-collection
     * notifications.
     */
    fun observe(prefix: ByteArray): Flow<QueryResult> = callbackFlow {
        ensureOpen()
        val initial = withContext(Dispatchers.IO) {
            nativeGet(handle, prefix, prefix.size)
        }
        send(QueryResult(prefix, initial))
        awaitClose { /* nothing to unregister for this stub */ }
    }

    // ── Typed Collections ──────────────────────────────────────────────

    /**
     * Insert or update a document in `collection`. The document JSON's
     * `id` field is honoured.
     */
    suspend fun collectionPut(collection: String, documentJson: ByteArray) {
        ensureOpen()
        withContext(Dispatchers.IO) {
            val code = nativeCollectionPut(handle, collection, documentJson)
            RaftError.check(code)
        }
    }

    /**
     * Insert a document into `collection`, letting the engine assign a
     * fresh document id. Returns the assigned id.
     */
    suspend fun collectionPutAuto(collection: String, documentJson: ByteArray): Long {
        ensureOpen()
        return withContext(Dispatchers.IO) {
            val outCode = IntArray(1)
            val docId = nativeCollectionPutAuto(handle, collection, documentJson, outCode)
            RaftError.check(outCode[0])
            docId
        }
    }

    /**
     * Fetch a document by id from `collection`. Returns its raw JSON
     * bytes, or `null` if no document with that id exists.
     */
    suspend fun collectionGet(collection: String, docId: Long): ByteArray? {
        ensureOpen()
        return withContext(Dispatchers.IO) {
            val outCode = IntArray(1)
            val bytes = nativeCollectionGet(handle, collection, docId, outCode)
            RaftError.check(outCode[0])
            bytes
        }
    }

    /**
     * Delete a document by id from `collection`. Not an error if the
     * id does not exist.
     */
    suspend fun collectionDelete(collection: String, docId: Long) {
        ensureOpen()
        withContext(Dispatchers.IO) {
            val code = nativeCollectionDelete(handle, collection, docId)
            RaftError.check(code)
        }
    }

    /**
     * Number of documents currently in `collection` (typed namespace).
     */
    suspend fun collectionCount(collection: String): Long {
        ensureOpen()
        return withContext(Dispatchers.IO) {
            val outCode = IntArray(1)
            val count = nativeCollectionCount(handle, collection, outCode)
            RaftError.check(outCode[0])
            count
        }
    }

    /**
     * All document ids currently in `collection` (typed namespace),
     * sorted ascending.
     */
    suspend fun collectionListIds(collection: String): LongArray {
        ensureOpen()
        return withContext(Dispatchers.IO) {
            val outCode = IntArray(1)
            val ids = nativeCollectionListIds(handle, collection, outCode)
            RaftError.check(outCode[0])
            ids ?: LongArray(0)
        }
    }

    // ── Queries ────────────────────────────────────────────────────────

    /**
     * Execute a predicate query (JSON-encoded) and return each
     * matching document as raw JSON bytes.
     */
    suspend fun executeQuery(queryJson: ByteArray): List<ByteArray> {
        ensureOpen()
        return withContext(Dispatchers.IO) {
            val outCode = IntArray(1)
            val docs = nativeQueryExecute(handle, queryJson, outCode)
            RaftError.check(outCode[0])
            docs?.toList() ?: emptyList()
        }
    }

    // ── Observation ────────────────────────────────────────────────────

    /**
     * Observe every insert / update / delete on `collection`. Emits a
     * [MutationEvent] per change.
     *
     * The Flow completes when the collector is cancelled. The native
     * subscription is cleaned up automatically.
     */
    fun observeCollection(collection: String): Flow<MutationEvent> = callbackFlow {
        ensureOpen()
        val callback = RaftObserverCallback { json ->
            try {
                trySend(MutationEvent.fromJson(json))
            } catch (e: Throwable) {
                // Swallow parse errors so a malformed event doesn't kill the stream.
            }
        }
        val outCode = IntArray(1)
        val pair = nativeObserveCollection(handle, collection, callback, outCode)
        if (pair == null || outCode[0] != 0) {
            close(RaftError.fromCode(outCode[0]) ?: RaftError.IoError())
            return@callbackFlow
        }
        val subId = pair[0]
        val ctxAddr = pair[1]
        awaitClose {
            nativeUnobserve(handle, subId, ctxAddr)
        }
    }

    /**
     * Observe a live query. Emits a [QueryDiff] immediately with the
     * initial snapshot, then again every time the result set changes.
     */
    fun observeQuery(queryJson: ByteArray): Flow<QueryDiff> = callbackFlow {
        ensureOpen()
        val callback = RaftObserverCallback { json ->
            try {
                trySend(QueryDiff.fromJson(json))
            } catch (e: Throwable) {
                // Ignore malformed events.
            }
        }
        val outCode = IntArray(1)
        val pair = nativeObserveQueryHandle(handle, queryJson, callback, outCode)
        if (pair == null || outCode[0] != 0) {
            close(RaftError.fromCode(outCode[0]) ?: RaftError.IoError())
            return@callbackFlow
        }
        val subId = pair[0]
        val ctxAddr = pair[1]
        awaitClose {
            nativeUnobserve(handle, subId, ctxAddr)
        }
    }

    // ── Transactions ───────────────────────────────────────────────────

    /**
     * Begin a new transaction. The caller takes ownership of the
     * returned [RaftTransaction] and must end it with `commit()` or
     * `rollback()`.
     */
    suspend fun beginTransaction(): RaftTransaction {
        ensureOpen()
        return withContext(Dispatchers.IO) {
            val outCode = IntArray(1)
            val txnHandle = nativeTransactionBegin(handle, outCode)
            RaftError.check(outCode[0])
            if (txnHandle == 0L) throw RaftError.InvalidHandle()
            RaftTransaction(txnHandle)
        }
    }

    /**
     * Run [block] inside a transaction. If it returns normally, the
     * transaction is committed; if it throws, it is rolled back and
     * the error is rethrown.
     */
    suspend fun <T> withTransaction(block: suspend (RaftTransaction) -> T): T {
        val txn = beginTransaction()
        try {
            val result = block(txn)
            txn.commit()
            return result
        } catch (e: Throwable) {
            txn.rollback()
            throw e
        }
    }

    // ── Lifecycle ──────────────────────────────────────────────────────

    /**
     * Close the database and release the native handle. Safe to call
     * multiple times; subsequent calls are no-ops.
     */
    override fun close() {
        if (closed.compareAndSet(false, true)) {
            nativeClose(handle)
        }
    }

    private fun ensureOpen() {
        check(!closed.get()) { "RaftDb is already closed" }
    }

    // ── JNI declarations ───────────────────────────────────────────────

    companion object {

        init {
            // `libraftdb.so` is the Rust core; `libraftdb-jni.so` is the
            // JNI shim built from `src/main/cpp` and depends on the core
            // (auto-loaded by the dynamic linker, but loading explicitly
            // makes the dependency unambiguous).
            System.loadLibrary("raftdb")
            System.loadLibrary("raftdb-jni")
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

        // ── Native methods (implemented in libraftdb-jni.so) ──────────

        @JvmStatic
        private external fun nativeOpen(path: String): Long

        @JvmStatic
        private external fun nativeClose(handle: Long)

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

        // Typed collections
        @JvmStatic
        private external fun nativeCollectionPut(
            handle: Long,
            collection: String,
            documentJson: ByteArray,
        ): Int

        @JvmStatic
        private external fun nativeCollectionPutAuto(
            handle: Long,
            collection: String,
            documentJson: ByteArray,
            outCode: IntArray,
        ): Long

        @JvmStatic
        private external fun nativeCollectionGet(
            handle: Long,
            collection: String,
            docId: Long,
            outCode: IntArray,
        ): ByteArray?

        @JvmStatic
        private external fun nativeCollectionDelete(
            handle: Long,
            collection: String,
            docId: Long,
        ): Int

        @JvmStatic
        private external fun nativeCollectionCount(
            handle: Long,
            collection: String,
            outCode: IntArray,
        ): Long

        @JvmStatic
        private external fun nativeCollectionListIds(
            handle: Long,
            collection: String,
            outCode: IntArray,
        ): LongArray?

        // Queries
        @JvmStatic
        private external fun nativeQueryExecute(
            handle: Long,
            queryJson: ByteArray,
            outCode: IntArray,
        ): Array<ByteArray>?

        // Transactions
        @JvmStatic
        internal external fun nativeTransactionBegin(
            handle: Long,
            outCode: IntArray,
        ): Long

        @JvmStatic
        internal external fun nativeTransactionGet(
            txnHandle: Long,
            collection: String,
            docId: Long,
            outCode: IntArray,
        ): ByteArray?

        @JvmStatic
        internal external fun nativeTransactionPut(
            txnHandle: Long,
            collection: String,
            documentJson: ByteArray,
        ): Int

        @JvmStatic
        internal external fun nativeTransactionDelete(
            txnHandle: Long,
            collection: String,
            docId: Long,
        ): Int

        @JvmStatic
        internal external fun nativeTransactionCommit(txnHandle: Long): Int

        @JvmStatic
        internal external fun nativeTransactionRollback(txnHandle: Long)

        // Observation
        @JvmStatic
        private external fun nativeObserveCollection(
            handle: Long,
            collection: String,
            callback: RaftObserverCallback,
            outCode: IntArray,
        ): LongArray?

        @JvmStatic
        private external fun nativeObserveQueryHandle(
            handle: Long,
            queryJson: ByteArray,
            callback: RaftObserverCallback,
            outCode: IntArray,
        ): LongArray?

        @JvmStatic
        private external fun nativeUnobserve(
            handle: Long,
            subId: Long,
            ctxAddr: Long,
        ): Int
    }
}

/**
 * Callback invoked by the JNI shim with a JSON-encoded event payload.
 * Implemented in `RaftDb.observeCollection` / `RaftDb.observeQuery`
 * to feed the returned [Flow].
 */
internal fun interface RaftObserverCallback {
    fun onEvent(eventJson: String)
}

/**
 * Legacy result type returned by [RaftDb.observe] (the raw-KV stub
 * observer). For typed-collection notifications, use [MutationEvent].
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
