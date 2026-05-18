package com.raft

import com.margelo.nitro.core.Promise
import com.margelo.nitro.raft.HybridRaftSpec
import com.margelo.nitro.raft.QueryResult
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class HybridRaft : HybridRaftSpec() {

    private var dbHandle: Long = 0L

    // Legacy raw-KV observers (string-prefix matching, all in Kotlin).
    private val kvObservers = ConcurrentHashMap<String, KvObserverEntry>()

    // Typed-FFI observers. Each entry stores the native subscription id +
    // the ObserverContext pointer (returned by the JNI shim), so unwatch
    // can free both.
    private val typedObservers = ConcurrentHashMap<String, TypedObserverEntry>()

    private data class KvObserverEntry(
        val query: String,
        val callback: (QueryResult) -> Unit,
    )

    private data class TypedObserverEntry(
        val subId: Long,
        val ctxAddr: Long,
        // Held to prevent the JS callback from being GC'd while
        // the native side still has a reference.
        val pinnedCallback: RaftCallback,
    )

    companion object {
        init {
            System.loadLibrary("raftdb")
        }

        // Raw KV
        @JvmStatic private external fun nativeOpen(path: String): Long
        @JvmStatic private external fun nativePut(handle: Long, key: ByteArray, keyLen: Int, value: ByteArray, valueLen: Int): Int
        @JvmStatic private external fun nativeGet(handle: Long, key: ByteArray, keyLen: Int): ByteArray?
        @JvmStatic private external fun nativeDelete(handle: Long, key: ByteArray, keyLen: Int): Int
        @JvmStatic private external fun nativeClose(handle: Long)

        // Typed collections
        @JvmStatic private external fun nativeCollectionPut(handle: Long, collection: String, documentJson: ByteArray): Int
        @JvmStatic private external fun nativeCollectionPutAuto(handle: Long, collection: String, documentJson: ByteArray, outCode: IntArray): Long
        @JvmStatic private external fun nativeCollectionGet(handle: Long, collection: String, docId: Long, outCode: IntArray): ByteArray?
        @JvmStatic private external fun nativeCollectionDelete(handle: Long, collection: String, docId: Long): Int
        @JvmStatic private external fun nativeCollectionCount(handle: Long, collection: String, outCode: IntArray): Long
        @JvmStatic private external fun nativeCollectionListIds(handle: Long, collection: String, outCode: IntArray): LongArray?

        // Queries
        @JvmStatic private external fun nativeQueryExecute(handle: Long, queryJson: ByteArray, outCode: IntArray): Array<ByteArray>?

        // Transactions
        @JvmStatic private external fun nativeTransactionBegin(handle: Long, outCode: IntArray): Long
        @JvmStatic private external fun nativeTransactionGet(txnHandle: Long, collection: String, docId: Long, outCode: IntArray): ByteArray?
        @JvmStatic private external fun nativeTransactionPut(txnHandle: Long, collection: String, documentJson: ByteArray): Int
        @JvmStatic private external fun nativeTransactionDelete(txnHandle: Long, collection: String, docId: Long): Int
        @JvmStatic private external fun nativeTransactionCommit(txnHandle: Long): Int
        @JvmStatic private external fun nativeTransactionRollback(txnHandle: Long)

        // Observation
        @JvmStatic private external fun nativeObserveCollection(handle: Long, collection: String, callback: RaftCallback, outCode: IntArray): LongArray?
        @JvmStatic private external fun nativeObserveQueryHandle(handle: Long, queryJson: ByteArray, callback: RaftCallback, outCode: IntArray): LongArray?
        @JvmStatic private external fun nativeUnobserve(handle: Long, subId: Long, ctxAddr: Long): Int
    }

    // -- Lifecycle ------------------------------------------------------

    override fun open(path: String) {
        if (dbHandle != 0L) throw IllegalStateException("Database is already open")
        val handle = nativeOpen(path)
        if (handle == 0L) throw RuntimeException("Failed to open database at: $path")
        dbHandle = handle
    }

    override fun close() {
        if (dbHandle != 0L) {
            kvObservers.clear()
            // Native subscriptions are cancelled by rft_close; explicit
            // unobserve isn't strictly required but it keeps the JNI
            // side's bookkeeping clean.
            for ((_, entry) in typedObservers) {
                nativeUnobserve(dbHandle, entry.subId, entry.ctxAddr)
            }
            typedObservers.clear()
            nativeClose(dbHandle)
            dbHandle = 0L
        }
    }

    // -- Raw KV ---------------------------------------------------------

    override fun put(key: String, value: String): Promise<Unit> = Promise.async {
        ensureOpen()
        val keyBytes = key.toByteArray(Charsets.UTF_8)
        val valueBytes = value.toByteArray(Charsets.UTF_8)
        val code = nativePut(dbHandle, keyBytes, keyBytes.size, valueBytes, valueBytes.size)
        if (code != 0) throw RuntimeException("rft_put failed with code $code")
        notifyKvObservers(key, value)
    }

    override fun get(key: String): Promise<String?> = Promise.async {
        ensureOpen()
        val keyBytes = key.toByteArray(Charsets.UTF_8)
        nativeGet(dbHandle, keyBytes, keyBytes.size)?.toString(Charsets.UTF_8)
    }

    override fun delete(key: String): Promise<String?> = Promise.async {
        ensureOpen()
        val keyBytes = key.toByteArray(Charsets.UTF_8)
        val previous = nativeGet(dbHandle, keyBytes, keyBytes.size)?.toString(Charsets.UTF_8)
        val code = nativeDelete(dbHandle, keyBytes, keyBytes.size)
        if (code != 0) throw RuntimeException("rft_delete failed with code $code")
        notifyKvObservers(key, null)
        previous
    }

    // -- Typed Collections ---------------------------------------------

    override fun collectionPut(collection: String, documentJson: String): Promise<Unit> = Promise.async {
        ensureOpen()
        val code = nativeCollectionPut(dbHandle, collection, documentJson.toByteArray(Charsets.UTF_8))
        if (code != 0) throw RuntimeException("rft_collection_put failed with code $code")
    }

    override fun collectionPutAuto(collection: String, documentJson: String): Promise<Double> = Promise.async {
        ensureOpen()
        val outCode = IntArray(1)
        val id = nativeCollectionPutAuto(dbHandle, collection,
            documentJson.toByteArray(Charsets.UTF_8), outCode)
        if (outCode[0] != 0) throw RuntimeException("rft_collection_put_auto failed with code ${outCode[0]}")
        id.toDouble()
    }

    override fun collectionGet(collection: String, docId: Double): Promise<String?> = Promise.async {
        ensureOpen()
        val outCode = IntArray(1)
        val bytes = nativeCollectionGet(dbHandle, collection, docId.toLong(), outCode)
        if (outCode[0] != 0) throw RuntimeException("rft_collection_get failed with code ${outCode[0]}")
        bytes?.toString(Charsets.UTF_8)
    }

    override fun collectionDelete(collection: String, docId: Double): Promise<Unit> = Promise.async {
        ensureOpen()
        val code = nativeCollectionDelete(dbHandle, collection, docId.toLong())
        if (code != 0) throw RuntimeException("rft_collection_delete failed with code $code")
    }

    override fun collectionCount(collection: String): Promise<Double> = Promise.async {
        ensureOpen()
        val outCode = IntArray(1)
        val count = nativeCollectionCount(dbHandle, collection, outCode)
        if (outCode[0] != 0) throw RuntimeException("rft_collection_count failed with code ${outCode[0]}")
        count.toDouble()
    }

    override fun collectionListIds(collection: String): Promise<Array<Double>> = Promise.async {
        ensureOpen()
        val outCode = IntArray(1)
        val ids = nativeCollectionListIds(dbHandle, collection, outCode)
        if (outCode[0] != 0) throw RuntimeException("rft_collection_list_ids failed with code ${outCode[0]}")
        (ids ?: LongArray(0)).map { it.toDouble() }.toTypedArray()
    }

    // -- Queries --------------------------------------------------------

    override fun executeQuery(queryJson: String): Promise<Array<String>> = Promise.async {
        ensureOpen()
        val outCode = IntArray(1)
        val docs = nativeQueryExecute(dbHandle, queryJson.toByteArray(Charsets.UTF_8), outCode)
        if (outCode[0] != 0) throw RuntimeException("rft_query_execute failed with code ${outCode[0]}")
        (docs ?: emptyArray()).map { it.toString(Charsets.UTF_8) }.toTypedArray()
    }

    // -- Transactions ---------------------------------------------------

    override fun transactionBegin(): Promise<Double> = Promise.async {
        ensureOpen()
        val outCode = IntArray(1)
        val txnHandle = nativeTransactionBegin(dbHandle, outCode)
        if (outCode[0] != 0) throw RuntimeException("rft_transaction_begin failed with code ${outCode[0]}")
        if (txnHandle == 0L) throw RuntimeException("rft_transaction_begin returned null handle")
        txnHandle.toDouble()
    }

    override fun transactionGet(txnHandle: Double, collection: String, docId: Double): Promise<String?> = Promise.async {
        val outCode = IntArray(1)
        val bytes = nativeTransactionGet(txnHandle.toLong(), collection, docId.toLong(), outCode)
        if (outCode[0] != 0) throw RuntimeException("rft_transaction_get failed with code ${outCode[0]}")
        bytes?.toString(Charsets.UTF_8)
    }

    override fun transactionPut(txnHandle: Double, collection: String, documentJson: String): Promise<Unit> = Promise.async {
        val code = nativeTransactionPut(txnHandle.toLong(), collection, documentJson.toByteArray(Charsets.UTF_8))
        if (code != 0) throw RuntimeException("rft_transaction_put failed with code $code")
    }

    override fun transactionDelete(txnHandle: Double, collection: String, docId: Double): Promise<Unit> = Promise.async {
        val code = nativeTransactionDelete(txnHandle.toLong(), collection, docId.toLong())
        if (code != 0) throw RuntimeException("rft_transaction_delete failed with code $code")
    }

    override fun transactionCommit(txnHandle: Double): Promise<Unit> = Promise.async {
        val code = nativeTransactionCommit(txnHandle.toLong())
        if (code != 0) throw RuntimeException("rft_transaction_commit failed with code $code")
    }

    override fun transactionRollback(txnHandle: Double): Promise<Unit> = Promise.async {
        nativeTransactionRollback(txnHandle.toLong())
    }

    // -- Observation ----------------------------------------------------

    override fun watch(query: String, callback: (QueryResult) -> Unit): String {
        ensureOpen()
        val subscriptionId = UUID.randomUUID().toString()
        kvObservers[subscriptionId] = KvObserverEntry(query, callback)
        val keyBytes = query.toByteArray(Charsets.UTF_8)
        val current = nativeGet(dbHandle, keyBytes, keyBytes.size)?.toString(Charsets.UTF_8)
        callback(QueryResult(query, current))
        return subscriptionId
    }

    override fun observeCollection(collection: String, callback: (String) -> Unit): String {
        ensureOpen()
        val subscriptionId = UUID.randomUUID().toString()
        val raftCallback = RaftCallback { json -> callback(json) }
        val outCode = IntArray(1)
        val pair = nativeObserveCollection(dbHandle, collection, raftCallback, outCode)
        if (outCode[0] != 0 || pair == null || pair.size < 2) {
            throw RuntimeException("rft_observe failed with code ${outCode[0]}")
        }
        typedObservers[subscriptionId] =
            TypedObserverEntry(subId = pair[0], ctxAddr = pair[1], pinnedCallback = raftCallback)
        return subscriptionId
    }

    override fun observeQuery(queryJson: String, callback: (String) -> Unit): String {
        ensureOpen()
        val subscriptionId = UUID.randomUUID().toString()
        val raftCallback = RaftCallback { json -> callback(json) }
        val outCode = IntArray(1)
        val pair = nativeObserveQueryHandle(
            dbHandle, queryJson.toByteArray(Charsets.UTF_8), raftCallback, outCode)
        if (outCode[0] != 0 || pair == null || pair.size < 2) {
            throw RuntimeException("rft_observe_query failed with code ${outCode[0]}")
        }
        typedObservers[subscriptionId] =
            TypedObserverEntry(subId = pair[0], ctxAddr = pair[1], pinnedCallback = raftCallback)
        return subscriptionId
    }

    override fun unwatch(subscriptionId: String) {
        kvObservers.remove(subscriptionId)
        val typed = typedObservers.remove(subscriptionId)
        if (typed != null && dbHandle != 0L) {
            nativeUnobserve(dbHandle, typed.subId, typed.ctxAddr)
        }
    }

    // -- Internal -------------------------------------------------------

    private fun ensureOpen() {
        if (dbHandle == 0L) throw IllegalStateException("Database is not open")
    }

    private fun notifyKvObservers(key: String, value: String?) {
        val snapshot = kvObservers.values.toList()
        for (entry in snapshot) {
            if (key.startsWith(entry.query)) {
                entry.callback(QueryResult(key, value))
            }
        }
    }

    protected fun finalize() {
        close()
    }
}

/**
 * Callback invoked by the JNI shim with a JSON-encoded event payload.
 * Routed by `nativeObserveCollection` / `nativeObserveQueryHandle` to
 * the JS callback the developer provided.
 */
fun interface RaftCallback {
    fun onEvent(eventJson: String)
}
