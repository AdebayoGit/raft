package com.raft

import com.margelo.nitro.core.Promise
import com.margelo.nitro.raft.HybridRaftSpec
import com.margelo.nitro.raft.QueryResult
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class HybridRaft : HybridRaftSpec() {

    private var dbHandle: Long = 0L
    private val observers = ConcurrentHashMap<String, ObserverEntry>()

    private data class ObserverEntry(
        val query: String,
        val callback: (QueryResult) -> Unit,
    )

    companion object {
        init {
            System.loadLibrary("raftdb")
        }

        @JvmStatic
        private external fun nativeOpen(path: String): Long

        @JvmStatic
        private external fun nativePut(handle: Long, key: ByteArray, keyLen: Int, value: ByteArray, valueLen: Int): Int

        @JvmStatic
        private external fun nativeGet(handle: Long, key: ByteArray, keyLen: Int): ByteArray?

        @JvmStatic
        private external fun nativeDelete(handle: Long, key: ByteArray, keyLen: Int): Int

        @JvmStatic
        private external fun nativeClose(handle: Long)
    }

    // -- Lifecycle -----------------------------------------------------------

    override fun open(path: String) {
        if (dbHandle != 0L) {
            throw IllegalStateException("Database is already open")
        }
        val handle = nativeOpen(path)
        if (handle == 0L) {
            throw RuntimeException("Failed to open database at: $path")
        }
        dbHandle = handle
    }

    override fun close() {
        if (dbHandle != 0L) {
            observers.clear()
            nativeClose(dbHandle)
            dbHandle = 0L
        }
    }

    // -- CRUD ----------------------------------------------------------------

    override fun put(key: String, value: String): Promise<Unit> {
        return Promise.async {
            ensureOpen()
            val keyBytes = key.toByteArray(Charsets.UTF_8)
            val valueBytes = value.toByteArray(Charsets.UTF_8)
            val code = nativePut(dbHandle, keyBytes, keyBytes.size, valueBytes, valueBytes.size)
            if (code != 0) {
                throw RuntimeException("rft_put failed with code $code")
            }
            notifyObservers(key, value)
        }
    }

    override fun get(key: String): Promise<String?> {
        return Promise.async {
            ensureOpen()
            val keyBytes = key.toByteArray(Charsets.UTF_8)
            val result = nativeGet(dbHandle, keyBytes, keyBytes.size)
            result?.toString(Charsets.UTF_8)
        }
    }

    override fun delete(key: String): Promise<String?> {
        return Promise.async {
            ensureOpen()
            val keyBytes = key.toByteArray(Charsets.UTF_8)
            // Read current value before deleting
            val previous = nativeGet(dbHandle, keyBytes, keyBytes.size)?.toString(Charsets.UTF_8)
            val code = nativeDelete(dbHandle, keyBytes, keyBytes.size)
            if (code != 0) {
                throw RuntimeException("rft_delete failed with code $code")
            }
            notifyObservers(key, null)
            previous
        }
    }

    // -- Live Queries --------------------------------------------------------

    override fun watch(query: String, callback: (result: QueryResult) -> Unit): String {
        ensureOpen()
        val subscriptionId = UUID.randomUUID().toString()
        observers[subscriptionId] = ObserverEntry(query, callback)

        // Emit initial snapshot
        val keyBytes = query.toByteArray(Charsets.UTF_8)
        val current = nativeGet(dbHandle, keyBytes, keyBytes.size)?.toString(Charsets.UTF_8)
        callback(QueryResult(query, current))

        return subscriptionId
    }

    override fun unwatch(subscriptionId: String) {
        observers.remove(subscriptionId)
    }

    // -- Internal ------------------------------------------------------------

    private fun ensureOpen() {
        if (dbHandle == 0L) {
            throw IllegalStateException("Database is not open")
        }
    }

    private fun notifyObservers(key: String, value: String?) {
        for ((_, entry) in observers) {
            if (key.startsWith(entry.query)) {
                entry.callback(QueryResult(key, value))
            }
        }
    }

    protected fun finalize() {
        close()
    }
}
