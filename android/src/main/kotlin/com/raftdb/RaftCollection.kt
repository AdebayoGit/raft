package com.raftdb

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

/**
 * A typed, collection-scoped wrapper around [RaftDb].
 *
 * All keys are automatically prefixed with `<collection>:` so that
 * multiple collections can coexist in the same database without
 * key collisions.
 *
 * @param T the document type stored in this collection.
 * @property db the underlying [RaftDb] instance.
 * @property name the collection name used as key prefix.
 * @property serialize converts a [T] to bytes for storage.
 * @property deserialize converts stored bytes back to [T].
 */
class RaftCollection<T>(
    private val db: RaftDb,
    val name: String,
    private val serialize: (T) -> ByteArray,
    private val deserialize: (ByteArray) -> T,
) {

    private val prefix: ByteArray = "$name:".toByteArray(Charsets.UTF_8)

    /**
     * Insert or update a document by [id].
     *
     * @throws RaftError on native failure.
     */
    suspend fun put(id: String, document: T) {
        db.put(scopedKey(id), serialize(document))
    }

    /**
     * Retrieve a document by [id].
     *
     * @return the deserialized document, or `null` if not found.
     * @throws RaftError on native failure (other than not-found).
     */
    suspend fun get(id: String): T? {
        val bytes = db.get(scopedKey(id)) ?: return null
        return deserialize(bytes)
    }

    /**
     * Delete a document by [id]. Deleting a non-existent id is not an error.
     *
     * @throws RaftError on native failure.
     */
    suspend fun delete(id: String) {
        db.delete(scopedKey(id))
    }

    /**
     * Observe changes to a specific document by raw-KV [id] (legacy).
     *
     * Emits the current value once. The underlying raw-KV observer is
     * a stub; use [observe] (no-arg) for proper typed-FFI notifications.
     */
    fun observe(id: String): Flow<T?> =
        db.observe(scopedKey(id)).map { result ->
            result.value?.let(deserialize)
        }

    /**
     * Observe all changes in this collection via the raw-KV stub (legacy).
     */
    fun observeAll(): Flow<QueryResult> = db.observe(prefix)

    private fun scopedKey(id: String): ByteArray =
        "$name:$id".toByteArray(Charsets.UTF_8)

    // ── Typed-FFI surface (separate storage namespace from String-id) ──

    /**
     * Insert a document into the typed collection store, letting the
     * engine assign a fresh `Long` (uint64) id. Returns the assigned id.
     */
    suspend fun putAuto(document: T): Long =
        db.collectionPutAuto(name, serialize(document))

    /**
     * Insert or update a document at the given `Long` id. The
     * serialized document's `id` field must equal [docId].
     */
    suspend fun putById(docId: Long, document: T) {
        db.collectionPut(name, serialize(document))
    }

    /**
     * Retrieve a document by `Long` id from the typed namespace.
     * Returns `null` if not found.
     */
    suspend fun getById(docId: Long): T? {
        val bytes = db.collectionGet(name, docId) ?: return null
        return deserialize(bytes)
    }

    /**
     * Delete a document by `Long` id. Not an error if the id does
     * not exist.
     */
    suspend fun deleteById(docId: Long) {
        db.collectionDelete(name, docId)
    }

    /**
     * Number of documents in this collection (typed namespace).
     */
    suspend fun count(): Long = db.collectionCount(name)

    /**
     * All document ids in this collection (typed namespace),
     * sorted ascending.
     */
    suspend fun listIds(): LongArray = db.collectionListIds(name)

    /**
     * Observe every insert / update / delete on this collection
     * (typed namespace). Emits a [MutationEvent] per change.
     */
    fun observe(): Flow<MutationEvent> = db.observeCollection(name)

    /**
     * Observe a live query against this collection. Emits a
     * [QueryDiff] immediately with the initial snapshot, then again
     * every time the result set changes.
     */
    fun liveQuery(queryJson: ByteArray): Flow<QueryDiff> = db.observeQuery(queryJson)
}
