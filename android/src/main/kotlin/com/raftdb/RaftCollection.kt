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
     * Observe changes to a specific document by [id].
     *
     * Emits the current value immediately, then on every subsequent change.
     */
    fun observe(id: String): Flow<T?> =
        db.observe(scopedKey(id)).map { result ->
            result.value?.let(deserialize)
        }

    /**
     * Observe all changes in this collection.
     *
     * Emits a [QueryResult] for every write/delete under the collection prefix.
     */
    fun observeAll(): Flow<QueryResult> = db.observe(prefix)

    private fun scopedKey(id: String): ByteArray =
        "$name:$id".toByteArray(Charsets.UTF_8)
}
