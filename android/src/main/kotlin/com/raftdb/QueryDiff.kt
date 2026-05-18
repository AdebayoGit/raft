package com.raftdb

import org.json.JSONArray
import org.json.JSONObject

/**
 * The diff between two consecutive live-query result sets.
 *
 * Emitted by [RaftDb.observeQuery]. Each bucket holds raw JSON bytes
 * (UTF-8) for the documents that were added, removed, or updated since
 * the previous tick. Decode each element with your own deserializer.
 *
 * @property added   documents present in the new result set but not the old
 * @property removed documents present in the old result set but not the new
 * @property updated documents present in both but with changed field values
 */
data class QueryDiff(
    val added: List<ByteArray> = emptyList(),
    val removed: List<ByteArray> = emptyList(),
    val updated: List<ByteArray> = emptyList(),
) {
    val isEmpty: Boolean
        get() = added.isEmpty() && removed.isEmpty() && updated.isEmpty()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is QueryDiff) return false
        return bucketsEqual(added, other.added) &&
            bucketsEqual(removed, other.removed) &&
            bucketsEqual(updated, other.updated)
    }

    override fun hashCode(): Int {
        var result = added.fold(1) { acc, b -> 31 * acc + b.contentHashCode() }
        result = 31 * result + removed.fold(1) { acc, b -> 31 * acc + b.contentHashCode() }
        result = 31 * result + updated.fold(1) { acc, b -> 31 * acc + b.contentHashCode() }
        return result
    }

    private fun bucketsEqual(a: List<ByteArray>, b: List<ByteArray>): Boolean {
        if (a.size != b.size) return false
        for (i in a.indices) {
            if (!a[i].contentEquals(b[i])) return false
        }
        return true
    }

    companion object {
        /**
         * Parse a JSON payload from `rft_observe_query`. Each element
         * in `added`/`removed`/`updated` is re-encoded into UTF-8 JSON
         * bytes for the caller.
         */
        fun fromJson(json: String): QueryDiff {
            val obj = JSONObject(json)
            return QueryDiff(
                added = bucket(obj.optJSONArray("added")),
                removed = bucket(obj.optJSONArray("removed")),
                updated = bucket(obj.optJSONArray("updated")),
            )
        }

        private fun bucket(arr: JSONArray?): List<ByteArray> {
            if (arr == null) return emptyList()
            return List(arr.length()) { i ->
                arr.get(i).toString().toByteArray(Charsets.UTF_8)
            }
        }
    }
}
