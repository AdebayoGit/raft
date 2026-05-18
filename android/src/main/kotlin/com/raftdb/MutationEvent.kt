package com.raftdb

import org.json.JSONObject

/**
 * What kind of mutation occurred.
 */
enum class MutationKind {
    INSERT,
    UPDATE,
    DELETE,
}

/**
 * Whether the mutation originated locally or arrived from a network peer.
 */
enum class MutationOrigin {
    LOCAL,
    REMOTE,
}

/**
 * A mutation notification emitted by [RaftDb.observeCollection].
 *
 * The Rust core emits these as JSON over the FFI; Kotlin parses them
 * into this class before emitting on the Flow.
 *
 * @property collection the collection that was mutated
 * @property docId      document id within the collection
 * @property mutationType the kind of change
 * @property origin     whether local or remote
 */
data class MutationEvent(
    val collection: String,
    val docId: Long,
    val mutationType: MutationKind,
    val origin: MutationOrigin,
) {
    companion object {
        /**
         * Parse a JSON payload from `rft_observe`. Throws if required
         * fields are missing or have unexpected values.
         */
        fun fromJson(json: String): MutationEvent {
            val obj = JSONObject(json)
            val kind = when (val t = obj.optString("mutation_type")) {
                "Insert" -> MutationKind.INSERT
                "Update" -> MutationKind.UPDATE
                "Delete" -> MutationKind.DELETE
                else -> throw IllegalArgumentException("Unknown mutation_type: $t")
            }
            val origin = if (obj.optString("origin") == "Remote") {
                MutationOrigin.REMOTE
            } else {
                MutationOrigin.LOCAL
            }
            return MutationEvent(
                collection = obj.getString("collection"),
                docId = obj.getLong("doc_id"),
                mutationType = kind,
                origin = origin,
            )
        }
    }
}
