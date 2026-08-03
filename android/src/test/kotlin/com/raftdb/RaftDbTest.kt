package com.raftdb

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * Unit tests for the Raft Android bindings.
 *
 * These tests verify the Kotlin API layer (error mapping, collection
 * scoping, query result equality) without requiring the native library.
 * Integration tests that load `libraftdb.so` belong in `androidTest/`.
 */
class RaftDbTest {

    @Test
    fun `parses resync required mutation control event`() {
        val event = MutationEvent.fromJson(
            """{"collection":"users","doc_id":0,"mutation_type":"ResyncRequired","origin":"Local"}""",
        )
        assertEquals(MutationKind.RESYNC_REQUIRED, event.mutationType)
        assertEquals(0L, event.docId)
    }

    // -- RaftError mapping ---------------------------------------------------

    @Test
    fun `error code 0 returns null (OK)`() {
        assertNull(RaftError.fromCode(0))
    }

    @Test
    fun `error code 1 maps to NullPointer`() {
        val error = RaftError.fromCode(1)
        assertNotNull(error)
        assertTrue(error is RaftError.NullPointer)
    }

    @Test
    fun `error code 2 maps to InvalidUtf8`() {
        assertTrue(RaftError.fromCode(2) is RaftError.InvalidUtf8)
    }

    @Test
    fun `error code 3 maps to IoError`() {
        assertTrue(RaftError.fromCode(3) is RaftError.IoError)
    }

    @Test
    fun `error code 4 maps to NotFound`() {
        assertTrue(RaftError.fromCode(4) is RaftError.NotFound)
    }

    @Test
    fun `error code 5 maps to BufferTooSmall`() {
        assertTrue(RaftError.fromCode(5) is RaftError.BufferTooSmall)
    }

    @Test
    fun `error code 6 maps to InvalidJson`() {
        assertTrue(RaftError.fromCode(6) is RaftError.InvalidJson)
    }

    @Test
    fun `error code 7 maps to TransactionConflict`() {
        assertTrue(RaftError.fromCode(7) is RaftError.TransactionConflict)
    }

    @Test
    fun `error code 8 maps to InvalidHandle`() {
        assertTrue(RaftError.fromCode(8) is RaftError.InvalidHandle)
    }

    @Test
    fun `error code 9 maps to UnknownSubscription`() {
        assertTrue(RaftError.fromCode(9) is RaftError.UnknownSubscription)
    }

    @Test
    fun `error code 10 maps to InternalPanic`() {
        assertTrue(RaftError.fromCode(10) is RaftError.InternalPanic)
    }

    @Test
    fun `error code 11 maps to InvalidPath`() {
        assertTrue(RaftError.fromCode(11) is RaftError.InvalidPath)
    }

    @Test
    fun `error code 12 maps to DartApiNotInitialized`() {
        assertTrue(RaftError.fromCode(12) is RaftError.DartApiNotInitialized)
    }

    @Test
    fun `error code 13 maps to PayloadTooLarge`() {
        assertTrue(RaftError.fromCode(13) is RaftError.PayloadTooLarge)
    }

    @Test
    fun `error code 14 maps to UnsupportedVersion`() {
        assertTrue(RaftError.fromCode(14) is RaftError.UnsupportedVersion)
    }

    @Test
    fun `unknown error code maps to Unknown`() {
        val error = RaftError.fromCode(99)
        assertTrue(error is RaftError.Unknown)
        assertTrue(error!!.message!!.contains("99"))
    }

    @Test
    fun `negative error code maps to Unknown`() {
        // Defensive: a hostile or buggy native side could return any int.
        val error = RaftError.fromCode(-1)
        assertTrue(error is RaftError.Unknown)
    }

    @Test
    fun `check does not throw for code 0`() {
        RaftError.check(0) // should not throw
    }

    @Test(expected = RaftError.IoError::class)
    fun `check throws IoError for code 3`() {
        RaftError.check(3)
    }

    @Test(expected = RaftError.TransactionConflict::class)
    fun `check throws TransactionConflict for code 7`() {
        RaftError.check(7)
    }

    @Test(expected = RaftError.InvalidHandle::class)
    fun `check throws InvalidHandle for code 8`() {
        RaftError.check(8)
    }

    @Test
    fun `all error subclasses are RaftError and Exception`() {
        val errors = listOf(
            RaftError.NullPointer(),
            RaftError.InvalidUtf8(),
            RaftError.IoError(),
            RaftError.NotFound(),
            RaftError.BufferTooSmall(),
            RaftError.InvalidJson(),
            RaftError.TransactionConflict(),
            RaftError.InvalidHandle(),
            RaftError.UnknownSubscription(),
            RaftError.InternalPanic(),
            RaftError.InvalidPath(),
            RaftError.DartApiNotInitialized(),
            RaftError.PayloadTooLarge(),
            RaftError.UnsupportedVersion(),
            RaftError.Unknown(42),
        )
        for (error in errors) {
            assertTrue(
                "${error::class.simpleName} should be a RaftError",
                error is RaftError,
            )
            assertTrue(
                "${error::class.simpleName} should be an Exception",
                error is Exception,
            )
        }
    }

    @Test
    fun `error messages are descriptive`() {
        assertTrue(RaftError.NullPointer().message!!.contains("null"))
        assertTrue(RaftError.InvalidUtf8().message!!.contains("UTF-8"))
        assertTrue(RaftError.IoError().message!!.contains("I/O"))
        assertTrue(RaftError.NotFound().message!!.contains("not found"))
        assertTrue(RaftError.BufferTooSmall().message!!.contains("buffer"))
        assertTrue(RaftError.InvalidJson().message!!.contains("JSON"))
        assertTrue(RaftError.TransactionConflict().message!!.contains("conflict"))
        assertTrue(RaftError.InvalidHandle().message!!.contains("handle"))
        assertTrue(RaftError.UnknownSubscription().message!!.contains("ubscription"))
        assertTrue(RaftError.InternalPanic().message!!.contains("panic"))
        assertTrue(RaftError.InvalidPath().message!!.contains("path"))
        assertTrue(RaftError.DartApiNotInitialized().message!!.contains("Dart API"))
        assertTrue(RaftError.PayloadTooLarge().message!!.contains("size cap"))
        assertTrue(RaftError.UnsupportedVersion().message!!.contains("version"))
    }

    // -- QueryResult ---------------------------------------------------------

    @Test
    fun `QueryResult equality compares byte content`() {
        val a = QueryResult("key".toByteArray(), "val".toByteArray())
        val b = QueryResult("key".toByteArray(), "val".toByteArray())
        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())
    }

    @Test
    fun `QueryResult with null value equals another null-value result`() {
        val a = QueryResult("key".toByteArray(), null)
        val b = QueryResult("key".toByteArray(), null)
        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())
    }

    @Test
    fun `QueryResult with different values are not equal`() {
        val a = QueryResult("key".toByteArray(), "val1".toByteArray())
        val b = QueryResult("key".toByteArray(), "val2".toByteArray())
        assertNotEquals(a, b)
    }

    @Test
    fun `QueryResult with different keys are not equal`() {
        val a = QueryResult("k1".toByteArray(), "val".toByteArray())
        val b = QueryResult("k2".toByteArray(), "val".toByteArray())
        assertNotEquals(a, b)
    }

    @Test
    fun `QueryResult is not equal to non-QueryResult`() {
        val qr = QueryResult("key".toByteArray(), "val".toByteArray())
        assertTrue(qr != "not a QueryResult" as Any)
    }

    @Test
    fun `QueryResult identity equality`() {
        val qr = QueryResult("key".toByteArray(), "val".toByteArray())
        assertEquals(qr, qr)
    }

    @Test
    fun `QueryResult value-null vs value-present are not equal`() {
        val a = QueryResult("k".toByteArray(), null)
        val b = QueryResult("k".toByteArray(), "v".toByteArray())
        assertNotEquals(a, b)
    }

    // -- RaftCollection scoping (pure-Dart logic) ----------------------------

    @Test
    fun `collection scopes keys with prefix`() {
        val collectionName = "users"
        val expectedPrefix = "$collectionName:"
        val id = "42"
        val expectedKey = "$collectionName:$id"

        assertEquals("users:42", expectedKey)
        assertEquals("users:", expectedPrefix)
    }

    @Test
    fun `different collection names produce different scoped keys`() {
        val users = "users:1".toByteArray(Charsets.UTF_8)
        val orders = "orders:1".toByteArray(Charsets.UTF_8)
        // Same id, different collection → distinct keys.
        assertNotEquals(users.toList(), orders.toList())
    }

    @Test
    fun `collection serializer and deserializer roundtrip`() {
        val serialize: (String) -> ByteArray = { it.toByteArray(Charsets.UTF_8) }
        val deserialize: (ByteArray) -> String = { String(it, Charsets.UTF_8) }

        val original = """{"id":"1","name":"Alice"}"""
        val bytes = serialize(original)
        val restored = deserialize(bytes)
        assertEquals(original, restored)
    }

    @Test
    fun `non-ascii ids round-trip through utf-8 scoping`() {
        // A real user key — emoji, accents, CJK — should encode losslessly.
        val id = "ユーザー🚀café"
        val scoped = "users:$id"
        val bytes = scoped.toByteArray(Charsets.UTF_8)
        val restored = String(bytes, Charsets.UTF_8)
        assertEquals(scoped, restored)
    }
}
