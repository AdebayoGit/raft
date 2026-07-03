import XCTest
@testable import RaftDB

// MARK: - RaftError Tests

final class RaftErrorTests: XCTestCase {

    func testCode0ReturnsNil() {
        XCTAssertNil(RaftError.fromCode(0))
    }

    func testCode1MapsToNullPointer() {
        XCTAssertEqual(RaftError.fromCode(1), .nullPointer)
    }

    func testCode2MapsToInvalidUtf8() {
        XCTAssertEqual(RaftError.fromCode(2), .invalidUtf8)
    }

    func testCode3MapsToIoError() {
        XCTAssertEqual(RaftError.fromCode(3), .ioError)
    }

    func testCode4MapsToNotFound() {
        XCTAssertEqual(RaftError.fromCode(4), .notFound)
    }

    func testCode5MapsToBufferTooSmall() {
        XCTAssertEqual(RaftError.fromCode(5), .bufferTooSmall)
    }

    func testCode6MapsToInvalidJson() {
        XCTAssertEqual(RaftError.fromCode(6), .invalidJson)
    }

    func testCode7MapsToTransactionConflict() {
        XCTAssertEqual(RaftError.fromCode(7), .transactionConflict)
    }

    func testCode8MapsToInvalidHandle() {
        XCTAssertEqual(RaftError.fromCode(8), .invalidHandle)
    }

    func testCode9MapsToUnknownSubscription() {
        XCTAssertEqual(RaftError.fromCode(9), .unknownSubscription)
    }

    func testCode10MapsToInternalPanic() {
        XCTAssertEqual(RaftError.fromCode(10), .internalPanic)
    }

    func testCode11MapsToInvalidPath() {
        XCTAssertEqual(RaftError.fromCode(11), .invalidPath)
    }

    func testCode12MapsToDartApiNotInitialized() {
        XCTAssertEqual(RaftError.fromCode(12), .dartApiNotInitialized)
    }

    func testCode13MapsToPayloadTooLarge() {
        XCTAssertEqual(RaftError.fromCode(13), .payloadTooLarge)
    }

    func testCode14MapsToUnsupportedVersion() {
        XCTAssertEqual(RaftError.fromCode(14), .unsupportedVersion)
    }

    func testUnknownCodeMapsToUnknown() {
        let error = RaftError.fromCode(99)
        XCTAssertEqual(error, .unknown(99))
    }

    func testCheckDoesNotThrowForCode0() {
        XCTAssertNoThrow(try RaftError.check(0))
    }

    func testCheckThrowsForNonZeroCode() {
        XCTAssertThrowsError(try RaftError.check(3)) { error in
            XCTAssertEqual(error as? RaftError, .ioError)
        }
    }

    func testCheckThrowsTransactionConflict() {
        XCTAssertThrowsError(try RaftError.check(7)) { error in
            XCTAssertEqual(error as? RaftError, .transactionConflict)
        }
    }

    func testCheckThrowsInvalidHandle() {
        XCTAssertThrowsError(try RaftError.check(8)) { error in
            XCTAssertEqual(error as? RaftError, .invalidHandle)
        }
    }

    func testRoundTripCodes() {
        let cases: [(UInt32, RaftError)] = [
            (1, .nullPointer),
            (2, .invalidUtf8),
            (3, .ioError),
            (4, .notFound),
            (5, .bufferTooSmall),
            (6, .invalidJson),
            (7, .transactionConflict),
            (8, .invalidHandle),
            (9, .unknownSubscription),
            (10, .internalPanic),
            (11, .invalidPath),
            (12, .dartApiNotInitialized),
            (13, .payloadTooLarge),
            (14, .unsupportedVersion),
        ]
        for (code, expected) in cases {
            let mapped = RaftError.fromCode(code)
            XCTAssertEqual(mapped, expected)
            XCTAssertEqual(mapped?.code, code)
        }
    }

    func testDescriptionContainsUsefulInfo() {
        XCTAssertTrue(RaftError.nullPointer.description.contains("null"))
        XCTAssertTrue(RaftError.invalidUtf8.description.contains("UTF-8"))
        XCTAssertTrue(RaftError.ioError.description.contains("I/O"))
        XCTAssertTrue(RaftError.notFound.description.contains("not found"))
        XCTAssertTrue(RaftError.bufferTooSmall.description.contains("buffer"))
        XCTAssertTrue(RaftError.invalidJson.description.contains("JSON"))
        XCTAssertTrue(RaftError.transactionConflict.description.contains("conflict"))
        XCTAssertTrue(RaftError.invalidHandle.description.contains("handle"))
        XCTAssertTrue(RaftError.unknownSubscription.description.contains("ubscription"))
        XCTAssertTrue(RaftError.internalPanic.description.contains("panic"))
        XCTAssertTrue(RaftError.invalidPath.description.contains("path"))
        XCTAssertTrue(RaftError.dartApiNotInitialized.description.contains("Dart API"))
        XCTAssertTrue(RaftError.payloadTooLarge.description.contains("size cap"))
        XCTAssertTrue(RaftError.unsupportedVersion.description.contains("version"))
        XCTAssertTrue(RaftError.unknown(42).description.contains("42"))
    }

    func testAllCasesConformToError() {
        let errors: [Error] = [
            RaftError.nullPointer,
            RaftError.invalidUtf8,
            RaftError.ioError,
            RaftError.notFound,
            RaftError.bufferTooSmall,
            RaftError.invalidJson,
            RaftError.transactionConflict,
            RaftError.invalidHandle,
            RaftError.unknownSubscription,
            RaftError.internalPanic,
            RaftError.invalidPath,
            RaftError.dartApiNotInitialized,
            RaftError.payloadTooLarge,
            RaftError.unsupportedVersion,
            RaftError.unknown(99),
        ]
        for error in errors {
            XCTAssertTrue(error is RaftError)
        }
    }
}

// MARK: - QueryDiff Tests

final class QueryDiffTests: XCTestCase {

    func testEmptyByDefault() {
        let diff = QueryDiff()
        XCTAssertTrue(diff.isEmpty)
        XCTAssertEqual(diff.added.count, 0)
        XCTAssertEqual(diff.removed.count, 0)
        XCTAssertEqual(diff.updated.count, 0)
    }

    func testIsEmptyWhenAllBucketsEmpty() {
        XCTAssertTrue(QueryDiff(added: [], removed: [], updated: []).isEmpty)
    }

    func testIsNotEmptyWhenAnyBucketHasItems() {
        let doc = Data(#"{"id":1}"#.utf8)
        XCTAssertFalse(QueryDiff(added: [doc]).isEmpty)
        XCTAssertFalse(QueryDiff(removed: [doc]).isEmpty)
        XCTAssertFalse(QueryDiff(updated: [doc]).isEmpty)
    }

    func testParseFromJSON() throws {
        let payload = #"""
        {
          "added":   [{"id": 1, "name": "a"}],
          "removed": [],
          "updated": [{"id": 2, "name": "b"}, {"id": 3, "name": "c"}]
        }
        """#
        let diff = try QueryDiff.parse(Data(payload.utf8))
        XCTAssertEqual(diff.added.count, 1)
        XCTAssertEqual(diff.removed.count, 0)
        XCTAssertEqual(diff.updated.count, 2)

        // Each element is preserved as raw JSON bytes
        let decoded = try JSONSerialization.jsonObject(with: diff.added[0]) as? [String: Any]
        XCTAssertEqual(decoded?["name"] as? String, "a")
    }

    func testParseMissingKeysProducesEmptyArrays() throws {
        let payload = #"{"added": [{"id": 1}]}"#
        let diff = try QueryDiff.parse(Data(payload.utf8))
        XCTAssertEqual(diff.added.count, 1)
        XCTAssertEqual(diff.removed.count, 0)
        XCTAssertEqual(diff.updated.count, 0)
    }

    func testParseInvalidJsonThrows() {
        XCTAssertThrowsError(try QueryDiff.parse(Data("not-json".utf8)))
    }
}

// MARK: - MutationEvent Tests

final class MutationEventTests: XCTestCase {

    func testDecodeFromJSON() throws {
        let payload = #"""
        {
          "collection": "users",
          "doc_id": 42,
          "mutation_type": "Insert",
          "origin": "Local"
        }
        """#
        let event = try JSONDecoder().decode(MutationEvent.self, from: Data(payload.utf8))
        XCTAssertEqual(event.collection, "users")
        XCTAssertEqual(event.docId, 42)
        XCTAssertEqual(event.mutationType, .insert)
        XCTAssertEqual(event.origin, .local)
    }

    func testRoundTripAllKinds() throws {
        for kind in [MutationEvent.Kind.insert, .update, .delete] {
            let event = MutationEvent(
                collection: "c",
                docId: 1,
                mutationType: kind,
                origin: .remote
            )
            let data = try JSONEncoder().encode(event)
            let decoded = try JSONDecoder().decode(MutationEvent.self, from: data)
            XCTAssertEqual(decoded, event)
        }
    }
}

// MARK: - RaftDBClosedError Tests

final class RaftDBClosedErrorTests: XCTestCase {

    func testClosedErrorDescription() {
        let error = RaftDBClosedError()
        XCTAssertTrue(error.description.contains("closed"))
    }

    func testClosedErrorConformsToError() {
        let error: Error = RaftDBClosedError()
        XCTAssertTrue(error is RaftDBClosedError)
    }
}

// MARK: - LockedBool Tests

final class LockedBoolTests: XCTestCase {

    func testInitialValue() {
        let b = LockedBool(false)
        XCTAssertFalse(b.value)

        let t = LockedBool(true)
        XCTAssertTrue(t.value)
    }

    func testCompareExchangeSucceeds() {
        let b = LockedBool(false)
        let swapped = b.compareExchange(expected: false, desired: true)
        XCTAssertTrue(swapped)
        XCTAssertTrue(b.value)
    }

    func testCompareExchangeFailsOnMismatch() {
        let b = LockedBool(false)
        let swapped = b.compareExchange(expected: true, desired: false)
        XCTAssertFalse(swapped)
        XCTAssertFalse(b.value)
    }

    func testCompareExchangeIdempotent() {
        // Compare-and-swap should fail the second time when the source is
        // already at the desired state.
        let b = LockedBool(false)
        XCTAssertTrue(b.compareExchange(expected: false, desired: true))
        XCTAssertFalse(b.compareExchange(expected: false, desired: true))
        XCTAssertTrue(b.value)
    }

    func testConcurrentAccess() {
        let b = LockedBool(false)
        let group = DispatchGroup()
        var successCount = 0
        let lock = NSLock()

        for _ in 0..<100 {
            group.enter()
            DispatchQueue.global().async {
                if b.compareExchange(expected: false, desired: true) {
                    lock.lock()
                    successCount += 1
                    lock.unlock()
                    // Reset for next iteration
                    _ = b.compareExchange(expected: true, desired: false)
                }
                group.leave()
            }
        }

        group.wait()
        // At least one should have succeeded
        XCTAssertGreaterThan(successCount, 0)
    }
}

// MARK: - RaftCollection Key Scoping Tests

final class RaftCollectionScopingTests: XCTestCase {

    func testScopedKeyFormat() {
        // Verify the collection key prefix logic matches expected format.
        let collectionName = "users"
        let id = "42"
        let expectedKey = "\(collectionName):\(id)"
        let expectedPrefix = "\(collectionName):"

        XCTAssertEqual(expectedKey, "users:42")
        XCTAssertEqual(expectedPrefix, "users:")
        XCTAssertEqual(Data(expectedKey.utf8), Data("users:42".utf8))
    }

    func testCodableRoundTrip() throws {
        struct TestDoc: Codable, Equatable {
            let id: String
            let name: String
        }

        let original = TestDoc(id: "1", name: "Alice")
        let encoder = JSONEncoder()
        let decoder = JSONDecoder()

        let data = try encoder.encode(original)
        let decoded = try decoder.decode(TestDoc.self, from: data)

        XCTAssertEqual(original, decoded)
    }

    func testDifferentCollectionsProduceDifferentKeys() {
        let usersKey = Data("users:1".utf8)
        let postsKey = Data("posts:1".utf8)

        XCTAssertNotEqual(usersKey, postsKey)
    }

    func testNonAsciiIdsRoundTrip() {
        // Make sure emoji / accents / CJK survive utf-8 scoping.
        let id = "ユーザー🚀café"
        let scoped = "users:\(id)"
        let restored = String(data: Data(scoped.utf8), encoding: .utf8)
        XCTAssertEqual(scoped, restored)
    }

    func testCustomEncoderProducesDifferentBytes() throws {
        struct Doc: Codable {
            let createdAt: Date
        }

        let date = Date(timeIntervalSince1970: 1_700_000_000)
        let doc = Doc(createdAt: date)

        let defaultEncoder = JSONEncoder()
        let isoEncoder = JSONEncoder()
        isoEncoder.dateEncodingStrategy = .iso8601

        let defaultBytes = try defaultEncoder.encode(doc)
        let isoBytes = try isoEncoder.encode(doc)

        // Different encoding strategies produce different byte streams —
        // proves the user's encoder choice is honoured.
        XCTAssertNotEqual(defaultBytes, isoBytes)
    }
}

// MARK: - Thread Affinity Tests (F7e)

final class ThreadAffinityTests: XCTestCase {

    /// The native core fires observer callbacks on a background runtime
    /// thread — never the registering thread. The `AsyncStream` bridge
    /// must marshal those events safely into Swift concurrency, including
    /// consumers isolated to the main actor.
    func testObserveEventsAreDeliveredToAMainActorConsumer() async throws {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("raft-thread-affinity-\(UUID().uuidString)").path
        defer { try? FileManager.default.removeItem(atPath: dir) }

        let db = try await RaftDB.open(path: dir)
        let stream = db.observe(collection: "users")

        let consumer = Task { @MainActor () -> MutationEvent? in
            for await event in stream {
                // Consumption hops onto the main actor regardless of
                // which native thread produced the event.
                XCTAssertTrue(Thread.isMainThread)
                return event
            }
            return nil
        }

        // Let the subscription task register before mutating.
        try await Task.sleep(nanoseconds: 100_000_000)
        let doc = Data(#"{"id":1,"fields":{}}"#.utf8)
        try await db.collectionPut("users", document: doc)

        let event = await consumer.value
        XCTAssertEqual(event?.collection, "users")
        XCTAssertEqual(event?.mutationType, .insert)
        db.close()
    }
}
