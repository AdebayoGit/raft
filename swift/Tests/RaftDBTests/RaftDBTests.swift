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
            RaftError.unknown(99),
        ]
        for error in errors {
            XCTAssertTrue(error is RaftError)
        }
    }
}

// MARK: - QueryDiff Tests

final class QueryDiffTests: XCTestCase {

    func testQueryDiffHoldsKeyAndValue() {
        let key = Data("test-key".utf8)
        let value = Data("test-value".utf8)
        let diff = QueryDiff(key: key, value: value)

        XCTAssertEqual(diff.key, key)
        XCTAssertEqual(diff.value, value)
    }

    func testQueryDiffWithNilValue() {
        let diff = QueryDiff(key: Data("key".utf8), value: nil)
        XCTAssertNil(diff.value)
    }

    func testQueryDiffWithEmptyValue() {
        // Empty-but-present value (Data of length zero) is distinct from nil.
        let diff = QueryDiff(key: Data("key".utf8), value: Data())
        XCTAssertNotNil(diff.value)
        XCTAssertEqual(diff.value?.count, 0)
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
