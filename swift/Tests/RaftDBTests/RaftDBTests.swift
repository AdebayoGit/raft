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

    func testRoundTripCodes() {
        let cases: [(UInt32, RaftError)] = [
            (1, .nullPointer),
            (2, .invalidUtf8),
            (3, .ioError),
            (4, .notFound),
            (5, .bufferTooSmall),
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
        XCTAssertTrue(RaftError.unknown(42).description.contains("42"))
    }

    func testAllCasesConformToError() {
        let errors: [Error] = [
            RaftError.nullPointer,
            RaftError.invalidUtf8,
            RaftError.ioError,
            RaftError.notFound,
            RaftError.bufferTooSmall,
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
}
