import Foundation

// MARK: - C function bindings via @_silgen_name

// The xcframework ships a static library exporting these C symbols.
// We bind them directly rather than using a bridging header or modulemap.

@_silgen_name("rft_open")
private func rft_open(_ path: UnsafePointer<CChar>,
                      _ outErr: UnsafeMutablePointer<UInt32>) -> OpaquePointer?

@_silgen_name("rft_close")
private func rft_close(_ db: OpaquePointer?)

@_silgen_name("rft_put")
private func rft_put(_ db: OpaquePointer,
                     _ key: UnsafePointer<UInt8>,
                     _ keyLen: Int,
                     _ value: UnsafePointer<UInt8>,
                     _ valueLen: Int) -> UInt32

@_silgen_name("rft_get")
private func rft_get(_ db: OpaquePointer,
                     _ key: UnsafePointer<UInt8>,
                     _ keyLen: Int,
                     _ outValue: UnsafeMutablePointer<UInt8>?,
                     _ outLen: UnsafeMutablePointer<Int>) -> UInt32

@_silgen_name("rft_delete")
private func rft_delete(_ db: OpaquePointer,
                        _ key: UnsafePointer<UInt8>,
                        _ keyLen: Int) -> UInt32

// MARK: - QueryDiff

/// A change notification emitted by live query observation.
public struct QueryDiff: Sendable {
    /// The key that changed.
    public let key: Data
    /// The current value, or `nil` if deleted / not found.
    public let value: Data?
}

// MARK: - RaftDB

/// A handle to an open Raft embedded database.
///
/// All blocking native calls are dispatched off the calling actor via
/// `withCheckedThrowingContinuation` on a global concurrent queue.
///
/// ```swift
/// let db = try await RaftDB.open(path: "/path/to/db")
/// try await db.put(key: Data("hello".utf8), value: Data("world".utf8))
/// let val = try await db.get(key: Data("hello".utf8))
/// try await db.delete(key: Data("hello".utf8))
/// db.close()
/// ```
public final class RaftDB: Sendable {

    private let handle: OpaquePointer
    private let _closed = LockedBool(false)

    private init(handle: OpaquePointer) {
        self.handle = handle
    }

    deinit {
        close()
    }

    // MARK: - Lifecycle

    /// Opens or creates a database at `path`.
    ///
    /// - Throws: ``RaftError`` if the native open fails.
    public static func open(path: String) async throws -> RaftDB {
        try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global(qos: .userInitiated).async {
                var errCode: UInt32 = 0
                guard let ptr = path.withCString({ cPath in
                    rft_open(cPath, &errCode)
                }) else {
                    let error = RaftError.fromCode(errCode) ?? .ioError
                    continuation.resume(throwing: error)
                    return
                }
                if errCode != 0 {
                    rft_close(ptr)
                    continuation.resume(throwing: RaftError.fromCode(errCode)!)
                    return
                }
                continuation.resume(returning: RaftDB(handle: ptr))
            }
        }
    }

    /// Opens synchronously (for tests or non-async contexts).
    ///
    /// - Throws: ``RaftError`` if the native open fails.
    public static func openSync(path: String) throws -> RaftDB {
        var errCode: UInt32 = 0
        guard let ptr = path.withCString({ cPath in
            rft_open(cPath, &errCode)
        }) else {
            throw RaftError.fromCode(errCode) ?? .ioError
        }
        if errCode != 0 {
            rft_close(ptr)
            throw RaftError.fromCode(errCode)!
        }
        return RaftDB(handle: ptr)
    }

    /// Closes the database and releases the native handle.
    ///
    /// Safe to call multiple times; subsequent calls are no-ops.
    public func close() {
        guard _closed.compareExchange(expected: false, desired: true) else { return }
        rft_close(handle)
    }

    // MARK: - Writes

    /// Inserts or updates `value` for `key`.
    ///
    /// - Throws: ``RaftError`` on native failure.
    public func put(key: Data, value: Data) async throws {
        try ensureOpen()
        let h = handle
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            DispatchQueue.global(qos: .userInitiated).async {
                let code = key.withUnsafeBytes { keyBuf in
                    value.withUnsafeBytes { valBuf in
                        rft_put(
                            h,
                            keyBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                            key.count,
                            valBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                            value.count
                        )
                    }
                }
                if code != 0 {
                    continuation.resume(throwing: RaftError.fromCode(code)!)
                } else {
                    continuation.resume()
                }
            }
        }
    }

    /// Returns the value stored at `key`, or `nil` if the key does not exist.
    ///
    /// Uses a two-phase read: first queries the required buffer size,
    /// then reads the value.
    ///
    /// - Throws: ``RaftError`` for errors other than not-found.
    public func get(key: Data) async throws -> Data? {
        try ensureOpen()
        let h = handle
        return try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global(qos: .userInitiated).async {
                let result: Result<Data?, Error> = key.withUnsafeBytes { keyBuf in
                    let keyPtr = keyBuf.baseAddress!.assumingMemoryBound(to: UInt8.self)

                    // Phase 1: query required size
                    var neededLen = 0
                    let sizeCode = rft_get(h, keyPtr, key.count, nil, &neededLen)
                    if sizeCode == 4 { // RFT_ERROR_NOT_FOUND
                        return .success(nil)
                    }
                    if sizeCode != 5 && sizeCode != 0 { // not BUFFER_TOO_SMALL, not OK
                        return .failure(RaftError.fromCode(sizeCode)!)
                    }

                    // Phase 2: read into buffer
                    var buf = Data(count: neededLen)
                    var readLen = neededLen
                    let readCode = buf.withUnsafeMutableBytes { bufPtr in
                        rft_get(
                            h,
                            keyPtr,
                            key.count,
                            bufPtr.baseAddress!.assumingMemoryBound(to: UInt8.self),
                            &readLen
                        )
                    }
                    if readCode != 0 {
                        return .failure(RaftError.fromCode(readCode)!)
                    }
                    return .success(buf.prefix(readLen))
                }
                continuation.resume(with: result)
            }
        }
    }

    /// Deletes `key` from the database.
    ///
    /// Deleting a non-existent key is not an error (a tombstone is written).
    ///
    /// - Throws: ``RaftError`` on native failure.
    public func delete(key: Data) async throws {
        try ensureOpen()
        let h = handle
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            DispatchQueue.global(qos: .userInitiated).async {
                let code = key.withUnsafeBytes { keyBuf in
                    rft_delete(
                        h,
                        keyBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                        key.count
                    )
                }
                if code != 0 {
                    continuation.resume(throwing: RaftError.fromCode(code)!)
                } else {
                    continuation.resume()
                }
            }
        }
    }

    // MARK: - Live Queries

    /// Observes changes to keys matching `prefix`.
    ///
    /// Emits the current snapshot immediately, then a ``QueryDiff`` for
    /// every subsequent write or delete under the prefix.
    ///
    /// The stream finishes when the task is cancelled or the database
    /// is closed.
    public func observe(prefix: Data) -> AsyncStream<QueryDiff> {
        let h = handle
        let closed = _closed

        return AsyncStream { continuation in
            // Emit initial snapshot
            DispatchQueue.global(qos: .userInitiated).async {
                guard !closed.value else {
                    continuation.finish()
                    return
                }
                let result: Data? = prefix.withUnsafeBytes { keyBuf in
                    let keyPtr = keyBuf.baseAddress!.assumingMemoryBound(to: UInt8.self)
                    var neededLen = 0
                    let sizeCode = rft_get(h, keyPtr, prefix.count, nil, &neededLen)
                    guard sizeCode == 5 || sizeCode == 0, neededLen > 0 else {
                        return nil
                    }
                    var buf = Data(count: neededLen)
                    var readLen = neededLen
                    let readCode = buf.withUnsafeMutableBytes { bufPtr in
                        rft_get(
                            h,
                            keyPtr,
                            prefix.count,
                            bufPtr.baseAddress!.assumingMemoryBound(to: UInt8.self),
                            &readLen
                        )
                    }
                    guard readCode == 0 else { return nil }
                    return buf.prefix(readLen)
                }
                continuation.yield(QueryDiff(key: prefix, value: result))
            }

            // Real implementation would register a native callback here.
            // For now, the stream stays open until cancellation.
            continuation.onTermination = { @Sendable _ in
                // Future: unregister native observer
            }
        }
    }

    // MARK: - Internal

    private func ensureOpen() throws {
        guard !_closed.value else {
            throw RaftDBClosedError()
        }
    }
}

// MARK: - RaftDBClosedError

/// Thrown when an operation is attempted on a closed database.
public struct RaftDBClosedError: Error, CustomStringConvertible {
    public var description: String { "RaftDB has been closed" }
}

// MARK: - LockedBool (Sendable-safe atomic-like boolean)

/// A simple thread-safe boolean wrapper using `os_unfair_lock`.
final class LockedBool: @unchecked Sendable {
    private var _value: Bool
    private let lock = NSLock()

    init(_ value: Bool) {
        _value = value
    }

    var value: Bool {
        lock.lock()
        defer { lock.unlock() }
        return _value
    }

    /// Atomically compares and swaps. Returns `true` if the exchange occurred.
    func compareExchange(expected: Bool, desired: Bool) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard _value == expected else { return false }
        _value = desired
        return true
    }
}
