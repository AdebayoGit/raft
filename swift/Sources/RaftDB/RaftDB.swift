import Foundation

// MARK: - C function bindings via @_silgen_name
//
// The xcframework ships a static library exporting these C symbols.
// We bind them directly rather than using a bridging header or modulemap.

// MARK: Raw KV

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

// MARK: Typed Collections

@_silgen_name("rft_collection_put")
private func rft_collection_put(_ db: OpaquePointer,
                                _ collection: UnsafePointer<CChar>,
                                _ docJson: UnsafePointer<UInt8>,
                                _ docJsonLen: Int) -> UInt32

@_silgen_name("rft_collection_put_auto")
private func rft_collection_put_auto(_ db: OpaquePointer,
                                     _ collection: UnsafePointer<CChar>,
                                     _ docJson: UnsafePointer<UInt8>,
                                     _ docJsonLen: Int,
                                     _ outDocId: UnsafeMutablePointer<UInt64>) -> UInt32

@_silgen_name("rft_collection_get")
private func rft_collection_get(_ db: OpaquePointer,
                                _ collection: UnsafePointer<CChar>,
                                _ docId: UInt64,
                                _ outBuf: UnsafeMutablePointer<UInt8>?,
                                _ outLen: UnsafeMutablePointer<Int>) -> UInt32

@_silgen_name("rft_collection_delete")
private func rft_collection_delete(_ db: OpaquePointer,
                                   _ collection: UnsafePointer<CChar>,
                                   _ docId: UInt64) -> UInt32

@_silgen_name("rft_collection_count")
private func rft_collection_count(_ db: OpaquePointer,
                                  _ collection: UnsafePointer<CChar>,
                                  _ outCount: UnsafeMutablePointer<Int>) -> UInt32

@_silgen_name("rft_collection_list_ids")
private func rft_collection_list_ids(_ db: OpaquePointer,
                                     _ collection: UnsafePointer<CChar>,
                                     _ outIds: UnsafeMutablePointer<UInt64>?,
                                     _ outLen: UnsafeMutablePointer<Int>) -> UInt32

// MARK: Queries

@_silgen_name("rft_query_execute")
private func rft_query_execute(_ db: OpaquePointer,
                               _ queryJson: UnsafePointer<UInt8>,
                               _ queryJsonLen: Int,
                               _ outResult: UnsafeMutablePointer<OpaquePointer?>) -> UInt32

@_silgen_name("rft_query_result_count")
private func rft_query_result_count(_ result: OpaquePointer?) -> Int

@_silgen_name("rft_query_result_get")
private func rft_query_result_get(_ result: OpaquePointer,
                                  _ index: Int,
                                  _ outBuf: UnsafeMutablePointer<UInt8>?,
                                  _ outLen: UnsafeMutablePointer<Int>) -> UInt32

@_silgen_name("rft_query_result_free")
private func rft_query_result_free(_ result: OpaquePointer?)

// MARK: Transactions

@_silgen_name("rft_transaction_begin")
internal func rft_transaction_begin(_ db: OpaquePointer,
                                    _ outTxn: UnsafeMutablePointer<OpaquePointer?>) -> UInt32

@_silgen_name("rft_transaction_get")
internal func rft_transaction_get(_ txn: OpaquePointer,
                                  _ collection: UnsafePointer<CChar>,
                                  _ docId: UInt64,
                                  _ outBuf: UnsafeMutablePointer<UInt8>?,
                                  _ outLen: UnsafeMutablePointer<Int>) -> UInt32

@_silgen_name("rft_transaction_put")
internal func rft_transaction_put(_ txn: OpaquePointer,
                                  _ collection: UnsafePointer<CChar>,
                                  _ docJson: UnsafePointer<UInt8>,
                                  _ docJsonLen: Int) -> UInt32

@_silgen_name("rft_transaction_delete")
internal func rft_transaction_delete(_ txn: OpaquePointer,
                                     _ collection: UnsafePointer<CChar>,
                                     _ docId: UInt64) -> UInt32

@_silgen_name("rft_transaction_commit")
internal func rft_transaction_commit(_ txn: OpaquePointer) -> UInt32

@_silgen_name("rft_transaction_rollback")
internal func rft_transaction_rollback(_ txn: OpaquePointer)

// MARK: Observation

private typealias RftObserveCallback = @convention(c) (UnsafePointer<CChar>?, UnsafeMutableRawPointer?) -> Void

@_silgen_name("rft_observe")
private func rft_observe(_ db: OpaquePointer,
                         _ collection: UnsafePointer<CChar>,
                         _ callback: RftObserveCallback,
                         _ userData: UnsafeMutableRawPointer?,
                         _ outSubId: UnsafeMutablePointer<UInt64>) -> UInt32

@_silgen_name("rft_observe_query")
private func rft_observe_query(_ db: OpaquePointer,
                               _ queryJson: UnsafePointer<UInt8>,
                               _ queryJsonLen: Int,
                               _ callback: RftObserveCallback,
                               _ userData: UnsafeMutableRawPointer?,
                               _ outSubId: UnsafeMutablePointer<UInt64>) -> UInt32

@_silgen_name("rft_unobserve")
private func rft_unobserve(_ db: OpaquePointer, _ subId: UInt64) -> UInt32

// MARK: - Event types
//
// Shapes match the Rust JSON envelopes from `core/src/reactive/`.

/// A mutation notification emitted by ``RaftDB/observe(collection:)``.
///
/// The Rust core emits these as JSON over the FFI; Swift decodes them
/// via `Codable` before yielding into the stream.
public struct MutationEvent: Sendable, Codable, Equatable {

    public enum Kind: String, Sendable, Codable {
        case insert = "Insert"
        case update = "Update"
        case delete = "Delete"
        case resyncRequired = "ResyncRequired"
    }

    public enum Origin: String, Sendable, Codable {
        case local = "Local"
        case remote = "Remote"
    }

    public let collection: String
    public let docId: UInt64
    public let mutationType: Kind
    public let origin: Origin

    enum CodingKeys: String, CodingKey {
        case collection
        case docId = "doc_id"
        case mutationType = "mutation_type"
        case origin
    }
}

/// The diff between two consecutive live-query result sets.
///
/// Emitted by ``RaftDB/observeQuery(_:)``. Each array holds raw JSON
/// bytes for the documents that were added, removed, or updated since
/// the previous tick.
public struct QueryDiff: Sendable, Equatable {
    /// JSON-encoded documents present in the new results but absent
    /// from the old.
    public let added: [Data]
    /// JSON-encoded documents present in the old results but absent
    /// from the new.
    public let removed: [Data]
    /// JSON-encoded documents present in both but with changed fields.
    public let updated: [Data]

    public init(added: [Data] = [], removed: [Data] = [], updated: [Data] = []) {
        self.added = added
        self.removed = removed
        self.updated = updated
    }

    /// `true` if no buckets contain any documents.
    public var isEmpty: Bool {
        added.isEmpty && removed.isEmpty && updated.isEmpty
    }

    /// Parse a live-query JSON payload into a `QueryDiff`. Each
    /// element is preserved as the raw JSON bytes of one document.
    static func parse(_ json: Data) throws -> QueryDiff {
        let obj = try JSONSerialization.jsonObject(with: json, options: [])
        guard let dict = obj as? [String: Any] else {
            throw RaftError.invalidJson
        }
        func encodeArray(_ key: String) throws -> [Data] {
            guard let arr = dict[key] as? [Any] else { return [] }
            return try arr.map { element in
                try JSONSerialization.data(withJSONObject: element, options: [])
            }
        }
        return QueryDiff(
            added: try encodeArray("added"),
            removed: try encodeArray("removed"),
            updated: try encodeArray("updated")
        )
    }
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
public final class RaftDB: @unchecked Sendable {

    internal let handle: OpaquePointer
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
    /// Safe to call multiple times; subsequent calls are no-ops. All
    /// pending observer tasks are aborted by `rft_close` on the Rust side.
    public func close() {
        guard _closed.compareExchange(expected: false, desired: true) else { return }
        rft_close(handle)
    }

    // MARK: - Raw KV

    /// Inserts or updates `value` for `key` on the raw KV engine.
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
    /// Uses the two-phase read protocol: query size with a null buffer,
    /// then read into an exact-size buffer.
    ///
    /// - Throws: ``RaftError`` for errors other than not-found.
    public func get(key: Data) async throws -> Data? {
        try ensureOpen()
        let h = handle
        return try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global(qos: .userInitiated).async {
                let result: Result<Data?, Error> = key.withUnsafeBytes { keyBuf in
                    let keyPtr = keyBuf.baseAddress!.assumingMemoryBound(to: UInt8.self)
                    var neededLen = 0
                    let sizeCode = rft_get(h, keyPtr, key.count, nil, &neededLen)
                    if sizeCode == 4 { return .success(nil) } // NOT_FOUND
                    if sizeCode != 5 && sizeCode != 0 {        // not BUFFER_TOO_SMALL or OK
                        return .failure(RaftError.fromCode(sizeCode)!)
                    }
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

    /// Deletes `key` from the raw KV engine.
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

    // MARK: - Typed Collections

    /// Insert or update a document (JSON) in `collection`. The document's
    /// `id` field is honoured.
    public func collectionPut(_ collection: String, document json: Data) async throws {
        try ensureOpen()
        let h = handle
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            DispatchQueue.global(qos: .userInitiated).async {
                let code = collection.withCString { cName in
                    json.withUnsafeBytes { jsonBuf in
                        rft_collection_put(
                            h,
                            cName,
                            jsonBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                            json.count
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

    /// Insert a document, letting the database assign a fresh id.
    /// Returns the assigned id.
    public func collectionPutAuto(_ collection: String, document json: Data) async throws -> UInt64 {
        try ensureOpen()
        let h = handle
        return try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global(qos: .userInitiated).async {
                var outId: UInt64 = 0
                let code = collection.withCString { cName in
                    json.withUnsafeBytes { jsonBuf in
                        rft_collection_put_auto(
                            h,
                            cName,
                            jsonBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                            json.count,
                            &outId
                        )
                    }
                }
                if code != 0 {
                    continuation.resume(throwing: RaftError.fromCode(code)!)
                } else {
                    continuation.resume(returning: outId)
                }
            }
        }
    }

    /// Fetch a document by id from `collection`, returning its raw JSON.
    /// Returns `nil` when the document is not found.
    public func collectionGet(_ collection: String, id docId: UInt64) async throws -> Data? {
        try ensureOpen()
        let h = handle
        return try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global(qos: .userInitiated).async {
                let result: Result<Data?, Error> = collection.withCString { cName in
                    var neededLen = 0
                    let sizeCode = rft_collection_get(h, cName, docId, nil, &neededLen)
                    if sizeCode == 4 { return .success(nil) }
                    if sizeCode != 5 && sizeCode != 0 {
                        return .failure(RaftError.fromCode(sizeCode)!)
                    }
                    var buf = Data(count: neededLen)
                    var readLen = neededLen
                    let readCode = buf.withUnsafeMutableBytes { bufPtr in
                        rft_collection_get(
                            h,
                            cName,
                            docId,
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

    /// Delete a document by id. Deleting a non-existent id is not an error.
    public func collectionDelete(_ collection: String, id docId: UInt64) async throws {
        try ensureOpen()
        let h = handle
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            DispatchQueue.global(qos: .userInitiated).async {
                let code = collection.withCString { cName in
                    rft_collection_delete(h, cName, docId)
                }
                if code != 0 {
                    continuation.resume(throwing: RaftError.fromCode(code)!)
                } else {
                    continuation.resume()
                }
            }
        }
    }

    /// Number of documents currently in `collection`.
    public func collectionCount(_ collection: String) async throws -> Int {
        try ensureOpen()
        let h = handle
        return try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global(qos: .userInitiated).async {
                var count = 0
                let code = collection.withCString { cName in
                    rft_collection_count(h, cName, &count)
                }
                if code != 0 {
                    continuation.resume(throwing: RaftError.fromCode(code)!)
                } else {
                    continuation.resume(returning: count)
                }
            }
        }
    }

    /// List all document ids in `collection`, sorted ascending.
    public func collectionListIds(_ collection: String) async throws -> [UInt64] {
        try ensureOpen()
        let h = handle
        return try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global(qos: .userInitiated).async {
                let result: Result<[UInt64], Error> = collection.withCString { cName in
                    var needed = 0
                    let sizeCode = rft_collection_list_ids(h, cName, nil, &needed)
                    if sizeCode != 5 && sizeCode != 0 {
                        return .failure(RaftError.fromCode(sizeCode)!)
                    }
                    if needed == 0 { return .success([]) }
                    var ids = [UInt64](repeating: 0, count: needed)
                    var actual = needed
                    let readCode = ids.withUnsafeMutableBufferPointer { buf -> UInt32 in
                        rft_collection_list_ids(h, cName, buf.baseAddress, &actual)
                    }
                    if readCode != 0 {
                        return .failure(RaftError.fromCode(readCode)!)
                    }
                    return .success(Array(ids.prefix(actual)))
                }
                continuation.resume(with: result)
            }
        }
    }

    // MARK: - Queries

    /// Execute a predicate query (JSON-encoded) and return each matching
    /// document as raw JSON `Data`.
    public func executeQuery(_ queryJson: Data) async throws -> [Data] {
        try ensureOpen()
        let h = handle
        return try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global(qos: .userInitiated).async {
                let result: Result<[Data], Error> = queryJson.withUnsafeBytes { qBuf in
                    var resultPtr: OpaquePointer?
                    let execCode = rft_query_execute(
                        h,
                        qBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                        queryJson.count,
                        &resultPtr
                    )
                    if execCode != 0 {
                        return .failure(RaftError.fromCode(execCode)!)
                    }
                    guard let resultHandle = resultPtr else {
                        return .success([])
                    }
                    defer { rft_query_result_free(resultHandle) }

                    let count = rft_query_result_count(resultHandle)
                    var docs = [Data]()
                    docs.reserveCapacity(count)
                    for i in 0..<count {
                        var needed = 0
                        let sizeCode = rft_query_result_get(resultHandle, i, nil, &needed)
                        if sizeCode != 5 && sizeCode != 0 {
                            return .failure(RaftError.fromCode(sizeCode)!)
                        }
                        var buf = Data(count: needed)
                        var actual = needed
                        let readCode = buf.withUnsafeMutableBytes { p in
                            rft_query_result_get(
                                resultHandle,
                                i,
                                p.baseAddress!.assumingMemoryBound(to: UInt8.self),
                                &actual
                            )
                        }
                        if readCode != 0 {
                            return .failure(RaftError.fromCode(readCode)!)
                        }
                        docs.append(buf.prefix(actual))
                    }
                    return .success(docs)
                }
                continuation.resume(with: result)
            }
        }
    }

    // MARK: - Observation

    /// Observe mutations within `collection`. Yields a ``MutationEvent``
    /// for every insert / update / delete on that collection.
    ///
    /// The stream finishes when the task is cancelled or the database
    /// is closed.
    public func observe(collection: String) -> AsyncStream<MutationEvent> {
        let h = handle
        let closed = _closed
        return AsyncStream { continuation in
            guard !closed.value else {
                continuation.finish()
                return
            }
            let decoder = JSONDecoder()
            let ctx = ObserveContext { jsonPtr in
                let s = String(cString: jsonPtr)
                guard let data = s.data(using: .utf8) else { return }
                if let event = try? decoder.decode(MutationEvent.self, from: data) {
                    continuation.yield(event)
                }
            }
            let userData = Unmanaged.passRetained(ctx).toOpaque()

            var subId: UInt64 = 0
            let code = collection.withCString { cName in
                rft_observe(h, cName, raftObserveTrampoline, userData, &subId)
            }
            if code != 0 {
                Unmanaged<ObserveContext>.fromOpaque(userData).release()
                continuation.finish()
                return
            }
            let pinnedSubId = subId
            continuation.onTermination = { @Sendable _ in
                _ = rft_unobserve(h, pinnedSubId)
                Unmanaged<ObserveContext>.fromOpaque(userData).release()
            }
        }
    }

    /// Observe a live query. Yields a ``QueryDiff`` immediately with the
    /// initial snapshot and again every time the result set changes.
    ///
    /// `queryJson` is the JSON encoding of the predicate query.
    /// The stream finishes when the task is cancelled or the database
    /// is closed.
    public func observeQuery(_ queryJson: Data) -> AsyncStream<QueryDiff> {
        let h = handle
        let closed = _closed
        return AsyncStream { continuation in
            guard !closed.value else {
                continuation.finish()
                return
            }
            let ctx = ObserveContext { jsonPtr in
                let s = String(cString: jsonPtr)
                guard let data = s.data(using: .utf8) else { return }
                if let diff = try? QueryDiff.parse(data) {
                    continuation.yield(diff)
                }
            }
            let userData = Unmanaged.passRetained(ctx).toOpaque()

            var subId: UInt64 = 0
            let code = queryJson.withUnsafeBytes { qBuf in
                rft_observe_query(
                    h,
                    qBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                    queryJson.count,
                    raftObserveTrampoline,
                    userData,
                    &subId
                )
            }
            if code != 0 {
                Unmanaged<ObserveContext>.fromOpaque(userData).release()
                continuation.finish()
                return
            }
            let pinnedSubId = subId
            continuation.onTermination = { @Sendable _ in
                _ = rft_unobserve(h, pinnedSubId)
                Unmanaged<ObserveContext>.fromOpaque(userData).release()
            }
        }
    }

    // MARK: - Internal

    internal func ensureOpen() throws {
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

// MARK: - Observe internals

/// Holds the closure that handles a callback from the C side.
///
/// A pointer to one of these is passed as `user_data` to `rft_observe`
/// / `rft_observe_query`. The C trampoline below recovers it via
/// `Unmanaged` and invokes `onEvent`. The instance is retained for the
/// lifetime of the subscription and released in `onTermination`.
final class ObserveContext: @unchecked Sendable {
    let onEvent: (UnsafePointer<CChar>) -> Void
    init(onEvent: @escaping (UnsafePointer<CChar>) -> Void) {
        self.onEvent = onEvent
    }
}

/// Top-level C-compatible callback. Cannot capture context, so it
/// recovers the closure from `userData`.
private let raftObserveTrampoline: RftObserveCallback = { eventJson, userData in
    guard let eventJson, let userData else { return }
    let ctx = Unmanaged<ObserveContext>.fromOpaque(userData).takeUnretainedValue()
    ctx.onEvent(eventJson)
}

// MARK: - LockedBool (Sendable-safe atomic-like boolean)

/// A simple thread-safe boolean wrapper using `NSLock`.
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
