import Foundation

/// An optimistic-concurrency transaction over a Raft database.
///
/// Begin a transaction with ``RaftDB/beginTransaction()``, read and
/// buffer writes, then call ``commit()`` or ``rollback()``. The handle
/// is consumed by either terminator and must not be reused.
///
/// At commit time, the engine validates that every document read during
/// the transaction has the same version it had when it was read. If any
/// tracked document was modified concurrently, ``commit()`` throws
/// ``RaftError/transactionConflict`` and no writes are applied.
///
/// ```swift
/// let txn = try db.beginTransaction()
/// let raw = try txn.get(collection: "users", id: 42)
/// // ...mutate `raw`...
/// try txn.put(collection: "users", document: raw)
/// try txn.commit()
/// ```
public final class RaftTransaction: @unchecked Sendable {

    private var handle: OpaquePointer?
    private let lock = NSLock()

    /// Initialised by ``RaftDB/beginTransaction()``.
    internal init(handle: OpaquePointer) {
        self.handle = handle
    }

    deinit {
        // If the user dropped the transaction without committing or
        // rolling back, free the native handle to avoid a leak.
        if let h = handle {
            rft_transaction_rollback(h)
        }
    }

    // MARK: - Operations

    /// Read a document by id. The version is recorded for conflict
    /// detection at commit time. Returns `nil` when the document does
    /// not exist (the read is still tracked).
    public func get(collection: String, id docId: UInt64) throws -> Data? {
        try withActiveHandle { h in
            try Self.readDocument(handle: h, collection: collection, docId: docId)
        }
    }

    /// Buffer a write inside the transaction. Applied atomically on
    /// commit. The document's `id` field must equal the storage doc id.
    public func put(collection: String, document json: Data) throws {
        try withActiveHandle { h in
            let code = collection.withCString { cName in
                json.withUnsafeBytes { jsonBuf in
                    rft_transaction_put(
                        h,
                        cName,
                        jsonBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                        json.count
                    )
                }
            }
            try RaftError.check(code)
        }
    }

    /// Buffer a delete inside the transaction.
    public func delete(collection: String, id docId: UInt64) throws {
        try withActiveHandle { h in
            let code = collection.withCString { cName in
                rft_transaction_delete(h, cName, docId)
            }
            try RaftError.check(code)
        }
    }

    // MARK: - Termination

    /// Validate the read set and atomically apply all buffered writes.
    ///
    /// Consumes the handle — calling `commit` or `rollback` again is a
    /// no-op (and the transaction is then unusable).
    ///
    /// - Throws: ``RaftError/transactionConflict`` if any tracked
    ///   document was modified concurrently. No writes are applied.
    public func commit() throws {
        lock.lock()
        defer { lock.unlock() }
        guard let h = handle else {
            throw RaftError.invalidHandle
        }
        handle = nil  // consumed regardless of outcome
        let code = rft_transaction_commit(h)
        try RaftError.check(code)
    }

    /// Discard the transaction. Consumes the handle.
    public func rollback() {
        lock.lock()
        defer { lock.unlock() }
        guard let h = handle else { return }
        handle = nil
        rft_transaction_rollback(h)
    }

    // MARK: - Internal

    private func withActiveHandle<T>(_ body: (OpaquePointer) throws -> T) throws -> T {
        lock.lock()
        defer { lock.unlock() }
        guard let h = handle else {
            throw RaftError.invalidHandle
        }
        return try body(h)
    }

    /// Two-phase get used by `get` and by the typed-Codable convenience
    /// on the extension below.
    private static func readDocument(handle: OpaquePointer,
                                     collection: String,
                                     docId: UInt64) throws -> Data? {
        try collection.withCString { cName -> Data? in
            var needed = 0
            let sizeCode = rft_transaction_get(handle, cName, docId, nil, &needed)
            if sizeCode == 4 { return nil } // NOT_FOUND
            if sizeCode != 5 && sizeCode != 0 {
                try RaftError.check(sizeCode)
            }
            var buf = Data(count: needed)
            var actual = needed
            let readCode = buf.withUnsafeMutableBytes { bufPtr in
                rft_transaction_get(
                    handle,
                    cName,
                    docId,
                    bufPtr.baseAddress!.assumingMemoryBound(to: UInt8.self),
                    &actual
                )
            }
            try RaftError.check(readCode)
            return buf.prefix(actual)
        }
    }
}

// MARK: - RaftDB factory

public extension RaftDB {

    /// Begin a new transaction. The caller takes ownership of the
    /// returned ``RaftTransaction`` and must end it with `commit()` or
    /// `rollback()`.
    func beginTransaction() throws -> RaftTransaction {
        try ensureOpen()
        var txnPtr: OpaquePointer?
        let code = rft_transaction_begin(handle, &txnPtr)
        try RaftError.check(code)
        guard let txn = txnPtr else {
            throw RaftError.invalidHandle
        }
        return RaftTransaction(handle: txn)
    }

    /// Run `block` inside a transaction. If `block` throws, the
    /// transaction is rolled back and the error is rethrown. Otherwise
    /// the transaction is committed.
    func withTransaction<T>(_ block: (RaftTransaction) throws -> T) throws -> T {
        let txn = try beginTransaction()
        do {
            let result = try block(txn)
            try txn.commit()
            return result
        } catch {
            txn.rollback()
            throw error
        }
    }
}
