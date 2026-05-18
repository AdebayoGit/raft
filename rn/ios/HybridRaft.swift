import Foundation
import NitroModules

// MARK: - C function bindings (matches core/include/raft.h)

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

@_silgen_name("rft_transaction_begin")
private func rft_transaction_begin(_ db: OpaquePointer,
                                   _ outTxn: UnsafeMutablePointer<OpaquePointer?>) -> UInt32

@_silgen_name("rft_transaction_get")
private func rft_transaction_get(_ txn: OpaquePointer,
                                 _ collection: UnsafePointer<CChar>,
                                 _ docId: UInt64,
                                 _ outBuf: UnsafeMutablePointer<UInt8>?,
                                 _ outLen: UnsafeMutablePointer<Int>) -> UInt32

@_silgen_name("rft_transaction_put")
private func rft_transaction_put(_ txn: OpaquePointer,
                                 _ collection: UnsafePointer<CChar>,
                                 _ docJson: UnsafePointer<UInt8>,
                                 _ docJsonLen: Int) -> UInt32

@_silgen_name("rft_transaction_delete")
private func rft_transaction_delete(_ txn: OpaquePointer,
                                    _ collection: UnsafePointer<CChar>,
                                    _ docId: UInt64) -> UInt32

@_silgen_name("rft_transaction_commit")
private func rft_transaction_commit(_ txn: OpaquePointer) -> UInt32

@_silgen_name("rft_transaction_rollback")
private func rft_transaction_rollback(_ txn: OpaquePointer)

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

// MARK: - Helpers

private func raftError(_ code: UInt32, _ op: String) -> NSError {
    NSError(domain: "RaftDB", code: Int(code),
            userInfo: [NSLocalizedDescriptionKey: "\(op) failed (code \(code))"])
}

// MARK: - Observer context (typed-FFI callbacks)

private final class JsCallbackContext {
    let handler: (String) -> Void
    var subId: UInt64 = 0
    init(handler: @escaping (String) -> Void) {
        self.handler = handler
    }
}

private let raftObserveTrampoline: RftObserveCallback = { eventJson, userData in
    guard let eventJson, let userData else { return }
    let ctx = Unmanaged<JsCallbackContext>.fromOpaque(userData).takeUnretainedValue()
    let s = String(cString: eventJson)
    ctx.handler(s)
}

// MARK: - HybridRaft

class HybridRaft: HybridRaftSpec {

    private var handle: OpaquePointer?
    private let lock = NSLock()

    // Legacy raw-KV observers (string-prefix matching).
    private struct KvObserver {
        let query: String
        let callback: (QueryResult) -> Void
    }
    private var kvObservers: [String: KvObserver] = [:]

    // Typed-FFI observers — each holds a retained JsCallbackContext that
    // owns the JS callback closure. Keyed by subscription id (string).
    private var typedObservers: [String: JsCallbackContext] = [:]

    // MARK: - Lifecycle

    func open(path: String) throws {
        guard handle == nil else {
            throw NSError(domain: "RaftDB", code: 1,
                          userInfo: [NSLocalizedDescriptionKey: "Database is already open"])
        }
        var errCode: UInt32 = 0
        let ptr = path.withCString { cPath in rft_open(cPath, &errCode) }
        guard errCode == 0, let ptr = ptr else {
            throw raftError(errCode, "open")
        }
        handle = ptr
    }

    func close() throws {
        if let h = handle {
            lock.lock()
            kvObservers.removeAll()
            // Native subscriptions are cancelled by rft_close. Release the
            // JS callback contexts on the Swift side.
            for (_, ctx) in typedObservers {
                _ = ctx // retained refs released by removeAll
            }
            typedObservers.removeAll()
            lock.unlock()
            rft_close(h)
            handle = nil
        }
    }

    deinit {
        if let h = handle { rft_close(h) }
    }

    // MARK: - Raw KV

    func put(key: String, value: String) throws -> Promise<Void> {
        return Promise.async { [self] in
            let h = try ensureOpen()
            let keyData = Data(key.utf8)
            let valData = Data(value.utf8)
            let code = keyData.withUnsafeBytes { keyBuf in
                valData.withUnsafeBytes { valBuf in
                    rft_put(h,
                            keyBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                            keyData.count,
                            valBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                            valData.count)
                }
            }
            guard code == 0 else { throw raftError(code, "put") }
            notifyKvObservers(key: key, value: value)
        }
    }

    func get(key: String) throws -> Promise<String?> {
        return Promise.async { [self] in
            let h = try ensureOpen()
            return nativeGet(handle: h, key: key)
        }
    }

    func delete(key: String) throws -> Promise<String?> {
        return Promise.async { [self] in
            let h = try ensureOpen()
            let previous = nativeGet(handle: h, key: key)
            let keyData = Data(key.utf8)
            let code = keyData.withUnsafeBytes { keyBuf in
                rft_delete(h,
                           keyBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                           keyData.count)
            }
            guard code == 0 else { throw raftError(code, "delete") }
            notifyKvObservers(key: key, value: nil)
            return previous
        }
    }

    // MARK: - Typed Collections

    func collectionPut(collection: String, documentJson: String) throws -> Promise<Void> {
        return Promise.async { [self] in
            let h = try ensureOpen()
            let json = Data(documentJson.utf8)
            let code = collection.withCString { cName in
                json.withUnsafeBytes { jsonBuf in
                    rft_collection_put(h, cName,
                                       jsonBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                                       json.count)
                }
            }
            guard code == 0 else { throw raftError(code, "collectionPut") }
        }
    }

    func collectionPutAuto(collection: String, documentJson: String) throws -> Promise<Double> {
        return Promise.async { [self] in
            let h = try ensureOpen()
            let json = Data(documentJson.utf8)
            var outId: UInt64 = 0
            let code = collection.withCString { cName in
                json.withUnsafeBytes { jsonBuf in
                    rft_collection_put_auto(h, cName,
                                            jsonBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                                            json.count, &outId)
                }
            }
            guard code == 0 else { throw raftError(code, "collectionPutAuto") }
            return Double(outId)
        }
    }

    func collectionGet(collection: String, docId: Double) throws -> Promise<String?> {
        return Promise.async { [self] in
            let h = try ensureOpen()
            return collection.withCString { cName -> String? in
                var needed = 0
                let sizeCode = rft_collection_get(h, cName, UInt64(docId), nil, &needed)
                if sizeCode == 4 { return nil }
                guard sizeCode == 5 || sizeCode == 0 else { return nil }
                if needed == 0 { return "" }
                var buf = Data(count: needed)
                var actual = needed
                let readCode = buf.withUnsafeMutableBytes { bufPtr in
                    rft_collection_get(h, cName, UInt64(docId),
                                       bufPtr.baseAddress!.assumingMemoryBound(to: UInt8.self),
                                       &actual)
                }
                guard readCode == 0 else { return nil }
                return String(data: buf.prefix(actual), encoding: .utf8)
            }
        }
    }

    func collectionDelete(collection: String, docId: Double) throws -> Promise<Void> {
        return Promise.async { [self] in
            let h = try ensureOpen()
            let code = collection.withCString { cName in
                rft_collection_delete(h, cName, UInt64(docId))
            }
            guard code == 0 else { throw raftError(code, "collectionDelete") }
        }
    }

    func collectionCount(collection: String) throws -> Promise<Double> {
        return Promise.async { [self] in
            let h = try ensureOpen()
            var count = 0
            let code = collection.withCString { cName in
                rft_collection_count(h, cName, &count)
            }
            guard code == 0 else { throw raftError(code, "collectionCount") }
            return Double(count)
        }
    }

    func collectionListIds(collection: String) throws -> Promise<[Double]> {
        return Promise.async { [self] in
            let h = try ensureOpen()
            return try collection.withCString { cName -> [Double] in
                var needed = 0
                let sizeCode = rft_collection_list_ids(h, cName, nil, &needed)
                if sizeCode != 5 && sizeCode != 0 {
                    throw raftError(sizeCode, "collectionListIds")
                }
                if needed == 0 { return [] }
                var ids = [UInt64](repeating: 0, count: needed)
                var actual = needed
                let readCode = ids.withUnsafeMutableBufferPointer { buf -> UInt32 in
                    rft_collection_list_ids(h, cName, buf.baseAddress, &actual)
                }
                guard readCode == 0 else { throw raftError(readCode, "collectionListIds") }
                return ids.prefix(actual).map { Double($0) }
            }
        }
    }

    // MARK: - Queries

    func executeQuery(queryJson: String) throws -> Promise<[String]> {
        return Promise.async { [self] in
            let h = try ensureOpen()
            let json = Data(queryJson.utf8)
            return try json.withUnsafeBytes { qBuf -> [String] in
                var resultPtr: OpaquePointer?
                let execCode = rft_query_execute(h,
                                                  qBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                                                  json.count, &resultPtr)
                guard execCode == 0 else { throw raftError(execCode, "executeQuery") }
                guard let result = resultPtr else { return [] }
                defer { rft_query_result_free(result) }
                let count = rft_query_result_count(result)
                var docs = [String]()
                docs.reserveCapacity(count)
                for i in 0..<count {
                    var needed = 0
                    let sizeCode = rft_query_result_get(result, i, nil, &needed)
                    guard sizeCode == 5 || sizeCode == 0 else {
                        throw raftError(sizeCode, "queryResultGet")
                    }
                    var buf = Data(count: needed)
                    var actual = needed
                    let readCode = buf.withUnsafeMutableBytes { p in
                        rft_query_result_get(result, i,
                                             p.baseAddress!.assumingMemoryBound(to: UInt8.self),
                                             &actual)
                    }
                    guard readCode == 0 else { throw raftError(readCode, "queryResultGet") }
                    if let s = String(data: buf.prefix(actual), encoding: .utf8) {
                        docs.append(s)
                    }
                }
                return docs
            }
        }
    }

    // MARK: - Transactions

    func transactionBegin() throws -> Promise<Double> {
        return Promise.async { [self] in
            let h = try ensureOpen()
            var txnPtr: OpaquePointer?
            let code = rft_transaction_begin(h, &txnPtr)
            guard code == 0, let txn = txnPtr else {
                throw raftError(code, "transactionBegin")
            }
            // Bit-cast OpaquePointer to UInt64 then to Double. Up to
            // 2^53 of address space is preserved precisely (sufficient
            // for typical iOS pointer values which are 48-bit).
            let addr = UInt(bitPattern: txn)
            return Double(addr)
        }
    }

    func transactionGet(txnHandle: Double, collection: String, docId: Double) throws -> Promise<String?> {
        return Promise.async {
            guard let txn = OpaquePointer(bitPattern: UInt(txnHandle)) else {
                throw raftError(8, "transactionGet") // INVALID_HANDLE
            }
            return collection.withCString { cName -> String? in
                var needed = 0
                let sizeCode = rft_transaction_get(txn, cName, UInt64(docId), nil, &needed)
                if sizeCode == 4 { return nil }
                guard sizeCode == 5 || sizeCode == 0 else { return nil }
                if needed == 0 { return "" }
                var buf = Data(count: needed)
                var actual = needed
                let readCode = buf.withUnsafeMutableBytes { bufPtr in
                    rft_transaction_get(txn, cName, UInt64(docId),
                                        bufPtr.baseAddress!.assumingMemoryBound(to: UInt8.self),
                                        &actual)
                }
                guard readCode == 0 else { return nil }
                return String(data: buf.prefix(actual), encoding: .utf8)
            }
        }
    }

    func transactionPut(txnHandle: Double, collection: String, documentJson: String) throws -> Promise<Void> {
        return Promise.async {
            guard let txn = OpaquePointer(bitPattern: UInt(txnHandle)) else {
                throw raftError(8, "transactionPut")
            }
            let json = Data(documentJson.utf8)
            let code = collection.withCString { cName in
                json.withUnsafeBytes { jsonBuf in
                    rft_transaction_put(txn, cName,
                                        jsonBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                                        json.count)
                }
            }
            guard code == 0 else { throw raftError(code, "transactionPut") }
        }
    }

    func transactionDelete(txnHandle: Double, collection: String, docId: Double) throws -> Promise<Void> {
        return Promise.async {
            guard let txn = OpaquePointer(bitPattern: UInt(txnHandle)) else {
                throw raftError(8, "transactionDelete")
            }
            let code = collection.withCString { cName in
                rft_transaction_delete(txn, cName, UInt64(docId))
            }
            guard code == 0 else { throw raftError(code, "transactionDelete") }
        }
    }

    func transactionCommit(txnHandle: Double) throws -> Promise<Void> {
        return Promise.async {
            guard let txn = OpaquePointer(bitPattern: UInt(txnHandle)) else {
                throw raftError(8, "transactionCommit")
            }
            let code = rft_transaction_commit(txn)
            guard code == 0 else { throw raftError(code, "transactionCommit") }
        }
    }

    func transactionRollback(txnHandle: Double) throws -> Promise<Void> {
        return Promise.async {
            guard let txn = OpaquePointer(bitPattern: UInt(txnHandle)) else { return }
            rft_transaction_rollback(txn)
        }
    }

    // MARK: - Observation

    func watch(query: String, callback: @escaping (QueryResult) -> Void) throws -> String {
        let h = try ensureOpen()
        let subscriptionId = UUID().uuidString
        lock.lock()
        kvObservers[subscriptionId] = KvObserver(query: query, callback: callback)
        lock.unlock()
        let current = nativeGet(handle: h, key: query)
        callback(QueryResult(key: query, value: current))
        return subscriptionId
    }

    func observeCollection(collection: String, callback: @escaping (String) -> Void) throws -> String {
        let h = try ensureOpen()
        let subscriptionId = UUID().uuidString
        let ctx = JsCallbackContext(handler: callback)
        let userData = Unmanaged.passUnretained(ctx).toOpaque()
        var subId: UInt64 = 0
        let code = collection.withCString { cName in
            rft_observe(h, cName, raftObserveTrampoline, userData, &subId)
        }
        guard code == 0 else { throw raftError(code, "observeCollection") }
        ctx.subId = subId
        lock.lock()
        typedObservers[subscriptionId] = ctx
        lock.unlock()
        return subscriptionId
    }

    func observeQuery(queryJson: String, callback: @escaping (String) -> Void) throws -> String {
        let h = try ensureOpen()
        let subscriptionId = UUID().uuidString
        let ctx = JsCallbackContext(handler: callback)
        let userData = Unmanaged.passUnretained(ctx).toOpaque()
        let json = Data(queryJson.utf8)
        var subId: UInt64 = 0
        let code = json.withUnsafeBytes { qBuf in
            rft_observe_query(h,
                              qBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                              json.count,
                              raftObserveTrampoline, userData, &subId)
        }
        guard code == 0 else { throw raftError(code, "observeQuery") }
        ctx.subId = subId
        lock.lock()
        typedObservers[subscriptionId] = ctx
        lock.unlock()
        return subscriptionId
    }

    func unwatch(subscriptionId: String) throws {
        var ctxToRelease: JsCallbackContext?
        lock.lock()
        if kvObservers.removeValue(forKey: subscriptionId) != nil {
            // Legacy raw-KV observer — no native subscription to cancel.
        } else if let ctx = typedObservers.removeValue(forKey: subscriptionId) {
            ctxToRelease = ctx
        }
        lock.unlock()
        if let ctx = ctxToRelease, let h = handle {
            _ = rft_unobserve(h, ctx.subId)
        }
    }

    // MARK: - Internal

    private func ensureOpen() throws -> OpaquePointer {
        guard let h = handle else {
            throw NSError(domain: "RaftDB", code: 0,
                          userInfo: [NSLocalizedDescriptionKey: "Database is not open"])
        }
        return h
    }

    private func nativeGet(handle h: OpaquePointer, key: String) -> String? {
        let keyData = Data(key.utf8)
        return keyData.withUnsafeBytes { keyBuf -> String? in
            let keyPtr = keyBuf.baseAddress!.assumingMemoryBound(to: UInt8.self)
            var neededLen = 0
            let sizeCode = rft_get(h, keyPtr, keyData.count, nil, &neededLen)
            guard sizeCode != 4 else { return nil }
            guard sizeCode == 5 || sizeCode == 0 else { return nil }
            if neededLen == 0 { return "" }
            var buf = Data(count: neededLen)
            var readLen = neededLen
            let readCode = buf.withUnsafeMutableBytes { bufPtr in
                rft_get(h, keyPtr, keyData.count,
                        bufPtr.baseAddress!.assumingMemoryBound(to: UInt8.self),
                        &readLen)
            }
            guard readCode == 0 else { return nil }
            return String(data: buf.prefix(readLen), encoding: .utf8)
        }
    }

    private func notifyKvObservers(key: String, value: String?) {
        lock.lock()
        let snapshot = kvObservers
        lock.unlock()
        for (_, entry) in snapshot {
            if key.hasPrefix(entry.query) {
                entry.callback(QueryResult(key: key, value: value))
            }
        }
    }
}
