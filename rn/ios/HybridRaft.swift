import Foundation
import NitroModules

// MARK: - C function bindings

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

// MARK: - Observer

private struct Observer {
    let query: String
    let callback: (QueryResult) -> Void
}

// MARK: - HybridRaft

class HybridRaft: HybridRaftSpec {

    private var handle: OpaquePointer?
    private var observers: [String: Observer] = [:]
    private let lock = NSLock()

    // MARK: - Lifecycle

    func open(path: String) throws {
        guard handle == nil else {
            throw NSError(domain: "RaftDB", code: 1,
                          userInfo: [NSLocalizedDescriptionKey: "Database is already open"])
        }
        var errCode: UInt32 = 0
        let ptr = path.withCString { cPath in
            rft_open(cPath, &errCode)
        }
        guard errCode == 0, let ptr = ptr else {
            throw NSError(domain: "RaftDB", code: Int(errCode),
                          userInfo: [NSLocalizedDescriptionKey: "Failed to open database (code \(errCode))"])
        }
        handle = ptr
    }

    func close() throws {
        if let h = handle {
            lock.lock()
            observers.removeAll()
            lock.unlock()
            rft_close(h)
            handle = nil
        }
    }

    deinit {
        if let h = handle {
            rft_close(h)
        }
    }

    // MARK: - CRUD

    func put(key: String, value: String) throws -> Promise<Void> {
        return Promise.async { [self] in
            let h = try ensureOpen()
            let keyData = Data(key.utf8)
            let valData = Data(value.utf8)
            let code = keyData.withUnsafeBytes { keyBuf in
                valData.withUnsafeBytes { valBuf in
                    rft_put(
                        h,
                        keyBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                        keyData.count,
                        valBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                        valData.count
                    )
                }
            }
            guard code == 0 else {
                throw NSError(domain: "RaftDB", code: Int(code),
                              userInfo: [NSLocalizedDescriptionKey: "rft_put failed (code \(code))"])
            }
            notifyObservers(key: key, value: value)
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
            // Read current value before deleting
            let previous = nativeGet(handle: h, key: key)
            let keyData = Data(key.utf8)
            let code = keyData.withUnsafeBytes { keyBuf in
                rft_delete(
                    h,
                    keyBuf.baseAddress!.assumingMemoryBound(to: UInt8.self),
                    keyData.count
                )
            }
            guard code == 0 else {
                throw NSError(domain: "RaftDB", code: Int(code),
                              userInfo: [NSLocalizedDescriptionKey: "rft_delete failed (code \(code))"])
            }
            notifyObservers(key: key, value: nil)
            return previous
        }
    }

    // MARK: - Live Queries

    func watch(query: String, callback: @escaping (QueryResult) -> Void) throws -> String {
        let h = try ensureOpen()
        let subscriptionId = UUID().uuidString

        lock.lock()
        observers[subscriptionId] = Observer(query: query, callback: callback)
        lock.unlock()

        // Emit initial snapshot
        let current = nativeGet(handle: h, key: query)
        callback(QueryResult(key: query, value: current))

        return subscriptionId
    }

    func unwatch(subscriptionId: String) throws {
        lock.lock()
        observers.removeValue(forKey: subscriptionId)
        lock.unlock()
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

            // Phase 1: query required size
            var neededLen = 0
            let sizeCode = rft_get(h, keyPtr, keyData.count, nil, &neededLen)
            // NOT_FOUND = 4
            guard sizeCode != 4 else { return nil }
            // BUFFER_TOO_SMALL = 5 or OK = 0
            guard sizeCode == 5 || sizeCode == 0, neededLen > 0 else { return nil }

            // Phase 2: read into buffer
            var buf = Data(count: neededLen)
            var readLen = neededLen
            let readCode = buf.withUnsafeMutableBytes { bufPtr in
                rft_get(
                    h,
                    keyPtr,
                    keyData.count,
                    bufPtr.baseAddress!.assumingMemoryBound(to: UInt8.self),
                    &readLen
                )
            }
            guard readCode == 0 else { return nil }
            return String(data: buf.prefix(readLen), encoding: .utf8)
        }
    }

    private func notifyObservers(key: String, value: String?) {
        lock.lock()
        let snapshot = observers
        lock.unlock()

        for (_, entry) in snapshot {
            if key.hasPrefix(entry.query) {
                entry.callback(QueryResult(key: key, value: value))
            }
        }
    }
}
