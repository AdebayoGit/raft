import Foundation

/// A typed wrapper around ``RaftDB`` with two complementary surfaces:
///
/// 1. **String-id raw-KV facade** — `put(id:document:)`, `get(id:)`,
///    `delete(id:)`. Documents are stored under `<name>:<id>` raw KV keys.
///    Useful when you control the id space (e.g. UUIDs, slugs).
///
/// 2. **Typed-FFI facade** — `putAuto(_:)`, `put(docId:document:)`,
///    `get(docId:)`, `delete(docId:)`, `count()`, `listIds()`,
///    `observe()`. Backed by Raft's typed collection store; document
///    ids are 64-bit unsigned integers assigned by the engine (or chosen
///    by the caller).
///
/// The two facades address **different storage namespaces**. Putting a
/// document via the String-id facade and reading it via the typed
/// facade will not work, and vice versa. Pick one per collection.
///
/// ```swift
/// struct User: Codable, Sendable { let id: UInt64; let name: String }
///
/// let db = try await RaftDB.open(path: path)
/// let users = RaftCollection<User>(db: db, name: "users")
///
/// // Typed facade
/// let id = try await users.putAuto(User(id: 0, name: "Alice"))
/// let alice = try await users.get(docId: id)
/// ```
public final class RaftCollection<T: Codable>: Sendable where T: Sendable {

    private let db: RaftDB
    public let name: String
    private let encoder: JSONEncoder
    private let decoder: JSONDecoder

    public init(
        db: RaftDB,
        name: String,
        encoder: JSONEncoder = JSONEncoder(),
        decoder: JSONDecoder = JSONDecoder()
    ) {
        self.db = db
        self.name = name
        self.encoder = encoder
        self.decoder = decoder
    }

    // MARK: - String-id raw-KV facade

    /// Insert or update a document under the String key `<name>:<id>`.
    public func put(id: String, document: T) async throws {
        let data = try encoder.encode(document)
        try await db.put(key: scopedKey(id), value: data)
    }

    /// Retrieve a document by String id from the raw-KV namespace.
    /// Returns `nil` if not found.
    public func get(id: String) async throws -> T? {
        guard let data = try await db.get(key: scopedKey(id)) else {
            return nil
        }
        return try decoder.decode(T.self, from: data)
    }

    /// Delete a document by String id from the raw-KV namespace. Not an
    /// error if the id does not exist.
    public func delete(id: String) async throws {
        try await db.delete(key: scopedKey(id))
    }

    // MARK: - Typed-FFI facade

    /// Insert a document into the typed collection store, letting the
    /// engine assign a fresh `UInt64` id. Returns the assigned id.
    public func putAuto(_ document: T) async throws -> UInt64 {
        let data = try encoder.encode(document)
        return try await db.collectionPutAuto(name, document: data)
    }

    /// Insert or update a document at the given `UInt64` id. The
    /// document's `id` field must equal `docId`.
    public func put(docId: UInt64, document: T) async throws {
        let data = try encoder.encode(document)
        try await db.collectionPut(name, document: data)
        _ = docId  // declared in signature to match call ergonomics with get/delete
    }

    /// Retrieve a document by `UInt64` id from the typed collection
    /// store. Returns `nil` if not found.
    public func get(docId: UInt64) async throws -> T? {
        guard let data = try await db.collectionGet(name, id: docId) else {
            return nil
        }
        return try decoder.decode(T.self, from: data)
    }

    /// Delete a document by `UInt64` id. Not an error if the id does
    /// not exist.
    public func delete(docId: UInt64) async throws {
        try await db.collectionDelete(name, id: docId)
    }

    /// Number of documents currently in this collection (typed
    /// namespace only).
    public func count() async throws -> Int {
        try await db.collectionCount(name)
    }

    /// All document ids currently in this collection (typed namespace),
    /// sorted ascending.
    public func listIds() async throws -> [UInt64] {
        try await db.collectionListIds(name)
    }

    // MARK: - Observation

    /// Observe every insert / update / delete on this collection
    /// (typed namespace). Yields a ``MutationEvent`` per change.
    public func observe() -> AsyncStream<MutationEvent> {
        db.observe(collection: name)
    }

    /// Observe a live query whose JSON encoding is `queryJson`. Yields
    /// a ``QueryDiff`` immediately with the initial snapshot and again
    /// every time the result set changes.
    public func liveQuery(_ queryJson: Data) -> AsyncStream<QueryDiff> {
        db.observeQuery(queryJson)
    }

    // MARK: - Internal

    private func scopedKey(_ id: String) -> Data {
        Data("\(name):\(id)".utf8)
    }
}
