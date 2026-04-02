import Foundation

/// A typed, collection-scoped wrapper around ``RaftDB``.
///
/// All keys are automatically prefixed with `<name>:` so that
/// multiple collections coexist without key collisions.
/// Documents are encoded/decoded via `Codable` using `JSONEncoder`/`JSONDecoder`.
///
/// ```swift
/// struct User: Codable { let id: String; let name: String }
///
/// let db = try await RaftDB.open(path: path)
/// let users = RaftCollection<User>(db: db, name: "users")
/// try await users.put(id: "1", document: User(id: "1", name: "Alice"))
/// let user = try await users.get(id: "1") // User?
/// ```
public final class RaftCollection<T: Codable>: Sendable where T: Sendable {

    private let db: RaftDB
    public let name: String
    private let encoder: JSONEncoder
    private let decoder: JSONDecoder

    /// Creates a collection wrapper.
    ///
    /// - Parameters:
    ///   - db: The underlying ``RaftDB`` instance.
    ///   - name: The collection name used as key prefix.
    ///   - encoder: JSON encoder (default: `JSONEncoder()`).
    ///   - decoder: JSON decoder (default: `JSONDecoder()`).
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

    // MARK: - CRUD

    /// Insert or update a document by `id`.
    ///
    /// - Throws: ``RaftError`` on native failure, or encoding errors.
    public func put(id: String, document: T) async throws {
        let data = try encoder.encode(document)
        try await db.put(key: scopedKey(id), value: data)
    }

    /// Retrieve a document by `id`.
    ///
    /// - Returns: The decoded document, or `nil` if not found.
    /// - Throws: ``RaftError`` on native failure, or decoding errors.
    public func get(id: String) async throws -> T? {
        guard let data = try await db.get(key: scopedKey(id)) else {
            return nil
        }
        return try decoder.decode(T.self, from: data)
    }

    /// Delete a document by `id`. Deleting a non-existent id is not an error.
    ///
    /// - Throws: ``RaftError`` on native failure.
    public func delete(id: String) async throws {
        try await db.delete(key: scopedKey(id))
    }

    // MARK: - Live Queries

    /// Observe changes to a specific document by `id`.
    ///
    /// Emits the current value immediately, then on every change.
    public func observe(id: String) -> AsyncStream<T?> {
        let dec = decoder
        return AsyncStream { continuation in
            let stream = db.observe(prefix: scopedKey(id))
            let task = Task {
                for await diff in stream {
                    if Task.isCancelled { break }
                    if let data = diff.value {
                        let doc = try? dec.decode(T.self, from: data)
                        continuation.yield(doc)
                    } else {
                        continuation.yield(nil)
                    }
                }
                continuation.finish()
            }
            continuation.onTermination = { @Sendable _ in
                task.cancel()
            }
        }
    }

    /// Observe all changes in this collection.
    public func observeAll() -> AsyncStream<QueryDiff> {
        db.observe(prefix: prefixData)
    }

    // MARK: - Internal

    private var prefixData: Data {
        Data("\(name):".utf8)
    }

    private func scopedKey(_ id: String) -> Data {
        Data("\(name):\(id)".utf8)
    }
}
