import { NitroModules } from 'react-native-nitro-modules'
import type { Raft as RaftSpec, QueryResult } from './specs/raft.nitro'

export type { QueryResult }
export { RaftError, RaftErrorCode, raftErrorMessage } from './errors'

const RaftHybrid = NitroModules.createHybridObject<RaftSpec>('Raft')

/**
 * What kind of mutation occurred.
 */
export type MutationKind = 'Insert' | 'Update' | 'Delete'

/**
 * Whether the mutation originated locally or arrived from a network peer.
 */
export type MutationOrigin = 'Local' | 'Remote'

/**
 * A mutation notification emitted by `observeCollection`.
 *
 * The Rust core emits these as JSON over the FFI; the TS layer parses
 * them with the field names left in their Rust form.
 */
export interface MutationEvent {
  collection: string
  doc_id: number
  mutation_type: MutationKind
  origin: MutationOrigin
}

/**
 * The diff between two consecutive live-query result sets.
 *
 * Emitted by `observeQuery`. Each bucket holds parsed document
 * objects (the JSON the engine emits).
 */
export interface QueryDiff<T = unknown> {
  added: T[]
  removed: T[]
  updated: T[]
}

/**
 * A Raft embedded database instance.
 *
 * ```ts
 * const db = RaftDB.open('/path/to/db')
 *
 * // Raw KV
 * await db.put('hello', 'world')
 *
 * // Typed collections
 * const id = await db.collectionPutAuto('users', JSON.stringify({ name: 'Alice' }))
 * const json = await db.collectionGet('users', id) // string | null
 *
 * // Observe
 * const unsubscribe = db.observeCollection('users', (event) => {
 *   console.log(event.mutation_type, event.doc_id)
 * })
 *
 * db.close()
 * ```
 */
export class RaftDB {
  private readonly native: RaftSpec
  private closed = false

  private constructor(native: RaftSpec) {
    this.native = native
  }

  /**
   * Open or create a database at `path`.
   */
  static open(path: string): RaftDB {
    const native = NitroModules.createHybridObject<RaftSpec>('Raft')
    native.open(path)
    return new RaftDB(native)
  }

  // ── Raw KV ──────────────────────────────────────────────────────────

  async put(key: string, value: string): Promise<void> {
    this.ensureOpen()
    await this.native.put(key, value)
  }

  async get(key: string): Promise<string | null> {
    this.ensureOpen()
    const result = await this.native.get(key)
    return result ?? null
  }

  async delete(key: string): Promise<string | null> {
    this.ensureOpen()
    const result = await this.native.delete(key)
    return result ?? null
  }

  // ── Typed Collections ───────────────────────────────────────────────

  /**
   * Insert or update a document. The JSON's `id` field is the storage
   * doc id.
   */
  async collectionPut(collection: string, documentJson: string): Promise<void> {
    this.ensureOpen()
    await this.native.collectionPut(collection, documentJson)
  }

  /**
   * Insert a document, letting the engine assign a fresh id.
   * Returns the assigned id.
   */
  async collectionPutAuto(collection: string, documentJson: string): Promise<number> {
    this.ensureOpen()
    return this.native.collectionPutAuto(collection, documentJson)
  }

  /**
   * Fetch a document by id. Returns the JSON, or `null` if not found.
   */
  async collectionGet(collection: string, docId: number): Promise<string | null> {
    this.ensureOpen()
    const result = await this.native.collectionGet(collection, docId)
    return result ?? null
  }

  /**
   * Delete a document. Not an error if the id does not exist.
   */
  async collectionDelete(collection: string, docId: number): Promise<void> {
    this.ensureOpen()
    await this.native.collectionDelete(collection, docId)
  }

  /** Number of documents in a collection (typed namespace). */
  async collectionCount(collection: string): Promise<number> {
    this.ensureOpen()
    return this.native.collectionCount(collection)
  }

  /** All document ids in a collection, sorted ascending. */
  async collectionListIds(collection: string): Promise<number[]> {
    this.ensureOpen()
    return this.native.collectionListIds(collection)
  }

  // ── Queries ─────────────────────────────────────────────────────────

  /**
   * Execute a predicate query and return each matching document's
   * JSON string. Decode with `JSON.parse` per-element.
   */
  async executeQuery(queryJson: string): Promise<string[]> {
    this.ensureOpen()
    return this.native.executeQuery(queryJson)
  }

  // ── Transactions ────────────────────────────────────────────────────

  /**
   * Run `block` inside a transaction. If it returns normally, the
   * transaction is committed; if it throws, it is rolled back and the
   * error is rethrown.
   */
  async withTransaction<T>(
    block: (txn: RaftTransaction) => Promise<T>,
  ): Promise<T> {
    this.ensureOpen()
    const handle = await this.native.transactionBegin()
    const txn = new RaftTransaction(this.native, handle)
    try {
      const result = await block(txn)
      await txn.commit()
      return result
    } catch (e) {
      await txn.rollback()
      throw e
    }
  }

  // ── Observation ─────────────────────────────────────────────────────

  /**
   * Register a live raw-KV observer for keys matching `query`
   * (legacy API). Returns an unsubscribe function.
   */
  watch(query: string, callback: (result: QueryResult) => void): () => void {
    this.ensureOpen()
    const subscriptionId = this.native.watch(query, callback)
    return () => {
      this.native.unwatch(subscriptionId)
    }
  }

  /**
   * Register a mutation observer for `collection`. The callback fires
   * with a parsed `MutationEvent` on every change. Returns an
   * unsubscribe function.
   */
  observeCollection(
    collection: string,
    callback: (event: MutationEvent) => void,
  ): () => void {
    this.ensureOpen()
    const subscriptionId = this.native.observeCollection(collection, (json) => {
      try {
        callback(JSON.parse(json) as MutationEvent)
      } catch {
        // Drop malformed events
      }
    })
    return () => {
      this.native.unwatch(subscriptionId)
    }
  }

  /**
   * Register a live-query observer. Emits a `QueryDiff` immediately
   * with the initial snapshot, then again every time the result set
   * changes. Returns an unsubscribe function.
   *
   * `queryJson` is the JSON-encoded predicate query.
   */
  observeQuery<T = unknown>(
    queryJson: string,
    callback: (diff: QueryDiff<T>) => void,
  ): () => void {
    this.ensureOpen()
    const subscriptionId = this.native.observeQuery(queryJson, (json) => {
      try {
        callback(JSON.parse(json) as QueryDiff<T>)
      } catch {
        // Drop malformed events
      }
    })
    return () => {
      this.native.unwatch(subscriptionId)
    }
  }

  // ── Lifecycle ───────────────────────────────────────────────────────

  /**
   * Close the database and release the native handle. Safe to call
   * multiple times.
   */
  close(): void {
    if (!this.closed) {
      this.closed = true
      this.native.close()
    }
  }

  get isClosed(): boolean {
    return this.closed
  }

  private ensureOpen(): void {
    if (this.closed) {
      throw new Error('RaftDB is already closed')
    }
  }
}

/**
 * An optimistic-concurrency transaction. Obtain via
 * `RaftDB.withTransaction`. The handle is consumed on `commit()` or
 * `rollback()`.
 */
export class RaftTransaction {
  private consumed = false

  /** @internal */
  constructor(private readonly native: RaftSpec, private readonly handle: number) {}

  async get(collection: string, docId: number): Promise<string | null> {
    this.ensureActive()
    const result = await this.native.transactionGet(this.handle, collection, docId)
    return result ?? null
  }

  async put(collection: string, documentJson: string): Promise<void> {
    this.ensureActive()
    await this.native.transactionPut(this.handle, collection, documentJson)
  }

  async delete(collection: string, docId: number): Promise<void> {
    this.ensureActive()
    await this.native.transactionDelete(this.handle, collection, docId)
  }

  /** @internal — invoked by `RaftDB.withTransaction`. */
  async commit(): Promise<void> {
    if (this.consumed) throw new Error('Transaction already finalised')
    this.consumed = true
    await this.native.transactionCommit(this.handle)
  }

  /** @internal — invoked by `RaftDB.withTransaction` on error. */
  async rollback(): Promise<void> {
    if (this.consumed) return
    this.consumed = true
    await this.native.transactionRollback(this.handle)
  }

  private ensureActive(): void {
    if (this.consumed) {
      throw new Error('Transaction already committed or rolled back')
    }
  }
}

// Re-export the raw Nitro hybrid object for advanced use cases
export { RaftHybrid }
