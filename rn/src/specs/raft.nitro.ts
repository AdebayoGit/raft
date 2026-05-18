import type { HybridObject } from 'react-native-nitro-modules'

/**
 * Result emitted by the legacy raw-KV `watch` observer.
 */
export interface QueryResult {
  key: string
  value: string | undefined
}

/**
 * Native Raft embedded database interface.
 *
 * Two surfaces:
 *
 * - **Raw KV** — `put`/`get`/`delete`/`watch` operate on string
 *   key/value pairs in the raw key-value namespace. Existing API.
 *
 * - **Typed collections** — `collectionPut*`/`collectionGet`/
 *   `collectionDelete`/`collectionCount`/`collectionListIds`,
 *   `executeQuery`, `transactionBegin`/`transactionGet`/
 *   `transactionPut`/`transactionDelete`/`transactionCommit`/
 *   `transactionRollback`, `observeCollection`/`observeQuery`.
 *   Documents are addressed by `number` doc IDs (uint64 on the
 *   native side; JS Number gives ≈2^53 of usable range, enough for
 *   the foreseeable future of any single device's id space).
 *
 * The two surfaces address different storage namespaces.
 *
 * All methods map to the `rft_*` C symbols exported by libraftdb.
 */
export interface Raft extends HybridObject<{
  ios: 'swift'
  android: 'kotlin'
}> {
  // ── Lifecycle ──────────────────────────────────────────────────────

  /**
   * Open or create a database at `path`. Must be called before any
   * other operation.
   */
  open(path: string): void

  /**
   * Close the database and release the native handle.
   */
  close(): void

  // ── Raw KV ─────────────────────────────────────────────────────────

  /** Insert or update a raw-KV pair. */
  put(key: string, value: string): Promise<void>

  /**
   * Look up a raw-KV key. Returns the value, or undefined if missing.
   */
  get(key: string): Promise<string | undefined>

  /**
   * Delete a raw-KV key. Returns the previous value, or undefined.
   */
  delete(key: string): Promise<string | undefined>

  // ── Typed Collections ──────────────────────────────────────────────

  /**
   * Insert or update a document in `collection`. The JSON's `id`
   * field is honoured as the storage doc id.
   */
  collectionPut(collection: string, documentJson: string): Promise<void>

  /**
   * Insert a document, letting the engine assign a fresh doc id.
   * Returns the assigned id.
   */
  collectionPutAuto(collection: string, documentJson: string): Promise<number>

  /**
   * Fetch a document by id. Returns its JSON, or undefined if not
   * found.
   */
  collectionGet(collection: string, docId: number): Promise<string | undefined>

  /** Delete a document. Not an error if the id does not exist. */
  collectionDelete(collection: string, docId: number): Promise<void>

  /** Number of documents in `collection` (typed namespace). */
  collectionCount(collection: string): Promise<number>

  /** All document ids in `collection`, sorted ascending. */
  collectionListIds(collection: string): Promise<number[]>

  // ── Queries ────────────────────────────────────────────────────────

  /**
   * Execute a predicate query (JSON-encoded) and return each
   * matching document's JSON.
   */
  executeQuery(queryJson: string): Promise<string[]>

  // ── Transactions ───────────────────────────────────────────────────

  /**
   * Begin a transaction. Returns an opaque handle to pass to the
   * other transaction methods. Must be finalised with
   * `transactionCommit` or `transactionRollback`.
   */
  transactionBegin(): Promise<number>

  /** Read inside a transaction. The version is tracked for conflict detection. */
  transactionGet(
    txnHandle: number,
    collection: string,
    docId: number,
  ): Promise<string | undefined>

  /** Buffer a write inside a transaction. Applied atomically at commit. */
  transactionPut(
    txnHandle: number,
    collection: string,
    documentJson: string,
  ): Promise<void>

  /** Buffer a delete inside a transaction. */
  transactionDelete(
    txnHandle: number,
    collection: string,
    docId: number,
  ): Promise<void>

  /**
   * Validate the read set and apply all buffered writes. Consumes
   * the handle. Rejects with a TransactionConflict error if a
   * tracked document was modified concurrently.
   */
  transactionCommit(txnHandle: number): Promise<void>

  /** Discard the transaction. Consumes the handle. */
  transactionRollback(txnHandle: number): Promise<void>

  // ── Observation ────────────────────────────────────────────────────

  /**
   * Register a live query observer for raw-KV keys matching `query`.
   * Returns a subscription id; pass it to `unwatch` to cancel.
   * (Legacy raw-KV API.)
   */
  watch(query: string, callback: (result: QueryResult) => void): string

  /**
   * Register a mutation observer for the typed collection `collection`.
   * Fires with a JSON-encoded `MutationEvent` on every insert / update
   * / delete.
   */
  observeCollection(
    collection: string,
    callback: (eventJson: string) => void,
  ): string

  /**
   * Register a live-query observer. Fires with a JSON-encoded
   * `QueryDiff` immediately (initial snapshot) and again every time
   * the result set changes.
   */
  observeQuery(
    queryJson: string,
    callback: (diffJson: string) => void,
  ): string

  /**
   * Remove a previously registered observer (works for `watch`,
   * `observeCollection`, and `observeQuery`).
   */
  unwatch(subscriptionId: string): void
}
