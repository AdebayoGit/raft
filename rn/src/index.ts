import { NitroModules } from 'react-native-nitro-modules'
import type { Raft as RaftSpec, QueryResult } from './specs/raft.nitro'

export type { QueryResult }

const RaftHybrid = NitroModules.createHybridObject<RaftSpec>('Raft')

/**
 * A Raft embedded database instance.
 *
 * ```ts
 * const db = RaftDB.open('/path/to/db')
 * await db.put('hello', 'world')
 * const val = await db.get('hello') // 'world'
 * await db.delete('hello')
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

  /**
   * Insert or update a key-value pair.
   */
  async put(key: string, value: string): Promise<void> {
    this.ensureOpen()
    await this.native.put(key, value)
  }

  /**
   * Look up a key.
   * Returns the value string, or `null` if the key does not exist.
   */
  async get(key: string): Promise<string | null> {
    this.ensureOpen()
    const result = await this.native.get(key)
    return result ?? null
  }

  /**
   * Delete a key. Returns the previous value, or `null` if it didn't exist.
   */
  async delete(key: string): Promise<string | null> {
    this.ensureOpen()
    const result = await this.native.delete(key)
    return result ?? null
  }

  /**
   * Close the database and release the native handle.
   * Safe to call multiple times.
   */
  close(): void {
    if (!this.closed) {
      this.closed = true
      this.native.close()
    }
  }

  /**
   * Register a live query observer for keys matching `query`.
   *
   * @param query - Key prefix to observe.
   * @param callback - Called with a `QueryResult` on every matching change.
   * @returns An unsubscribe function.
   */
  watch(query: string, callback: (result: QueryResult) => void): () => void {
    this.ensureOpen()
    const subscriptionId = this.native.watch(query, callback)
    return () => {
      this.native.unwatch(subscriptionId)
    }
  }

  /**
   * Whether this database handle has been closed.
   */
  get isClosed(): boolean {
    return this.closed
  }

  private ensureOpen(): void {
    if (this.closed) {
      throw new Error('RaftDB is already closed')
    }
  }
}

// Re-export the raw Nitro hybrid object for advanced use cases
export { RaftHybrid }
