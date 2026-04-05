import type { HybridObject } from 'react-native-nitro-modules'

/**
 * Result emitted by live query observation.
 */
export interface QueryResult {
  key: string
  value: string | undefined
}

/**
 * Native Raft embedded database interface.
 *
 * All methods map to the `rft_*` C symbols exported by libraftdb.
 */
export interface Raft extends HybridObject<{
  ios: 'swift'
  android: 'kotlin'
}> {
  /**
   * Open or create a database at `path`.
   * Must be called before any other operation.
   */
  open(path: string): void

  /**
   * Insert or update a key-value pair.
   */
  put(key: string, value: string): Promise<void>

  /**
   * Look up a key.
   * Returns the value string, or undefined if the key does not exist.
   */
  get(key: string): Promise<string | undefined>

  /**
   * Delete a key. Deleting a non-existent key is not an error.
   */
  delete(key: string): Promise<string | undefined>

  /**
   * Close the database and release the native handle.
   */
  close(): void

  /**
   * Register a live query observer for keys matching `query`.
   * The `callback` fires with a `QueryResult` on every change.
   * Returns an unsubscribe function ID (call `unwatch` with it).
   */
  watch(query: string, callback: (result: QueryResult) => void): string

  /**
   * Remove a previously registered live query observer.
   */
  unwatch(subscriptionId: string): void
}
