/**
 * The canonical Raft error taxonomy, mirroring the C `RftError` enum in
 * `core/include/raft.h`. Keep this in sync when new `RftError` variants
 * are added on the Rust side.
 */
export enum RaftErrorCode {
  Ok = 0,
  NullPointer = 1,
  InvalidUtf8 = 2,
  IoError = 3,
  NotFound = 4,
  BufferTooSmall = 5,
  InvalidJson = 6,
  TransactionConflict = 7,
  InvalidHandle = 8,
  UnknownSubscription = 9,
  InternalPanic = 10,
  InvalidPath = 11,
  DartApiNotInitialized = 12,
  PayloadTooLarge = 13,
  UnsupportedVersion = 14,
}

const MESSAGES: Record<number, string> = {
  [RaftErrorCode.NullPointer]: 'A required pointer argument was null',
  [RaftErrorCode.InvalidUtf8]: 'A string argument was not valid UTF-8',
  [RaftErrorCode.IoError]: 'An I/O or storage engine error occurred',
  [RaftErrorCode.NotFound]: 'The requested key was not found',
  [RaftErrorCode.BufferTooSmall]: 'The caller-provided buffer is too small',
  [RaftErrorCode.InvalidJson]:
    'A document or filter passed via JSON failed to parse',
  [RaftErrorCode.TransactionConflict]:
    'Transaction commit conflicted with a concurrent write',
  [RaftErrorCode.InvalidHandle]:
    'Native handle is invalid or already consumed',
  [RaftErrorCode.UnknownSubscription]: 'Subscription id is not registered',
  [RaftErrorCode.InternalPanic]:
    'Internal panic in native core; close and reopen the database',
  [RaftErrorCode.InvalidPath]:
    'Invalid database path (empty, contains "..", or escapes the confinement root)',
  [RaftErrorCode.DartApiNotInitialized]:
    'Dart API not initialized (rft_dart_init was not called)',
  [RaftErrorCode.PayloadTooLarge]: 'JSON payload exceeds its size cap',
  [RaftErrorCode.UnsupportedVersion]:
    'JSON envelope declared an unsupported schema version',
}

/**
 * Human-readable message for a raw `RftError` code. Unknown codes fall
 * back to a generic message that includes the code.
 */
export function raftErrorMessage(code: number): string {
  return MESSAGES[code] ?? `Unknown raft error code: ${code}`
}

/**
 * A typed error carrying the canonical `RftError` code.
 *
 * The native layers (`HybridRaft.swift` / `HybridRaft.kt`) throw errors
 * whose message embeds the raw code as `(code N)`; use
 * {@link RaftError.fromNative} to recover the taxonomy from them.
 */
export class RaftError extends Error {
  readonly code: number

  constructor(code: number) {
    super(raftErrorMessage(code))
    this.name = 'RaftError'
    this.code = code
  }

  /**
   * Wrap an error thrown by the native layer. If its message embeds a
   * code marker — `(code N)` on iOS or `with code N` on Android — the
   * returned `RaftError` carries that code and the canonical message;
   * otherwise the original error is returned unchanged.
   */
  static fromNative(error: unknown): unknown {
    if (error instanceof Error) {
      const match = error.message.match(/\bcode (\d+)\)?\s*$/)
      if (match) {
        return new RaftError(Number(match[1]))
      }
    }
    return error
  }
}
