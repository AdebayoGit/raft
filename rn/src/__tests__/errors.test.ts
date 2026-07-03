import { RaftError, RaftErrorCode, raftErrorMessage } from '../errors'

// ---------------------------------------------------------------------------
// Error-taxonomy conformance — codes mirror core/include/raft.h
// ---------------------------------------------------------------------------

describe('RaftErrorCode', () => {
  it('matches the canonical RftError codes from raft.h', () => {
    expect(RaftErrorCode.Ok).toBe(0)
    expect(RaftErrorCode.NullPointer).toBe(1)
    expect(RaftErrorCode.InvalidUtf8).toBe(2)
    expect(RaftErrorCode.IoError).toBe(3)
    expect(RaftErrorCode.NotFound).toBe(4)
    expect(RaftErrorCode.BufferTooSmall).toBe(5)
    expect(RaftErrorCode.InvalidJson).toBe(6)
    expect(RaftErrorCode.TransactionConflict).toBe(7)
    expect(RaftErrorCode.InvalidHandle).toBe(8)
    expect(RaftErrorCode.UnknownSubscription).toBe(9)
    expect(RaftErrorCode.InternalPanic).toBe(10)
    expect(RaftErrorCode.InvalidPath).toBe(11)
    expect(RaftErrorCode.DartApiNotInitialized).toBe(12)
    expect(RaftErrorCode.PayloadTooLarge).toBe(13)
    expect(RaftErrorCode.UnsupportedVersion).toBe(14)
  })
})

describe('raftErrorMessage', () => {
  it.each([
    [1, 'null'],
    [2, 'UTF-8'],
    [3, 'I/O'],
    [4, 'not found'],
    [5, 'buffer'],
    [6, 'JSON'],
    [7, 'conflict'],
    [8, 'handle'],
    [9, 'ubscription'],
    [10, 'panic'],
    [11, 'path'],
    [12, 'Dart API'],
    [13, 'size cap'],
    [14, 'version'],
  ])('code %i message mentions "%s"', (code, fragment) => {
    expect(raftErrorMessage(code)).toContain(fragment)
  })

  it('falls back to a generic message including the code', () => {
    expect(raftErrorMessage(99)).toContain('99')
    expect(raftErrorMessage(99).toLowerCase()).toContain('unknown')
  })
})

describe('RaftError', () => {
  it('carries the code and the canonical message', () => {
    const error = new RaftError(RaftErrorCode.TransactionConflict)
    expect(error.code).toBe(7)
    expect(error.message).toContain('conflict')
    expect(error.name).toBe('RaftError')
    expect(error).toBeInstanceOf(Error)
  })

  it('every known code round-trips through the constructor', () => {
    for (let code = 1; code <= 14; code++) {
      const error = new RaftError(code)
      expect(error.code).toBe(code)
      expect(error.message).not.toContain('Unknown raft error code')
    }
  })

  it('fromNative recovers the code from a native "(code N)" message', () => {
    const native = new Error('collectionPut failed (code 13)')
    const wrapped = RaftError.fromNative(native)
    expect(wrapped).toBeInstanceOf(RaftError)
    expect((wrapped as RaftError).code).toBe(13)
    expect((wrapped as RaftError).message).toContain('size cap')
  })

  it('fromNative recovers the code from an Android "with code N" message', () => {
    const native = new Error('rft_transaction_commit failed with code 7')
    const wrapped = RaftError.fromNative(native)
    expect(wrapped).toBeInstanceOf(RaftError)
    expect((wrapped as RaftError).code).toBe(7)
    expect((wrapped as RaftError).message).toContain('conflict')
  })

  it('fromNative passes through errors without a code marker', () => {
    const native = new Error('Database is already open')
    expect(RaftError.fromNative(native)).toBe(native)
  })

  it('fromNative passes through non-Error values', () => {
    expect(RaftError.fromNative('boom')).toBe('boom')
  })
})
