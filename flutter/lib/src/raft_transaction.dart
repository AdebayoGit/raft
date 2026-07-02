import 'dart:ffi' as ffi;
import 'dart:isolate';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'raft_db.dart';
import 'raft_db_bindings.dart' as bindings;

/// An optimistic-concurrency transaction over a Raft database.
///
/// Begin via [RaftDb.beginTransaction], read and buffer writes, then
/// call [commit] or [rollback]. The handle is consumed by either
/// terminator and must not be reused afterwards.
///
/// At commit time, the engine validates that every document read inside
/// the transaction has the same version it had when read. If any
/// tracked document was modified concurrently, [commit] throws
/// [RaftDbException] with code 7 (`TransactionConflict`) and no writes
/// are applied.
///
/// ```dart
/// final txn = await db.beginTransaction();
/// try {
///   final raw = await txn.get('users', 42);
///   // ...mutate raw...
///   await txn.put('users', raw!);
///   await txn.commit();
/// } catch (e) {
///   await txn.rollback();
///   rethrow;
/// }
/// ```
class RaftTransaction {
  RaftTransaction._(this._address, this._libLoader);

  /// The native pointer address, passed across isolate boundaries as int.
  final int _address;
  final ffi.DynamicLibrary Function() _libLoader;
  bool _consumed = false;

  /// Serializes every native call on this handle. Each operation is
  /// chained onto the previous one, so [commit] / [rollback] cannot free
  /// the native transaction while an unawaited [get] / [put] / [delete]
  /// is still running in its worker isolate (use-after-free).
  Future<void> _queue = Future<void>.value();

  /// Enqueue [op] after all previously started operations. Errors from
  /// earlier operations do not poison the chain — each caller only sees
  /// its own operation's error.
  Future<T> _serialize<T>(Future<T> Function() op) {
    final result = _queue.then((_) => op());
    _queue = result.then<void>((_) {}, onError: (_) {});
    return result;
  }

  /// Read a document by id. The read is tracked for conflict detection
  /// at commit time. Returns `null` when the document does not exist.
  Future<Uint8List?> get(String collection, int docId) {
    _assertActive();
    final addr = _address;
    final loader = _libLoader;
    return _serialize(
      () => Isolate.run(() {
        final db = bindings.RaftDbBindings(loader());
        final txn = ffi.Pointer<bindings.RaftTransaction>.fromAddress(addr);
        final cName = collection.toNativeUtf8();
        final lenPtr = calloc<ffi.UintPtr>();
        try {
          final sizeCode = db.rft_transaction_get(
            txn,
            cName.cast(),
            docId,
            ffi.nullptr,
            lenPtr,
          );
          if (sizeCode == bindings.RftError.RFT_ERROR_NOT_FOUND.value) {
            return null;
          }
          if (sizeCode != bindings.RftError.RFT_ERROR_BUFFER_TOO_SMALL.value &&
              sizeCode != bindings.RftError.RFT_ERROR_OK.value) {
            throw RaftDbException.fromCode(sizeCode);
          }
          final needed = lenPtr.value;
          final bufPtr = malloc<ffi.Uint8>(needed);
          try {
            final readCode = db.rft_transaction_get(
              txn,
              cName.cast(),
              docId,
              bufPtr,
              lenPtr,
            );
            if (readCode != bindings.RftError.RFT_ERROR_OK.value) {
              throw RaftDbException.fromCode(readCode);
            }
            return Uint8List.fromList(bufPtr.asTypedList(lenPtr.value));
          } finally {
            malloc.free(bufPtr);
          }
        } finally {
          malloc.free(cName);
          calloc.free(lenPtr);
        }
      }),
    );
  }

  /// Buffer a write inside the transaction. Applied atomically on commit.
  /// `documentJson` must be a UTF-8 encoded JSON object; its `id` field
  /// is the storage document id.
  Future<void> put(String collection, Uint8List documentJson) {
    _assertActive();
    final addr = _address;
    final loader = _libLoader;
    return _serialize(
      () => Isolate.run(() {
        final db = bindings.RaftDbBindings(loader());
        final txn = ffi.Pointer<bindings.RaftTransaction>.fromAddress(addr);
        final cName = collection.toNativeUtf8();
        final jsonPtr = malloc<ffi.Uint8>(documentJson.length);
        try {
          jsonPtr.asTypedList(documentJson.length).setAll(0, documentJson);
          final code = db.rft_transaction_put(
            txn,
            cName.cast(),
            jsonPtr,
            documentJson.length,
          );
          if (code != bindings.RftError.RFT_ERROR_OK.value) {
            throw RaftDbException.fromCode(code);
          }
        } finally {
          malloc.free(cName);
          malloc.free(jsonPtr);
        }
      }),
    );
  }

  /// Buffer a delete inside the transaction.
  Future<void> delete(String collection, int docId) {
    _assertActive();
    final addr = _address;
    final loader = _libLoader;
    return _serialize(
      () => Isolate.run(() {
        final db = bindings.RaftDbBindings(loader());
        final txn = ffi.Pointer<bindings.RaftTransaction>.fromAddress(addr);
        final cName = collection.toNativeUtf8();
        try {
          final code = db.rft_transaction_delete(txn, cName.cast(), docId);
          if (code != bindings.RftError.RFT_ERROR_OK.value) {
            throw RaftDbException.fromCode(code);
          }
        } finally {
          malloc.free(cName);
        }
      }),
    );
  }

  /// Validate the read set and atomically apply all buffered writes.
  /// Consumes the handle — calling [commit] or [rollback] again is a
  /// no-op (and the transaction is then unusable).
  ///
  /// Throws [RaftDbException] with code 7 if a tracked document was
  /// modified concurrently. No writes are applied.
  Future<void> commit() {
    _assertActive();
    // Mark consumed synchronously so no new operations can be enqueued;
    // the commit itself is serialized behind any in-flight operations so
    // the native handle is never freed while another isolate is using it.
    _consumed = true;
    final addr = _address;
    final loader = _libLoader;
    return _serialize(
      () => Isolate.run(() {
        final db = bindings.RaftDbBindings(loader());
        final txn = ffi.Pointer<bindings.RaftTransaction>.fromAddress(addr);
        final code = db.rft_transaction_commit(txn);
        if (code != bindings.RftError.RFT_ERROR_OK.value) {
          throw RaftDbException.fromCode(code);
        }
      }),
    );
  }

  /// Discard the transaction. Consumes the handle.
  Future<void> rollback() {
    // Idempotent: a second rollback (or rollback after commit) is a no-op.
    if (_consumed) return Future.value();
    _consumed = true;
    final addr = _address;
    final loader = _libLoader;
    return _serialize(
      () => Isolate.run(() {
        final db = bindings.RaftDbBindings(loader());
        final txn = ffi.Pointer<bindings.RaftTransaction>.fromAddress(addr);
        db.rft_transaction_rollback(txn);
      }),
    );
  }

  void _assertActive() {
    if (_consumed) {
      throw StateError(
        'RaftTransaction has already been committed or rolled back',
      );
    }
  }

  // Internal — used by RaftDb.beginTransaction.
  static RaftTransaction internalNew(
    int address,
    ffi.DynamicLibrary Function() libLoader,
  ) => RaftTransaction._(address, libLoader);
}
