import 'dart:ffi' as ffi;

import 'package:ffi/ffi.dart';

/// Minimal, hand-written bindings to the raft-db C ABI (`core/include/raft.h`).
///
/// Only the symbols the benchmark needs are bound. Unlike the high-level
/// `raft_db_flutter` Dart API — which dispatches every call through
/// `Isolate.run` — these are direct synchronous FFI calls on the calling
/// isolate, so the harness measures the engine, not an isolate hop. This
/// matches how the synchronous competitors (Isar, ObjectBox, Realm, Hive) run.
class RaftFfi {
  RaftFfi(ffi.DynamicLibrary lib)
      : open = lib.lookupFunction<_OpenC, _OpenD>('rft_open'),
        close = lib.lookupFunction<_CloseC, _CloseD>('rft_close'),
        collectionPut = lib
            .lookupFunction<_CollPutC, _CollPutD>('rft_collection_put'),
        collectionGet = lib
            .lookupFunction<_CollGetC, _CollGetD>('rft_collection_get'),
        collectionDelete = lib
            .lookupFunction<_CollDelC, _CollDelD>('rft_collection_delete'),
        collectionCount = lib
            .lookupFunction<_CollCountC, _CollCountD>('rft_collection_count'),
        collectionListIds = lib
            .lookupFunction<_CollListC, _CollListD>('rft_collection_list_ids'),
        txnBegin = lib.lookupFunction<_TxnBeginC, _TxnBeginD>(
            'rft_transaction_begin'),
        txnPut =
            lib.lookupFunction<_TxnPutC, _TxnPutD>('rft_transaction_put'),
        txnDelete = lib
            .lookupFunction<_TxnDelC, _TxnDelD>('rft_transaction_delete'),
        txnCommit = lib.lookupFunction<_TxnCommitC, _TxnCommitD>(
            'rft_transaction_commit'),
        txnRollback = lib.lookupFunction<_TxnRollbackC, _TxnRollbackD>(
            'rft_transaction_rollback'),
        putMany = lib
            .lookupFunction<_PutManyC, _PutManyD>('rft_collection_put_many'),
        deleteMany = lib.lookupFunction<_DeleteManyC, _DeleteManyD>(
            'rft_collection_delete_many'),
        scan = lib.lookupFunction<_ScanC, _ScanD>('rft_collection_scan'),
        getBuf =
            lib.lookupFunction<_GetBufC, _GetBufD>('rft_collection_get_buf'),
        bufData = lib.lookupFunction<_BufDataC, _BufDataD>('rft_buf_data'),
        bufLen = lib.lookupFunction<_BufLenC, _BufLenD>('rft_buf_len'),
        bufFree = lib.lookupFunction<_BufFreeC, _BufFreeD>('rft_buf_free'),
        collOpen =
            lib.lookupFunction<_CollOpenC, _CollOpenD>('rft_collection_open'),
        collClose = lib
            .lookupFunction<_CollCloseC, _CollCloseD>('rft_collection_close'),
        collGetBuf =
            lib.lookupFunction<_CollGetBufC, _CollGetBufD>('rft_coll_get_buf'),
        collGetMany = lib
            .lookupFunction<_CollGetManyC, _CollGetManyD>('rft_coll_get_many'),
        collGeneration = lib.lookupFunction<_CollGenC, _CollGenD>(
            'rft_coll_generation');

  final ffi.Pointer<ffi.Void> Function(ffi.Pointer<Utf8>, ffi.Pointer<ffi.Uint32>) open;
  final void Function(ffi.Pointer<ffi.Void>) close;
  final int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Pointer<ffi.Uint8>, int) collectionPut;
  final int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, int, ffi.Pointer<ffi.Uint8>, ffi.Pointer<ffi.UintPtr>) collectionGet;
  final int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, int) collectionDelete;
  final int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Pointer<ffi.UintPtr>) collectionCount;
  final int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Pointer<ffi.Uint64>, ffi.Pointer<ffi.UintPtr>) collectionListIds;
  final int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<ffi.Pointer<ffi.Void>>) txnBegin;
  final int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Pointer<ffi.Uint8>, int) txnPut;
  final int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, int) txnDelete;
  final int Function(ffi.Pointer<ffi.Void>) txnCommit;
  final void Function(ffi.Pointer<ffi.Void>) txnRollback;

  // Batch / scan hot path (binary codec — see core/src/ffi/codec.rs).
  final int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Pointer<ffi.Uint8>, int) putMany;
  final int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Pointer<ffi.Uint64>, int) deleteMany;
  final int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Pointer<ffi.Pointer<ffi.Void>>) scan;
  final int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, int, ffi.Pointer<ffi.Uint8>, ffi.Pointer<ffi.UintPtr>) getBuf;
  final ffi.Pointer<ffi.Uint8> Function(ffi.Pointer<ffi.Void>) bufData;
  final int Function(ffi.Pointer<ffi.Void>) bufLen;
  final void Function(ffi.Pointer<ffi.Void>) bufFree;

  // Prepared collection handles (hot-read surface).
  final int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Pointer<ffi.Pointer<ffi.Void>>) collOpen;
  final void Function(ffi.Pointer<ffi.Void>) collClose;
  final int Function(ffi.Pointer<ffi.Void>, int, ffi.Pointer<ffi.Uint8>, ffi.Pointer<ffi.UintPtr>) collGetBuf;
  final int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<ffi.Uint64>, int, ffi.Pointer<ffi.Pointer<ffi.Void>>) collGetMany;
  final ffi.Pointer<ffi.Uint64> Function(ffi.Pointer<ffi.Void>) collGeneration;
}

/// Error codes mirror `RftError` in `core/include/raft.h`.
class RftErr {
  static const ok = 0;
  static const notFound = 4;
  static const bufferTooSmall = 5;
}

// --- C signatures ---
typedef _OpenC = ffi.Pointer<ffi.Void> Function(
    ffi.Pointer<Utf8>, ffi.Pointer<ffi.Uint32>);
typedef _OpenD = ffi.Pointer<ffi.Void> Function(
    ffi.Pointer<Utf8>, ffi.Pointer<ffi.Uint32>);

typedef _CloseC = ffi.Void Function(ffi.Pointer<ffi.Void>);
typedef _CloseD = void Function(ffi.Pointer<ffi.Void>);

typedef _CollPutC = ffi.Uint32 Function(
    ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Pointer<ffi.Uint8>, ffi.UintPtr);
typedef _CollPutD = int Function(
    ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Pointer<ffi.Uint8>, int);

typedef _CollGetC = ffi.Uint32 Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>,
    ffi.Uint64, ffi.Pointer<ffi.Uint8>, ffi.Pointer<ffi.UintPtr>);
typedef _CollGetD = int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, int,
    ffi.Pointer<ffi.Uint8>, ffi.Pointer<ffi.UintPtr>);

typedef _CollDelC = ffi.Uint32 Function(
    ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Uint64);
typedef _CollDelD = int Function(
    ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, int);

typedef _CollCountC = ffi.Uint32 Function(
    ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Pointer<ffi.UintPtr>);
typedef _CollCountD = int Function(
    ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Pointer<ffi.UintPtr>);

typedef _CollListC = ffi.Uint32 Function(ffi.Pointer<ffi.Void>,
    ffi.Pointer<Utf8>, ffi.Pointer<ffi.Uint64>, ffi.Pointer<ffi.UintPtr>);
typedef _CollListD = int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>,
    ffi.Pointer<ffi.Uint64>, ffi.Pointer<ffi.UintPtr>);

typedef _TxnBeginC = ffi.Uint32 Function(
    ffi.Pointer<ffi.Void>, ffi.Pointer<ffi.Pointer<ffi.Void>>);
typedef _TxnBeginD = int Function(
    ffi.Pointer<ffi.Void>, ffi.Pointer<ffi.Pointer<ffi.Void>>);

typedef _TxnPutC = ffi.Uint32 Function(
    ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Pointer<ffi.Uint8>, ffi.UintPtr);
typedef _TxnPutD = int Function(
    ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Pointer<ffi.Uint8>, int);

typedef _TxnDelC = ffi.Uint32 Function(
    ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Uint64);
typedef _TxnDelD = int Function(
    ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, int);

typedef _TxnCommitC = ffi.Uint32 Function(ffi.Pointer<ffi.Void>);
typedef _TxnCommitD = int Function(ffi.Pointer<ffi.Void>);

typedef _TxnRollbackC = ffi.Void Function(ffi.Pointer<ffi.Void>);
typedef _TxnRollbackD = void Function(ffi.Pointer<ffi.Void>);

typedef _PutManyC = ffi.Uint32 Function(
    ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Pointer<ffi.Uint8>, ffi.UintPtr);
typedef _PutManyD = int Function(
    ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Pointer<ffi.Uint8>, int);

typedef _DeleteManyC = ffi.Uint32 Function(
    ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Pointer<ffi.Uint64>, ffi.UintPtr);
typedef _DeleteManyD = int Function(
    ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, ffi.Pointer<ffi.Uint64>, int);

typedef _ScanC = ffi.Uint32 Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>,
    ffi.Pointer<ffi.Pointer<ffi.Void>>);
typedef _ScanD = int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>,
    ffi.Pointer<ffi.Pointer<ffi.Void>>);

typedef _GetBufC = ffi.Uint32 Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>,
    ffi.Uint64, ffi.Pointer<ffi.Uint8>, ffi.Pointer<ffi.UintPtr>);
typedef _GetBufD = int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>, int,
    ffi.Pointer<ffi.Uint8>, ffi.Pointer<ffi.UintPtr>);

typedef _BufDataC = ffi.Pointer<ffi.Uint8> Function(ffi.Pointer<ffi.Void>);
typedef _BufDataD = ffi.Pointer<ffi.Uint8> Function(ffi.Pointer<ffi.Void>);

typedef _BufLenC = ffi.UintPtr Function(ffi.Pointer<ffi.Void>);
typedef _BufLenD = int Function(ffi.Pointer<ffi.Void>);

typedef _BufFreeC = ffi.Void Function(ffi.Pointer<ffi.Void>);
typedef _BufFreeD = void Function(ffi.Pointer<ffi.Void>);

typedef _CollOpenC = ffi.Uint32 Function(ffi.Pointer<ffi.Void>,
    ffi.Pointer<Utf8>, ffi.Pointer<ffi.Pointer<ffi.Void>>);
typedef _CollOpenD = int Function(ffi.Pointer<ffi.Void>, ffi.Pointer<Utf8>,
    ffi.Pointer<ffi.Pointer<ffi.Void>>);

typedef _CollCloseC = ffi.Void Function(ffi.Pointer<ffi.Void>);
typedef _CollCloseD = void Function(ffi.Pointer<ffi.Void>);

typedef _CollGetBufC = ffi.Uint32 Function(ffi.Pointer<ffi.Void>, ffi.Uint64,
    ffi.Pointer<ffi.Uint8>, ffi.Pointer<ffi.UintPtr>);
typedef _CollGetBufD = int Function(ffi.Pointer<ffi.Void>, int,
    ffi.Pointer<ffi.Uint8>, ffi.Pointer<ffi.UintPtr>);

typedef _CollGetManyC = ffi.Uint32 Function(ffi.Pointer<ffi.Void>,
    ffi.Pointer<ffi.Uint64>, ffi.UintPtr, ffi.Pointer<ffi.Pointer<ffi.Void>>);
typedef _CollGetManyD = int Function(ffi.Pointer<ffi.Void>,
    ffi.Pointer<ffi.Uint64>, int, ffi.Pointer<ffi.Pointer<ffi.Void>>);

typedef _CollGenC = ffi.Pointer<ffi.Uint64> Function(ffi.Pointer<ffi.Void>);
typedef _CollGenD = ffi.Pointer<ffi.Uint64> Function(ffi.Pointer<ffi.Void>);
