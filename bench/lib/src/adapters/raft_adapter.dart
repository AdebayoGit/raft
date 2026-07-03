import 'dart:convert';
import 'dart:ffi' as ffi;
import 'dart:io';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';
import 'package:path/path.dart' as p;

import '../adapter.dart';
import '../model.dart';
import 'raft_ffi.dart';

/// Drives raft-db through its C ABI collection/document store.
///
/// Documents use raft's typed-field JSON envelope, e.g.
/// `{"id":1,"fields":{"name":{"String":"user-00000001"},"score":{"Int":1},
/// "payload":{"String":"..."}}}`.
class RaftAdapter implements DbAdapter {
  /// [loadLibrary] returns the loaded native library (opened however the host
  /// prefers: by path on desktop, `DynamicLibrary.process()` on iOS, or
  /// `DynamicLibrary.open('libraftdb.so')` on Android). [availabilityCheck],
  /// if given, runs before loading so a missing library is reported cleanly.
  RaftAdapter({
    required ffi.DynamicLibrary Function() loadLibrary,
    Availability Function()? availabilityCheck,
    String versionLabel = '0.1.0 (FFI, collection store)',
  })  : _loadLibrary = loadLibrary,
        _availabilityCheck = availabilityCheck,
        _versionLabel = versionLabel;

  /// Convenience constructor for desktop hosts: load `libraftdb.*` from a path.
  factory RaftAdapter.fromPath(String dylibPath) => RaftAdapter(
        loadLibrary: () => ffi.DynamicLibrary.open(dylibPath),
        availabilityCheck: () => File(dylibPath).existsSync()
            ? const Availability.available()
            : Availability.unavailable(
                'raft dylib not found at $dylibPath — run build-mobile.sh or '
                '`cargo build --release --features ffi`'),
      );

  final ffi.DynamicLibrary Function() _loadLibrary;
  final Availability Function()? _availabilityCheck;
  final String _versionLabel;

  RaftFfi? _ffi;
  ffi.Pointer<ffi.Void> _db = ffi.nullptr;
  final _cCollection = 'bench'.toNativeUtf8();

  @override
  String get name => 'raft-db';

  @override
  String get version => _versionLabel;

  @override
  String get durabilityNote =>
      'Every commit fsyncs (SyncMode::Always, F_FULLFSYNC) — no FFI knob to '
      'disable it; the only engine here that flushes to stable storage by default.';

  @override
  Set<Workload> get supported => Workload.values.toSet();

  @override
  Future<Availability> probe() async {
    if (_availabilityCheck != null) {
      final a = _availabilityCheck();
      if (!a.isAvailable) return a;
    }
    try {
      _ffi = RaftFfi(_loadLibrary());
      return const Availability.available();
    } catch (e) {
      return Availability.unavailable('failed to load raft native library: $e');
    }
  }

  @override
  Future<void> openFresh(String dir) async {
    _ffi ??= RaftFfi(_loadLibrary());
    final path = p.join(dir, 'raft');
    final pathPtr = path.toNativeUtf8();
    final errPtr = calloc<ffi.Uint32>();
    try {
      _db = _ffi!.open(pathPtr, errPtr);
      final code = errPtr.value;
      if (code != RftErr.ok || _db == ffi.nullptr) {
        throw StateError('rft_open failed (code $code)');
      }
    } finally {
      malloc.free(pathPtr);
      calloc.free(errPtr);
    }
  }

  Uint8List _docJson(BenchDoc d) {
    // raft's typed-field envelope. Built by hand (rather than a Map) to keep
    // the hot path allocation-light.
    final obj = {
      'id': d.id,
      'fields': {
        'name': {'String': d.name},
        'score': {'Int': d.score},
        'payload': {'String': d.payload},
      },
    };
    return utf8.encode(jsonEncode(obj));
  }

  void _putInTxn(ffi.Pointer<ffi.Void> txn, BenchDoc d) {
    final json = _docJson(d);
    final ptr = malloc<ffi.Uint8>(json.length);
    try {
      ptr.asTypedList(json.length).setAll(0, json);
      final code = _ffi!.txnPut(txn, _cCollection, ptr, json.length);
      if (code != RftErr.ok) {
        throw StateError('rft_transaction_put failed (code $code)');
      }
    } finally {
      malloc.free(ptr);
    }
  }

  Future<void> _inTransaction(void Function(ffi.Pointer<ffi.Void> txn) body) async {
    final outTxn = calloc<ffi.Pointer<ffi.Void>>();
    try {
      final beginCode = _ffi!.txnBegin(_db, outTxn);
      if (beginCode != RftErr.ok) {
        throw StateError('rft_transaction_begin failed (code $beginCode)');
      }
      final txn = outTxn.value;
      try {
        body(txn);
        final commitCode = _ffi!.txnCommit(txn);
        if (commitCode != RftErr.ok) {
          throw StateError('rft_transaction_commit failed (code $commitCode)');
        }
      } catch (_) {
        _ffi!.txnRollback(txn);
        rethrow;
      }
    } finally {
      calloc.free(outTxn);
    }
  }

  @override
  Future<void> bulkWrite(List<BenchDoc> docs) async {
    await _inTransaction((txn) {
      for (final d in docs) {
        _putInTxn(txn, d);
      }
    });
  }

  @override
  Future<void> bulkUpdate(List<BenchDoc> docs) async {
    // Re-put every doc with a bumped score in one transaction.
    await _inTransaction((txn) {
      for (final d in docs) {
        _putInTxn(
          txn,
          BenchDoc(id: d.id, name: d.name, score: d.score + 1, payload: d.payload),
        );
      }
    });
  }

  @override
  Future<void> bulkDelete(List<int> ids) async {
    await _inTransaction((txn) {
      for (final id in ids) {
        final code = _ffi!.txnDelete(txn, _cCollection, id);
        if (code != RftErr.ok) {
          throw StateError('rft_transaction_delete failed (code $code)');
        }
      }
    });
  }

  @override
  Future<void> durableWrites(List<BenchDoc> docs) async {
    // Each collection_put is its own commit → its own fsync.
    for (final d in docs) {
      final json = _docJson(d);
      final ptr = malloc<ffi.Uint8>(json.length);
      try {
        ptr.asTypedList(json.length).setAll(0, json);
        final code = _ffi!.collectionPut(_db, _cCollection, ptr, json.length);
        if (code != RftErr.ok) {
          throw StateError('rft_collection_put failed (code $code)');
        }
      } finally {
        malloc.free(ptr);
      }
    }
  }

  @override
  Future<int> pointReads(List<int> ids) async {
    var found = 0;
    final lenPtr = calloc<ffi.UintPtr>();
    try {
      for (final id in ids) {
        // Phase 1: size query.
        lenPtr.value = 0;
        final sizeCode =
            _ffi!.collectionGet(_db, _cCollection, id, ffi.nullptr, lenPtr);
        if (sizeCode == RftErr.notFound) continue;
        if (sizeCode != RftErr.bufferTooSmall && sizeCode != RftErr.ok) {
          throw StateError('rft_collection_get(size) failed (code $sizeCode)');
        }
        final needed = lenPtr.value;
        final buf = malloc<ffi.Uint8>(needed);
        try {
          final code = _ffi!.collectionGet(_db, _cCollection, id, buf, lenPtr);
          if (code == RftErr.ok) {
            found++;
          } else if (code != RftErr.notFound) {
            throw StateError('rft_collection_get failed (code $code)');
          }
        } finally {
          malloc.free(buf);
        }
      }
    } finally {
      calloc.free(lenPtr);
    }
    return found;
  }

  @override
  Future<int> iterateAll() async {
    // List every id, then materialise each document — the honest "read all
    // records" equivalent of a competitor's full-table SELECT.
    final lenPtr = calloc<ffi.UintPtr>();
    try {
      final sizeCode =
          _ffi!.collectionListIds(_db, _cCollection, ffi.nullptr, lenPtr);
      if (sizeCode != RftErr.bufferTooSmall && sizeCode != RftErr.ok) {
        throw StateError('rft_collection_list_ids(size) failed ($sizeCode)');
      }
      final count = lenPtr.value;
      if (count == 0) return 0;
      final idsBuf = malloc<ffi.Uint64>(count);
      try {
        final code =
            _ffi!.collectionListIds(_db, _cCollection, idsBuf, lenPtr);
        if (code != RftErr.ok) {
          throw StateError('rft_collection_list_ids failed (code $code)');
        }
        final actual = lenPtr.value;
        final ids = List<int>.generate(actual, (i) => idsBuf[i]);
        return pointReads(ids);
      } finally {
        malloc.free(idsBuf);
      }
    } finally {
      calloc.free(lenPtr);
    }
  }

  @override
  Future<void> close() async {
    if (_db != ffi.nullptr) {
      _ffi!.close(_db);
      _db = ffi.nullptr;
    }
  }
}
