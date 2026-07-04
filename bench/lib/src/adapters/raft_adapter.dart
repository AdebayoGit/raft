import 'dart:convert';
import 'dart:ffi' as ffi;
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';
import 'package:path/path.dart' as p;

import '../adapter.dart';
import '../model.dart';
import 'raft_codec.dart';
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
    String versionLabel = '0.1.0 (FFI, batch binary codec)',
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


  /// One FFI crossing: binary batch encode + `rft_collection_put_many`
  /// (one transaction, one WAL write, one fsync — same durability contract
  /// as the per-doc transaction loop it replaces).
  Future<void> _putManyBinary(List<BenchDoc> docs, {int scoreDelta = 0}) async {
    final batch = RaftCodec.encodeBatch(docs, scoreDelta: scoreDelta);
    final ptr = malloc<ffi.Uint8>(batch.length);
    try {
      ptr.asTypedList(batch.length).setAll(0, batch);
      final code = _ffi!.putMany(_db, _cCollection, ptr, batch.length);
      if (code != RftErr.ok) {
        throw StateError('rft_collection_put_many failed (code $code)');
      }
    } finally {
      malloc.free(ptr);
    }
  }

  @override
  Future<void> bulkWrite(List<BenchDoc> docs) => _putManyBinary(docs);

  @override
  Future<void> bulkUpdate(List<BenchDoc> docs) =>
      _putManyBinary(docs, scoreDelta: 1);

  @override
  Future<void> bulkDelete(List<int> ids) async {
    final ptr = malloc<ffi.Uint64>(ids.length);
    try {
      for (var i = 0; i < ids.length; i++) {
        ptr[i] = ids[i];
      }
      final code = _ffi!.deleteMany(_db, _cCollection, ptr, ids.length);
      if (code != RftErr.ok) {
        throw StateError('rft_collection_delete_many failed (code $code)');
      }
    } finally {
      malloc.free(ptr);
    }
  }

  @override
  Future<void> durableWrites(List<BenchDoc> docs) async {
    // Each collection_put is its own commit → its own fsync.
    for (final d in docs) {
      final json = _encodeDocJson(d);
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
  Future<void> concurrentDurableWrites(List<List<BenchDoc>> chunks) async {
    // True thread-level concurrency: each chunk runs on its own isolate,
    // calling the synchronous per-commit FFI against the same database
    // handle. The core's group commit merges concurrent fsyncs — several
    // writers share one flush instead of queueing 19 ms flushes serially.
    final loadLibrary = _loadLibrary;
    final dbAddress = _db.address;
    await Future.wait([
      for (final chunk in chunks)
        Isolate.run(() {
          final ffi_ = RaftFfi(loadLibrary());
          final db = ffi.Pointer<ffi.Void>.fromAddress(dbAddress);
          final coll = 'bench'.toNativeUtf8();
          try {
            for (final d in chunk) {
              final json = _encodeDocJson(d);
              final ptr = malloc<ffi.Uint8>(json.length);
              try {
                ptr.asTypedList(json.length).setAll(0, json);
                final code = ffi_.collectionPut(db, coll, ptr, json.length);
                if (code != RftErr.ok) {
                  throw StateError('rft_collection_put failed (code $code)');
                }
              } finally {
                malloc.free(ptr);
              }
            }
          } finally {
            malloc.free(coll);
          }
        }),
    ]);
  }

  @override
  Future<int> pointReads(List<int> ids) async {
    // Single-phase get into one reusable buffer: one FFI call and one
    // engine lookup per read. The document is fully decoded — the same
    // object materialisation a competitor's `get` performs.
    var found = 0;
    const cap = 8192;
    final buf = malloc<ffi.Uint8>(cap);
    final lenPtr = calloc<ffi.UintPtr>();
    try {
      final view = buf.asTypedList(cap);
      final bd = ByteData.view(view.buffer, view.offsetInBytes, cap);
      for (final id in ids) {
        lenPtr.value = cap;
        final code = _ffi!.getBuf(_db, _cCollection, id, buf, lenPtr);
        if (code == RftErr.ok) {
          RaftCodec.decodeDoc(view, bd, 0, lenPtr.value);
          found++;
        } else if (code == RftErr.bufferTooSmall) {
          // Oversized doc (rare): retry with an exact buffer.
          final needed = lenPtr.value;
          final big = malloc<ffi.Uint8>(needed);
          try {
            lenPtr.value = needed;
            final rc = _ffi!.getBuf(_db, _cCollection, id, big, lenPtr);
            if (rc == RftErr.ok) {
              final bigView = big.asTypedList(needed);
              RaftCodec.decodeDoc(
                bigView,
                ByteData.view(bigView.buffer, bigView.offsetInBytes, needed),
                0,
                lenPtr.value,
              );
              found++;
            }
          } finally {
            malloc.free(big);
          }
        } else if (code != RftErr.notFound) {
          throw StateError('rft_collection_get_buf failed (code $code)');
        }
      }
    } finally {
      malloc.free(buf);
      calloc.free(lenPtr);
    }
    return found;
  }

  @override
  Future<int> iterateAll() async {
    // One engine pass via rft_collection_scan; every document is decoded —
    // the honest equivalent of a competitor's full findAll().
    final outBuf = calloc<ffi.Pointer<ffi.Void>>();
    try {
      final code = _ffi!.scan(_db, _cCollection, outBuf);
      if (code != RftErr.ok) {
        throw StateError('rft_collection_scan failed (code $code)');
      }
      final handle = outBuf.value;
      try {
        final len = _ffi!.bufLen(handle);
        if (len == 0) return 0;
        final data = _ffi!.bufData(handle);
        // Decode directly from the native view — the buffer stays alive
        // until bufFree below, so no copy is needed.
        return RaftCodec.decodeBatch(data.asTypedList(len)).length;
      } finally {
        _ffi!.bufFree(handle);
      }
    } finally {
      calloc.free(outBuf);
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

/// raft's typed-field JSON envelope for the per-commit durable path.
/// Top-level (not a method) so isolate closures stay sendable.
Uint8List _encodeDocJson(BenchDoc d) {
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
