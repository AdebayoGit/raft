import 'dart:async';
import 'dart:convert';
import 'dart:ffi' as ffi;
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'raft_collection.dart';
import 'raft_db_bindings.dart' as bindings;
import 'raft_doc.dart';
import 'raft_events.dart';
import 'raft_transaction.dart';

/// Loads the native RaftDB library for the current platform.
ffi.DynamicLibrary openRaftLib() => _openLib();

ffi.DynamicLibrary _openLib() {
  if (Platform.isAndroid || Platform.isLinux) {
    return ffi.DynamicLibrary.open('libraftdb.so');
  }
  if (Platform.isIOS || Platform.isMacOS) {
    // Static xcframework on iOS; process-global symbol table on macOS.
    return ffi.DynamicLibrary.process();
  }
  if (Platform.isWindows) {
    return ffi.DynamicLibrary.open('raftdb.dll');
  }
  throw UnsupportedError('Unsupported platform: ${Platform.operatingSystem}');
}

/// Thrown when a native RaftDB call returns a non-OK error code.
class RaftDbException implements Exception {
  const RaftDbException(this.message, {this.code});

  final String message;
  final int? code;

  factory RaftDbException.fromCode(int code) {
    // Codes mirror `core/include/raft.h`. Keep this in sync when new
    // RftError variants are added on the Rust side.
    final message = switch (code) {
      1 => 'Null pointer argument',
      2 => 'Invalid UTF-8 in path or key',
      3 => 'I/O or storage engine error',
      4 => 'Key not found',
      5 => 'Buffer too small',
      6 => 'Invalid JSON in document or query',
      7 => 'Transaction commit conflicted with a concurrent write',
      8 => 'Native handle is invalid or already consumed',
      9 => 'Subscription id is not registered',
      10 => 'Internal panic in native core; close and reopen the database',
      11 =>
        'Invalid database path (empty, contains "..", or escapes the '
            'confinement root)',
      12 => 'Dart API not initialized (rft_dart_init was not called)',
      13 => 'JSON payload exceeds its size cap',
      14 => 'JSON envelope declared an unsupported schema version',
      _ => 'Unknown error (code $code)',
    };
    return RaftDbException(message, code: code);
  }

  @override
  String toString() =>
      'RaftDbException: $message${code != null ? ' (code $code)' : ''}';
}

/// A handle to an open Raft embedded database.
///
/// Each operation is dispatched via [Isolate.run] so the calling isolate
/// is never blocked by native I/O.
///
/// ```dart
/// final db = await RaftDb.open('/data/user/0/myapp/files/raft');
/// await db.put(utf8.encode('hello'), utf8.encode('world'));
/// final value = await db.get(utf8.encode('hello')); // Uint8List or null
/// await db.delete(utf8.encode('hello'));
/// await db.close();
/// ```
class RaftDb {
  RaftDb._(this._address);

  /// The native pointer address, passed as an [int] across isolate boundaries.
  final int _address;
  bool _closed = false;

  /// Bindings bound on the opening isolate — the synchronous fast paths
  /// (typed collections) run here, without a per-call isolate hop.
  late final bindings.RaftDbBindings _syncBindings = bindings.RaftDbBindings(
    _openLib(),
  );

  /// Typed collections opened from this database, closed automatically
  /// (before the native handle) by [close].
  final List<RaftCollection<dynamic>> _collections = [];

  /// The raw native handle. Internal — used by [RaftCollection].
  ffi.Pointer<bindings.RaftDb> get nativeHandle =>
      ffi.Pointer<bindings.RaftDb>.fromAddress(_address);

  /// Run [f] with [name] as a temporary native UTF-8 string. Internal.
  int withNativeCollectionName(
    String name,
    int Function(ffi.Pointer<ffi.Char> cName) f,
  ) {
    final cName = name.toNativeUtf8();
    try {
      return f(cName.cast());
    } finally {
      malloc.free(cName);
    }
  }

  /// Open a typed collection — raft's flagship API. No code generation:
  /// supply [encode] / [decode] closures over [RaftDocWriter] /
  /// [RaftDocReader] and [id] to extract the primary key. See
  /// [RaftCollection] for the full surface and an example.
  RaftCollection<T> collection<T>({
    required String name,
    required int Function(T value) id,
    required void Function(T value, RaftDocWriter w) encode,
    required T Function(RaftDocReader r) decode,
  }) {
    _assertOpen();
    final coll = RaftCollection<T>.internal(
      db: this,
      name: name,
      id: id,
      encode: encode,
      decode: decode,
      bindings: _syncBindings,
      dbHandle: nativeHandle,
    );
    _collections.add(coll);
    return coll;
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  /// Opens or creates a Raft database at [path].
  ///
  /// Throws [RaftDbException] if the open fails.
  static Future<RaftDb> open(String path) async {
    final address = await Isolate.run(() {
      final db = bindings.RaftDbBindings(_openLib());
      final pathPtr = path.toNativeUtf8();
      final errPtr = calloc<ffi.Uint32>();
      try {
        final handle = db.rft_open(pathPtr.cast(), errPtr.cast());
        final code = errPtr.value;
        if (code != bindings.RftError.RFT_ERROR_OK.value) {
          throw RaftDbException.fromCode(code);
        }
        if (handle == ffi.nullptr) {
          throw const RaftDbException('rft_open returned null with OK status');
        }
        return handle.address;
      } finally {
        malloc.free(pathPtr);
        calloc.free(errPtr);
      }
    });
    return RaftDb._(address);
  }

  /// Flushes pending writes and releases the native database handle.
  ///
  /// After [close], any further calls on this instance throw [StateError].
  Future<void> close() async {
    _assertOpen();
    _closed = true;
    // Collection handles must be released before the database handle.
    for (final coll in _collections) {
      coll.close();
    }
    _collections.clear();
    final address = _address;
    return Isolate.run(() {
      final db = bindings.RaftDbBindings(_openLib());
      db.rft_close(ffi.Pointer<bindings.RaftDb>.fromAddress(address));
    });
  }

  // ---------------------------------------------------------------------------
  // Writes
  // ---------------------------------------------------------------------------

  /// Inserts or updates [value] for [key].
  ///
  /// Throws [RaftDbException] on failure.
  Future<void> put(Uint8List key, Uint8List value) {
    _assertOpen();
    final address = _address;
    return Isolate.run(() {
      final db = bindings.RaftDbBindings(_openLib());
      final handle = ffi.Pointer<bindings.RaftDb>.fromAddress(address);

      final keyPtr = malloc<ffi.Uint8>(key.length);
      final valPtr = malloc<ffi.Uint8>(value.length);
      try {
        keyPtr.asTypedList(key.length).setAll(0, key);
        valPtr.asTypedList(value.length).setAll(0, value);
        final code = db.rft_put(
          handle,
          keyPtr,
          key.length,
          valPtr,
          value.length,
        );
        if (code != bindings.RftError.RFT_ERROR_OK.value) {
          throw RaftDbException.fromCode(code);
        }
      } finally {
        malloc.free(keyPtr);
        malloc.free(valPtr);
      }
    });
  }

  /// Deletes [key] from the database.
  ///
  /// Deleting a non-existent key is a no-op (a tombstone is written).
  /// Throws [RaftDbException] on failure.
  Future<void> delete(Uint8List key) {
    _assertOpen();
    final address = _address;
    return Isolate.run(() {
      final db = bindings.RaftDbBindings(_openLib());
      final handle = ffi.Pointer<bindings.RaftDb>.fromAddress(address);

      final keyPtr = malloc<ffi.Uint8>(key.length);
      try {
        keyPtr.asTypedList(key.length).setAll(0, key);
        final code = db.rft_delete(handle, keyPtr, key.length);
        if (code != bindings.RftError.RFT_ERROR_OK.value) {
          throw RaftDbException.fromCode(code);
        }
      } finally {
        malloc.free(keyPtr);
      }
    });
  }

  // ---------------------------------------------------------------------------
  // Reads
  // ---------------------------------------------------------------------------

  /// Returns the value stored at [key], or `null` if the key does not exist.
  ///
  /// Uses a two-phase read: first queries the required buffer size, then
  /// reads the value — no hard-coded buffer limits.
  ///
  /// Throws [RaftDbException] for errors other than key-not-found.
  Future<Uint8List?> get(Uint8List key) {
    _assertOpen();
    final address = _address;
    return Isolate.run(() {
      final db = bindings.RaftDbBindings(_openLib());
      final handle = ffi.Pointer<bindings.RaftDb>.fromAddress(address);

      final keyPtr = malloc<ffi.Uint8>(key.length);
      keyPtr.asTypedList(key.length).setAll(0, key);

      final lenPtr = calloc<ffi.UintPtr>();
      try {
        // Phase 1: query required buffer size by passing a null output pointer.
        final sizeCode = db.rft_get(
          handle,
          keyPtr,
          key.length,
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

        // Phase 2: allocate exact buffer and read.
        final required = lenPtr.value;
        final bufPtr = malloc<ffi.Uint8>(required);
        try {
          final readCode = db.rft_get(
            handle,
            keyPtr,
            key.length,
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
        malloc.free(keyPtr);
        calloc.free(lenPtr);
      }
    });
  }

  // ---------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  // Typed-FFI surface (collections, queries, transactions)
  //
  // These methods address Raft's typed document store and are distinct
  // from the raw-KV `put/get/delete` namespace. Documents are addressed
  // by `int` (UInt64 on the native side); the document's `id` field
  // must match.
  // ---------------------------------------------------------------------------

  /// Insert or update a document in `collection`. The document JSON's
  /// `id` field is honoured.
  Future<void> collectionPut(String collection, Uint8List documentJson) {
    _assertOpen();
    final addr = _address;
    return Isolate.run(() {
      final db = bindings.RaftDbBindings(_openLib());
      final handle = ffi.Pointer<bindings.RaftDb>.fromAddress(addr);
      final cName = collection.toNativeUtf8();
      final jsonPtr = malloc<ffi.Uint8>(documentJson.length);
      try {
        jsonPtr.asTypedList(documentJson.length).setAll(0, documentJson);
        final code = db.rft_collection_put(
          handle,
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
    });
  }

  /// Insert a document into `collection`, letting the engine assign a
  /// fresh document id. Returns the assigned id.
  Future<int> collectionPutAuto(String collection, Uint8List documentJson) {
    _assertOpen();
    final addr = _address;
    return Isolate.run(() {
      final db = bindings.RaftDbBindings(_openLib());
      final handle = ffi.Pointer<bindings.RaftDb>.fromAddress(addr);
      final cName = collection.toNativeUtf8();
      final jsonPtr = malloc<ffi.Uint8>(documentJson.length);
      final outId = calloc<ffi.Uint64>();
      try {
        jsonPtr.asTypedList(documentJson.length).setAll(0, documentJson);
        final code = db.rft_collection_put_auto(
          handle,
          cName.cast(),
          jsonPtr,
          documentJson.length,
          outId,
        );
        if (code != bindings.RftError.RFT_ERROR_OK.value) {
          throw RaftDbException.fromCode(code);
        }
        return outId.value;
      } finally {
        malloc.free(cName);
        malloc.free(jsonPtr);
        calloc.free(outId);
      }
    });
  }

  /// Fetch a document by id from `collection`. Returns its raw JSON
  /// bytes, or `null` if no document with that id exists.
  Future<Uint8List?> collectionGet(String collection, int docId) {
    _assertOpen();
    final addr = _address;
    return Isolate.run(() {
      final db = bindings.RaftDbBindings(_openLib());
      final handle = ffi.Pointer<bindings.RaftDb>.fromAddress(addr);
      final cName = collection.toNativeUtf8();
      final lenPtr = calloc<ffi.UintPtr>();
      try {
        final sizeCode = db.rft_collection_get(
          handle,
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
          final readCode = db.rft_collection_get(
            handle,
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
    });
  }

  /// Delete a document by id from `collection`. Not an error if the id
  /// does not exist.
  Future<void> collectionDelete(String collection, int docId) {
    _assertOpen();
    final addr = _address;
    return Isolate.run(() {
      final db = bindings.RaftDbBindings(_openLib());
      final handle = ffi.Pointer<bindings.RaftDb>.fromAddress(addr);
      final cName = collection.toNativeUtf8();
      try {
        final code = db.rft_collection_delete(handle, cName.cast(), docId);
        if (code != bindings.RftError.RFT_ERROR_OK.value) {
          throw RaftDbException.fromCode(code);
        }
      } finally {
        malloc.free(cName);
      }
    });
  }

  /// Number of documents currently in `collection` (typed namespace).
  Future<int> collectionCount(String collection) {
    _assertOpen();
    final addr = _address;
    return Isolate.run(() {
      final db = bindings.RaftDbBindings(_openLib());
      final handle = ffi.Pointer<bindings.RaftDb>.fromAddress(addr);
      final cName = collection.toNativeUtf8();
      final out = calloc<ffi.UintPtr>();
      try {
        final code = db.rft_collection_count(handle, cName.cast(), out);
        if (code != bindings.RftError.RFT_ERROR_OK.value) {
          throw RaftDbException.fromCode(code);
        }
        return out.value;
      } finally {
        malloc.free(cName);
        calloc.free(out);
      }
    });
  }

  /// All document ids currently in `collection` (typed namespace),
  /// sorted ascending.
  Future<List<int>> collectionListIds(String collection) {
    _assertOpen();
    final addr = _address;
    return Isolate.run(() {
      final db = bindings.RaftDbBindings(_openLib());
      final handle = ffi.Pointer<bindings.RaftDb>.fromAddress(addr);
      final cName = collection.toNativeUtf8();
      final lenPtr = calloc<ffi.UintPtr>();
      try {
        final sizeCode = db.rft_collection_list_ids(
          handle,
          cName.cast(),
          ffi.nullptr,
          lenPtr,
        );
        if (sizeCode != bindings.RftError.RFT_ERROR_BUFFER_TOO_SMALL.value &&
            sizeCode != bindings.RftError.RFT_ERROR_OK.value) {
          throw RaftDbException.fromCode(sizeCode);
        }
        final needed = lenPtr.value;
        if (needed == 0) return <int>[];
        final buf = malloc<ffi.Uint64>(needed);
        try {
          final readCode = db.rft_collection_list_ids(
            handle,
            cName.cast(),
            buf,
            lenPtr,
          );
          if (readCode != bindings.RftError.RFT_ERROR_OK.value) {
            throw RaftDbException.fromCode(readCode);
          }
          final actual = lenPtr.value;
          final ids = <int>[];
          for (var i = 0; i < actual; i++) {
            ids.add(buf[i]);
          }
          return ids;
        } finally {
          malloc.free(buf);
        }
      } finally {
        malloc.free(cName);
        calloc.free(lenPtr);
      }
    });
  }

  /// Execute a predicate query (JSON-encoded) and return each matching
  /// document as raw JSON bytes.
  Future<List<Uint8List>> executeQuery(Uint8List queryJson) {
    _assertOpen();
    final addr = _address;
    return Isolate.run(() {
      final db = bindings.RaftDbBindings(_openLib());
      final handle = ffi.Pointer<bindings.RaftDb>.fromAddress(addr);
      final jsonPtr = malloc<ffi.Uint8>(queryJson.length);
      final outResult = calloc<ffi.Pointer<bindings.RaftQueryResult>>();
      try {
        jsonPtr.asTypedList(queryJson.length).setAll(0, queryJson);
        final execCode = db.rft_query_execute(
          handle,
          jsonPtr,
          queryJson.length,
          outResult,
        );
        if (execCode != bindings.RftError.RFT_ERROR_OK.value) {
          throw RaftDbException.fromCode(execCode);
        }
        final resultHandle = outResult.value;
        if (resultHandle == ffi.nullptr) return <Uint8List>[];
        try {
          final count = db.rft_query_result_count(resultHandle);
          final docs = <Uint8List>[];
          for (var i = 0; i < count; i++) {
            final lenPtr = calloc<ffi.UintPtr>();
            try {
              final sizeCode = db.rft_query_result_get(
                resultHandle,
                i,
                ffi.nullptr,
                lenPtr,
              );
              if (sizeCode !=
                      bindings.RftError.RFT_ERROR_BUFFER_TOO_SMALL.value &&
                  sizeCode != bindings.RftError.RFT_ERROR_OK.value) {
                throw RaftDbException.fromCode(sizeCode);
              }
              final needed = lenPtr.value;
              final bufPtr = malloc<ffi.Uint8>(needed);
              try {
                final readCode = db.rft_query_result_get(
                  resultHandle,
                  i,
                  bufPtr,
                  lenPtr,
                );
                if (readCode != bindings.RftError.RFT_ERROR_OK.value) {
                  throw RaftDbException.fromCode(readCode);
                }
                docs.add(Uint8List.fromList(bufPtr.asTypedList(lenPtr.value)));
              } finally {
                malloc.free(bufPtr);
              }
            } finally {
              calloc.free(lenPtr);
            }
          }
          return docs;
        } finally {
          db.rft_query_result_free(resultHandle);
        }
      } finally {
        malloc.free(jsonPtr);
        calloc.free(outResult);
      }
    });
  }

  // ---------------------------------------------------------------------------
  // Reactive observers
  //
  // Events are delivered from the Rust core to a Dart `ReceivePort` via
  // `Dart_PostCObject_DL`: the VM copies each JSON payload into the port
  // queue during the post call, so there is no native-callback lifetime
  // hazard and no cross-isolate callback restriction.
  // ---------------------------------------------------------------------------

  /// Whether `rft_dart_init` has registered `Dart_PostCObject_DL` with
  /// the native core. Process-wide, so static.
  static bool _dartApiReady = false;

  static void _ensureDartApi(bindings.RaftDbBindings db) {
    if (_dartApiReady) return;
    final code = db.rft_dart_init(
      ffi.Pointer<ffi.Void>.fromAddress(ffi.NativeApi.postCObject.address),
    );
    if (code != bindings.RftError.RFT_ERROR_OK.value) {
      throw RaftDbException.fromCode(code);
    }
    _dartApiReady = true;
  }

  /// Watch [collection] for mutations. Emits a [MutationEvent] each time
  /// a document in the collection is inserted, updated, or deleted.
  ///
  /// The native subscription is registered on first listen and cancelled
  /// when the subscription is cancelled. Events that arrive while the
  /// native side is faster than the listener are buffered by the port.
  Stream<MutationEvent> observeCollection(String collection) {
    _assertOpen();
    return _observeStream<MutationEvent>(
      parse: (json) =>
          MutationEvent.fromJson(jsonDecode(json) as Map<String, dynamic>),
      register: (db, handle, port, outSub) {
        final cName = collection.toNativeUtf8();
        try {
          return db.rft_observe_dart_port(handle, cName.cast(), port, outSub);
        } finally {
          malloc.free(cName);
        }
      },
    );
  }

  /// Run a live query. Emits an initial [QueryDiff] snapshot (all
  /// matching documents in `added`) as soon as the stream is listened
  /// to, then a new diff every time a mutation changes the result set.
  ///
  /// [queryJson] uses the same predicate-query encoding as [executeQuery].
  Stream<QueryDiff> observeQuery(Uint8List queryJson) {
    _assertOpen();
    return _observeStream<QueryDiff>(
      parse: QueryDiff.fromJson,
      register: (db, handle, port, outSub) {
        final jsonPtr = malloc<ffi.Uint8>(queryJson.length);
        try {
          jsonPtr.asTypedList(queryJson.length).setAll(0, queryJson);
          return db.rft_observe_query_dart_port(
            handle,
            jsonPtr,
            queryJson.length,
            port,
            outSub,
          );
        } finally {
          malloc.free(jsonPtr);
        }
      },
    );
  }

  /// Shared plumbing for the observer streams: sets up a [ReceivePort],
  /// registers the native subscription on listen, decodes each posted
  /// JSON string with [parse], and unsubscribes on cancel.
  Stream<T> _observeStream<T>({
    required T Function(String json) parse,
    required int Function(
      bindings.RaftDbBindings db,
      ffi.Pointer<bindings.RaftDb> handle,
      int nativePort,
      ffi.Pointer<ffi.Uint64> outSubId,
    )
    register,
  }) {
    final address = _address;
    late final StreamController<T> controller;
    ReceivePort? receivePort;
    var subId = 0;

    Future<void> stop() async {
      receivePort?.close();
      receivePort = null;
      if (subId != 0) {
        final id = subId;
        subId = 0;
        await Isolate.run(() {
          final db = bindings.RaftDbBindings(_openLib());
          db.rft_unobserve(
            ffi.Pointer<bindings.RaftDb>.fromAddress(address),
            id,
          );
        });
      }
    }

    controller = StreamController<T>(
      onListen: () {
        final db = bindings.RaftDbBindings(_openLib());
        _ensureDartApi(db);

        final port = ReceivePort();
        receivePort = port;
        port.listen((message) {
          if (message is! String) return;
          try {
            controller.add(parse(message));
          } catch (e, st) {
            controller.addError(e, st);
          }
        });

        // Registration only spawns a background task in the core — it
        // does not block — so it is safe to call on this isolate. It
        // must NOT hop isolates: the SendPort belongs to this one.
        final outSub = calloc<ffi.Uint64>();
        try {
          final code = register(
            db,
            ffi.Pointer<bindings.RaftDb>.fromAddress(address),
            port.sendPort.nativePort,
            outSub,
          );
          if (code != bindings.RftError.RFT_ERROR_OK.value) {
            port.close();
            receivePort = null;
            controller.addError(RaftDbException.fromCode(code));
            controller.close();
            return;
          }
          subId = outSub.value;
        } finally {
          calloc.free(outSub);
        }
      },
      onCancel: stop,
    );
    return controller.stream;
  }

  /// Begin a new transaction. The caller takes ownership of the returned
  /// [RaftTransaction] and must end it with `commit` or `rollback`.
  Future<RaftTransaction> beginTransaction() async {
    _assertOpen();
    final addr = _address;
    final txnAddr = await Isolate.run(() {
      final db = bindings.RaftDbBindings(_openLib());
      final handle = ffi.Pointer<bindings.RaftDb>.fromAddress(addr);
      final outTxn = calloc<ffi.Pointer<bindings.RaftTransaction>>();
      try {
        final code = db.rft_transaction_begin(handle, outTxn);
        if (code != bindings.RftError.RFT_ERROR_OK.value) {
          throw RaftDbException.fromCode(code);
        }
        return outTxn.value.address;
      } finally {
        calloc.free(outTxn);
      }
    });
    return RaftTransaction.internalNew(txnAddr, _openLib);
  }

  /// Run `block` inside a transaction. If it returns normally, the
  /// transaction is committed; if it throws, it is rolled back and the
  /// error is rethrown.
  Future<T> withTransaction<T>(
    Future<T> Function(RaftTransaction txn) block,
  ) async {
    final txn = await beginTransaction();
    try {
      final result = await block(txn);
      await txn.commit();
      return result;
    } catch (_) {
      await txn.rollback();
      rethrow;
    }
  }

  void _assertOpen() {
    if (_closed) throw StateError('RaftDb has been closed');
  }
}
