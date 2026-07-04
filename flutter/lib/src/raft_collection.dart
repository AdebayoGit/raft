import 'dart:ffi' as ffi;
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'raft_db.dart';
import 'raft_db_bindings.dart' as b;
import 'raft_doc.dart';
import 'raft_events.dart';

/// A typed, high-performance collection — raft's flagship API.
///
/// No code generation, no build_runner: you supply one `encode` and one
/// `decode` closure and get a fully typed collection backed by raft's
/// binary codec and prepared-handle FFI. Hot operations are synchronous —
/// a point read costs about a microsecond and cached reads never cross
/// the FFI boundary at all, so both are safe to call inside `build()`
/// even at 240 Hz.
///
/// ```dart
/// final db = await RaftDb.open(path);
/// final todos = db.collection<Todo>(
///   name: 'todos',
///   id: (t) => t.id,
///   encode: (t, w) => w
///     ..string('title', t.title)
///     ..boolean('done', t.done),
///   decode: (r) => Todo(
///     id: r.id,
///     title: r.string('title'),
///     done: r.boolean('done'),
///   ),
/// );
///
/// todos.putAll([Todo(id: 1, title: 'ship raft', done: false)]);
/// final todo = todos.get(1);            // ~1 µs
/// final hot = todos.getCached(1);       // no FFI crossing when unchanged
/// todos.watch().listen((_) => setState(() {}));
/// ```
///
/// Writes are durable: every commit is fsynced to stable storage before
/// the call returns. `putAll`/`deleteAll` batch any number of documents
/// into **one** commit (one WAL write, one fsync).
class RaftCollection<T> {
  RaftCollection.internal({
    required RaftDb db,
    required this.name,
    required T Function(RaftDocReader r) decode,
    required void Function(T value, RaftDocWriter w) encode,
    required int Function(T value) id,
    required b.RaftDbBindings bindings,
    required ffi.Pointer<b.RaftDb> dbHandle,
  }) : _db = db,
       _decode = decode,
       _encode = encode,
       _id = id,
       _b = bindings {
    final cName = name.toNativeUtf8();
    final out = calloc<ffi.Pointer<b.RaftCollection>>();
    try {
      final code = _b.rft_collection_open(dbHandle, cName.cast(), out);
      if (code != b.RftError.RFT_ERROR_OK.value) {
        throw RaftDbException.fromCode(code);
      }
      _coll = out.value;
      _genPtr = _b.rft_coll_generation(_coll).cast<ffi.Uint64>();
    } finally {
      malloc.free(cName);
      calloc.free(out);
    }
    // Allocated only after the native open succeeded — a throwing
    // constructor must not leak native memory (close() is the only
    // place these are freed, and it never runs for a discarded object).
    _readBuf = malloc<ffi.Uint8>(_readBufCap);
    _readLen = calloc<ffi.UintPtr>();
  }

  /// Collection name.
  final String name;

  final RaftDb _db;
  final T Function(RaftDocReader r) _decode;
  final void Function(T value, RaftDocWriter w) _encode;
  final int Function(T value) _id;
  final b.RaftDbBindings _b;

  late final ffi.Pointer<b.RaftCollection> _coll;
  late final ffi.Pointer<ffi.Uint64> _genPtr;
  var _closed = false;

  // Generation-stamped read-through cache: entries are valid while the
  // shared counter (bumped by every write, from any isolate) is unchanged.
  final Map<int, T> _cache = {};
  int _cacheGen = -1;

  // Reusable read buffer for the synchronous get path. Allocated at the
  // end of the constructor (after the native open succeeds) so a failed
  // open cannot leak them.
  static const _readBufCap = 16 * 1024;
  late final ffi.Pointer<ffi.Uint8> _readBuf;
  late final ffi.Pointer<ffi.UintPtr> _readLen;

  // ── Writes ────────────────────────────────────────────────────────────

  /// Insert or update one document. One durable commit.
  void put(T value) => putAll([value]);

  /// Insert or update any number of documents in **one atomic commit** —
  /// one WAL write, one fsync, regardless of count.
  void putAll(List<T> values) {
    _assertOpen();
    if (values.isEmpty) return;
    final batch = RaftWire.encodeBatch(
      values.length,
      (i, w) => _encode(values[i], w),
      (i) => _id(values[i]),
    );
    final ptr = malloc<ffi.Uint8>(batch.length);
    try {
      ptr.asTypedList(batch.length).setAll(0, batch);
      final code = _db.withNativeCollectionName(
        name,
        (cName) => _b.rft_collection_put_many(
          _db.nativeHandle,
          cName,
          ptr,
          batch.length,
        ),
      );
      if (code != b.RftError.RFT_ERROR_OK.value) {
        throw RaftDbException.fromCode(code);
      }
    } finally {
      malloc.free(ptr);
    }
  }

  /// Delete one document by id. Deleting a missing id is a no-op.
  void delete(int id) => deleteAll([id]);

  /// Delete any number of ids in one atomic commit.
  void deleteAll(List<int> ids) {
    _assertOpen();
    if (ids.isEmpty) return;
    final ptr = malloc<ffi.Uint64>(ids.length);
    try {
      for (var i = 0; i < ids.length; i++) {
        ptr[i] = ids[i];
      }
      final code = _db.withNativeCollectionName(
        name,
        (cName) => _b.rft_collection_delete_many(
          _db.nativeHandle,
          cName,
          ptr,
          ids.length,
        ),
      );
      if (code != b.RftError.RFT_ERROR_OK.value) {
        throw RaftDbException.fromCode(code);
      }
    } finally {
      malloc.free(ptr);
    }
  }

  // ── Reads ─────────────────────────────────────────────────────────────

  /// Fetch a document by id, or `null` if absent. Synchronous — one FFI
  /// call, about a microsecond.
  T? get(int id) {
    _assertOpen();
    _readLen.value = _readBufCap;
    final code = _b.rft_coll_get_buf(_coll, id, _readBuf, _readLen);
    if (code == b.RftError.RFT_ERROR_NOT_FOUND.value) return null;
    if (code == b.RftError.RFT_ERROR_BUFFER_TOO_SMALL.value) {
      return _getLarge(id, _readLen.value);
    }
    if (code != b.RftError.RFT_ERROR_OK.value) {
      throw RaftDbException.fromCode(code);
    }
    final view = _readBuf.asTypedList(_readBufCap);
    return _decode(
      RaftWire.decodeDoc(
        view,
        ByteData.view(view.buffer, view.offsetInBytes, _readBufCap),
        0,
        _readLen.value,
      ),
    );
  }

  T? _getLarge(int id, int needed) {
    final buf = malloc<ffi.Uint8>(needed);
    final lenPtr = calloc<ffi.UintPtr>()..value = needed;
    try {
      final code = _b.rft_coll_get_buf(_coll, id, buf, lenPtr);
      if (code == b.RftError.RFT_ERROR_NOT_FOUND.value) return null;
      if (code != b.RftError.RFT_ERROR_OK.value) {
        throw RaftDbException.fromCode(code);
      }
      final view = buf.asTypedList(needed);
      return _decode(
        RaftWire.decodeDoc(
          view,
          ByteData.view(view.buffer, view.offsetInBytes, needed),
          0,
          lenPtr.value,
        ),
      );
    } finally {
      malloc.free(buf);
      calloc.free(lenPtr);
    }
  }

  /// Fetch a document through the read-through cache. When nothing in the
  /// collection has changed, this is a plain map lookup plus one shared-
  /// memory load — **no FFI crossing** — making it safe to call in the
  /// frame loop at any refresh rate. Any write to the collection (from
  /// any isolate) invalidates the cache; the next read refetches.
  ///
  /// **Treat returned objects as immutable.** Cache hits return the same
  /// instance each time (that is what makes them free); mutating it in
  /// place changes what later hits see without writing anything to the
  /// database. To change a document, write a new value with [put].
  T? getCached(int id) {
    _assertOpen();
    final gen = _genPtr.value;
    if (gen != _cacheGen) {
      _cache.clear();
      _cacheGen = gen;
    }
    final hit = _cache[id];
    if (hit != null) return hit;
    final value = get(id);
    if (value != null) _cache[id] = value;
    return value;
  }

  /// Fetch many documents by id in **one FFI crossing**. Missing ids are
  /// skipped. The fast path for hydrating list views.
  List<T> getMany(List<int> ids) {
    _assertOpen();
    if (ids.isEmpty) return const [];
    final idsPtr = malloc<ffi.Uint64>(ids.length);
    final out = calloc<ffi.Pointer<b.RftBuf>>();
    try {
      for (var i = 0; i < ids.length; i++) {
        idsPtr[i] = ids[i];
      }
      final code = _b.rft_coll_get_many(_coll, idsPtr, ids.length, out);
      if (code != b.RftError.RFT_ERROR_OK.value) {
        throw RaftDbException.fromCode(code);
      }
      return _drainBuf(out.value);
    } finally {
      malloc.free(idsPtr);
      calloc.free(out);
    }
  }

  /// Read every document in the collection (ids ascending) in one engine
  /// pass.
  List<T> all() {
    _assertOpen();
    final out = calloc<ffi.Pointer<b.RftBuf>>();
    try {
      final code = _db.withNativeCollectionName(
        name,
        (cName) => _b.rft_collection_scan(_db.nativeHandle, cName, out),
      );
      if (code != b.RftError.RFT_ERROR_OK.value) {
        throw RaftDbException.fromCode(code);
      }
      return _drainBuf(out.value);
    } finally {
      calloc.free(out);
    }
  }

  List<T> _drainBuf(ffi.Pointer<b.RftBuf> buf) {
    try {
      final len = _b.rft_buf_len(buf);
      if (len == 0) return const [];
      final data = _b.rft_buf_data(buf).asTypedList(len);
      final readers = RaftWire.decodeBatch(data);
      return [for (final r in readers) _decode(r)];
    } finally {
      _b.rft_buf_free(buf);
    }
  }

  /// Number of documents in the collection.
  int count() {
    _assertOpen();
    final out = calloc<ffi.UintPtr>();
    try {
      final code = _db.withNativeCollectionName(
        name,
        (cName) => _b.rft_collection_count(_db.nativeHandle, cName, out),
      );
      if (code != b.RftError.RFT_ERROR_OK.value) {
        throw RaftDbException.fromCode(code);
      }
      return out.value;
    } finally {
      calloc.free(out);
    }
  }

  // ── Reactive ──────────────────────────────────────────────────────────

  /// Stream of mutation events for this collection — fires on every
  /// insert, update, or delete, from any isolate.
  Stream<MutationEvent> watch() => _db.observeCollection(name);

  /// Whether [close] has been called.
  bool get isClosed => _closed;

  /// Release the native handle. Called automatically by [RaftDb.close];
  /// call it earlier if the collection's lifetime is shorter than the
  /// database's.
  void close() {
    if (_closed) return;
    _closed = true;
    _b.rft_collection_close(_coll);
    malloc.free(_readBuf);
    calloc.free(_readLen);
    _cache.clear();
  }

  void _assertOpen() {
    if (_closed) throw StateError('collection "$name" has been closed');
  }
}
