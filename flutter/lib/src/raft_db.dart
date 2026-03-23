import 'dart:ffi' as ffi;
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'raft_db_bindings.dart' as bindings;

/// Loads the native RaftDB library for the current platform.
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

  factory RaftDbException._fromCode(int code) {
    final message = switch (code) {
      1 => 'Null pointer argument',
      2 => 'Invalid UTF-8 in path or key',
      3 => 'I/O or storage engine error',
      4 => 'Key not found',
      5 => 'Buffer too small',
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
          throw RaftDbException._fromCode(code);
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
        final code =
            db.rft_put(handle, keyPtr, key.length, valPtr, value.length);
        if (code != bindings.RftError.RFT_ERROR_OK.value) {
          throw RaftDbException._fromCode(code);
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
          throw RaftDbException._fromCode(code);
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
        final sizeCode =
            db.rft_get(handle, keyPtr, key.length, ffi.nullptr, lenPtr);
        if (sizeCode == bindings.RftError.RFT_ERROR_NOT_FOUND.value) {
          return null;
        }
        if (sizeCode != bindings.RftError.RFT_ERROR_BUFFER_TOO_SMALL.value &&
            sizeCode != bindings.RftError.RFT_ERROR_OK.value) {
          throw RaftDbException._fromCode(sizeCode);
        }

        // Phase 2: allocate exact buffer and read.
        final required = lenPtr.value;
        final bufPtr = malloc<ffi.Uint8>(required);
        try {
          final readCode =
              db.rft_get(handle, keyPtr, key.length, bufPtr, lenPtr);
          if (readCode != bindings.RftError.RFT_ERROR_OK.value) {
            throw RaftDbException._fromCode(readCode);
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

  void _assertOpen() {
    if (_closed) throw StateError('RaftDb has been closed');
  }
}
