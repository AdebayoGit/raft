import 'dart:ffi' as ffi;
import 'dart:io';

import 'package:raft_bench/raft_bench_core.dart';
import 'package:raft_bench/raft_bench_ffi.dart';

/// Builds a [RaftAdapter] wired to load the native raft library the way each
/// platform ships it.
///
/// - Android: `libraftdb.so`, bundled by the `raft_db_flutter` plugin.
/// - iOS: statically linked — symbols live in the process image.
/// - macOS: no plugin bundle yet, so load the dev dylib by path (override with
///   the `RAFT_DYLIB` environment variable).
DbAdapter buildRaftAdapter() {
  return RaftAdapter(
    availabilityCheck: () {
      if (Platform.isMacOS) {
        final path = _macosDylibPath();
        if (path == null) {
          return const Availability.unavailable(
            'raft dylib not found on macOS — build it with '
            '`cargo build --release --features ffi` or set RAFT_DYLIB',
          );
        }
      }
      return const Availability.available();
    },
    loadLibrary: () {
      if (Platform.isAndroid) return ffi.DynamicLibrary.open('libraftdb.so');
      if (Platform.isIOS) return ffi.DynamicLibrary.process();
      if (Platform.isMacOS) {
        final path = _macosDylibPath();
        if (path != null) return ffi.DynamicLibrary.open(path);
        return ffi.DynamicLibrary.process();
      }
      if (Platform.isLinux) return ffi.DynamicLibrary.open('libraftdb.so');
      if (Platform.isWindows) return ffi.DynamicLibrary.open('raftdb.dll');
      throw UnsupportedError('Unsupported platform for raft-db');
    },
    versionLabel: '0.1.0 (FFI, collection store)',
  );
}

String? _macosDylibPath() {
  final env = Platform.environment['RAFT_DYLIB'];
  if (env != null && File(env).existsSync()) return env;
  // Walk up from the executable / cwd to find the repo's built dylib.
  final candidates = <String>[
    for (final root in _repoRoots())
      ...[
        '$root/core/target/x86_64-apple-darwin/release/libraftdb.dylib',
        '$root/core/target/aarch64-apple-darwin/release/libraftdb.dylib',
        '$root/core/target/release/libraftdb.dylib',
      ],
  ];
  for (final c in candidates) {
    if (File(c).existsSync()) return c;
  }
  return null;
}

Iterable<String> _repoRoots() sync* {
  var dir = Directory.current;
  for (var i = 0; i < 8; i++) {
    yield dir.path;
    final parent = dir.parent;
    if (parent.path == dir.path) break;
    dir = parent;
  }
}
