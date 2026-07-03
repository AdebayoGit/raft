/// Flutter-safe Raft adapter + raw FFI bindings (dart:ffi only, no
/// `sqflite_common_ffi`). The Flutter app imports this to benchmark raft-db
/// through the exact same code path as the headless CLI.
library;

export 'src/adapters/raft_adapter.dart';
export 'src/adapters/raft_ffi.dart';
