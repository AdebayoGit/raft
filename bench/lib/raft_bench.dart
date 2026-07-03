/// Shared benchmark harness: models, workloads, the runner, results
/// serialization, and the engine adapters. Imported by the CLI (`bin/bench.dart`)
/// and reused by the Flutter benchmark app.
library;

export 'src/adapter.dart';
export 'src/harness.dart';
export 'src/model.dart';
export 'src/results.dart';
export 'src/adapters/raft_codec.dart';
export 'src/adapters/raft_adapter.dart';
export 'src/adapters/hive_adapter.dart';
export 'src/adapters/sqlite_adapter.dart';
