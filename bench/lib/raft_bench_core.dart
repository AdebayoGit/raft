/// Flutter-safe subset of the harness: models, workloads, the runner, results,
/// and the adapter interface — but NOT the CLI's concrete adapters (which pull
/// in `sqflite_common_ffi`). The Flutter app imports this and supplies its own
/// Flutter-native adapters.
library;

export 'src/adapter.dart';
export 'src/harness.dart';
export 'src/model.dart';
export 'src/results.dart';
