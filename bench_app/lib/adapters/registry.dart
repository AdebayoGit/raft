import 'package:raft_bench/raft_bench_core.dart';

import 'hive_flutter_adapter.dart';
import 'isar_adapter.dart';
import 'objectbox_adapter.dart';
import 'raft_flutter_adapter.dart';
import 'realm_adapter.dart';
import 'sqflite_adapter.dart';

/// One selectable engine in the app.
class EngineEntry {
  EngineEntry(this.key, this.build, {this.enabledByDefault = true});
  final String key;
  final DbAdapter Function() build;
  final bool enabledByDefault;
}

/// All engines the app can benchmark. Order is display order.
final List<EngineEntry> engineRegistry = [
  EngineEntry('raft-db', buildRaftAdapter),
  EngineEntry('SQLite (sqflite)', SqfliteAppAdapter.new),
  EngineEntry('Hive', HiveFlutterAdapter.new),
  EngineEntry('Isar', IsarAdapter.new),
  EngineEntry('ObjectBox', ObjectBoxAdapter.new),
  EngineEntry('Realm', RealmAdapter.new),
];
