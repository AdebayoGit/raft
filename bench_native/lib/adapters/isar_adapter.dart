import 'dart:ffi';
import 'dart:io';

import 'package:isar_community/isar.dart';
import 'package:raft_bench/raft_bench_core.dart';

part 'isar_adapter.g.dart';

/// Isar collection entity mirroring [BenchDoc].
@collection
class IsarDoc {
  IsarDoc({
    required this.id,
    required this.name,
    required this.score,
    required this.payload,
  });

  final Id id;
  final String name;
  final int score;
  final String payload;
}

/// Drives Isar (community fork) under the standalone Dart VM, loading the
/// native core from the `isar_community_flutter_libs` bundle already in the
/// pub cache (no download).
class IsarAdapter implements DbAdapter {
  Isar? _isar;
  static bool _coreInit = false;

  @override
  String get name => 'Isar';

  @override
  String get version => 'isar_community 3.x (Dart VM)';

  @override
  String get durabilityNote =>
      'Native mmap engine; write transactions are durable.';

  @override
  Future<Availability> probe() async {
    final lib = _findLibisar();
    if (lib == null) {
      return const Availability.unavailable(
          'libisar.dylib not found in pub cache — add isar_community_flutter_libs');
    }
    try {
      if (!_coreInit) {
        await Isar.initializeIsarCore(libraries: {
          Abi.current(): lib,
        });
        _coreInit = true;
      }
      return const Availability.available();
    } catch (e) {
      return Availability.unavailable('isar core init failed: $e');
    }
  }

  @override
  Future<void> openFresh(String dir) async {
    _isar = await Isar.open([IsarDocSchema], directory: dir, name: 'bench');
    await _isar!.writeTxn(() => _isar!.isarDocs.clear());
  }

  IsarDoc _entity(BenchDoc d) =>
      IsarDoc(id: d.id, name: d.name, score: d.score, payload: d.payload);

  @override
  Future<void> bulkWrite(List<BenchDoc> docs) async {
    final e = docs.map(_entity).toList();
    await _isar!.writeTxn(() => _isar!.isarDocs.putAll(e));
  }

  @override
  Future<void> durableWrites(List<BenchDoc> docs) async {
    for (final d in docs) {
      await _isar!.writeTxn(() => _isar!.isarDocs.put(_entity(d)));
    }
  }

  @override
  Future<int> pointReads(List<int> ids) async {
    var found = 0;
    for (final id in ids) {
      if (await _isar!.isarDocs.get(id) != null) found++;
    }
    return found;
  }

  @override
  Future<int> iterateAll() async => _isar!.isarDocs.where().count();

  @override
  Future<void> bulkUpdate(List<BenchDoc> docs) async {
    final e = docs
        .map((d) => IsarDoc(
            id: d.id, name: d.name, score: d.score + 1, payload: d.payload))
        .toList();
    await _isar!.writeTxn(() => _isar!.isarDocs.putAll(e));
  }

  @override
  Future<void> bulkDelete(List<int> ids) async {
    await _isar!.writeTxn(() => _isar!.isarDocs.deleteAll(ids));
  }

  @override
  Future<void> close() async {
    await _isar?.close();
    _isar = null;
  }

  @override
  Set<Workload> get supported => Workload.values.toSet();

  /// Locate a bundled `libisar.dylib` in the pub cache.
  static String? _findLibisar() {
    final home = Platform.environment['HOME'];
    if (home == null) return null;
    final roots = [
      Directory('$home/.pub-cache/hosted/pub.dev'),
    ];
    for (final root in roots) {
      if (!root.existsSync()) continue;
      for (final entity in root.listSync()) {
        if (entity is Directory &&
            entity.path.contains('isar_community_flutter_libs')) {
          final lib = File('${entity.path}/macos/libisar.dylib');
          if (lib.existsSync()) return lib.path;
        }
      }
    }
    return null;
  }
}
