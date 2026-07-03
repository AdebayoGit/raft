import 'dart:io';

import 'package:raft_bench/raft_bench_core.dart';

import '../objectbox.g.dart';

/// ObjectBox entity mirroring [BenchDoc].
@Entity()
class ObxDoc {
  ObxDoc({
    required this.id,
    required this.name,
    required this.score,
    required this.payload,
  });

  @Id(assignable: true)
  int id;
  String name;
  int score;
  String payload;
}

/// Drives ObjectBox under the standalone Dart VM.
class ObjectBoxAdapter implements DbAdapter {
  Store? _store;
  Box<ObxDoc>? _box;

  @override
  String get name => 'ObjectBox';

  @override
  String get version => 'objectbox 4.0.x (Dart VM)';

  @override
  String get durabilityNote =>
      'Native object store; putMany is one transaction, single put is durable.';

  @override
  Future<Availability> probe() async {
    // Actually try to open a throwaway store: this is what forces the native
    // libobjectbox load, so a missing native lib is caught here (not mid-run).
    Directory? tmp;
    try {
      tmp = Directory.systemTemp.createTempSync('obx_probe_');
      final s = openStore(directory: tmp.path);
      s.close();
      return const Availability.available();
    } catch (e) {
      return Availability.unavailable(
          'libobjectbox not installed for Dart VM (needs `objectbox` install '
          'script / download); benchmark ObjectBox via the Flutter app instead');
    } finally {
      try {
        tmp?.deleteSync(recursive: true);
      } catch (_) {}
    }
  }

  @override
  Future<void> openFresh(String dir) async {
    _store = openStore(directory: dir);
    _box = _store!.box<ObxDoc>();
    _box!.removeAll();
  }

  ObxDoc _entity(BenchDoc d) =>
      ObxDoc(id: d.id, name: d.name, score: d.score, payload: d.payload);

  @override
  Future<void> bulkWrite(List<BenchDoc> docs) async =>
      _box!.putMany(docs.map(_entity).toList());

  @override
  Future<void> durableWrites(List<BenchDoc> docs) async {
    for (final d in docs) {
      _box!.put(_entity(d));
    }
  }

  @override
  Future<int> pointReads(List<int> ids) async {
    var found = 0;
    for (final id in ids) {
      if (_box!.get(id) != null) found++;
    }
    return found;
  }

  @override
  Future<int> iterateAll() async => _box!.getAll().length;

  @override
  Future<void> bulkUpdate(List<BenchDoc> docs) async => _box!.putMany(docs
      .map((d) => ObxDoc(
          id: d.id, name: d.name, score: d.score + 1, payload: d.payload))
      .toList());

  @override
  Future<void> bulkDelete(List<int> ids) async => _box!.removeMany(ids);

  @override
  Future<void> close() async {
    _store?.close();
    _store = null;
    _box = null;
  }

  @override
  Set<Workload> get supported => Workload.values.toSet();
}
