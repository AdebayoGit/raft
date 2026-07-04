import 'package:hive/hive.dart';
import 'package:raft_bench/raft_bench_core.dart';

/// Hive adapter for the app. Identical logic to the CLI's HiveAdapter, but the
/// caller supplies the storage directory (from `path_provider`).
class HiveFlutterAdapter implements DbAdapter {
  Box<Map>? _box;

  @override
  String get name => 'Hive';

  @override
  String get version => '2.2.3';

  @override
  String get durabilityNote =>
      'Buffered — no fsync per commit even on flush(). Fast, not crash-durable.';

  @override
  Future<Availability> probe() async => const Availability.available();

  @override
  Future<void> openFresh(String dir) async {
    Hive.init(dir);
    _box = await Hive.openBox<Map>('bench');
    await _box!.clear();
  }

  Map<String, Object> _map(BenchDoc d) =>
      {'id': d.id, 'name': d.name, 'score': d.score, 'payload': d.payload};

  @override
  Future<void> bulkWrite(List<BenchDoc> docs) async =>
      _box!.putAll({for (final d in docs) d.id: _map(d)});

  @override
  Future<void> durableWrites(List<BenchDoc> docs) async {
    for (final d in docs) {
      await _box!.put(d.id, _map(d));
      await _box!.flush();
    }
  }

  @override
  Future<void> concurrentDurableWrites(List<List<BenchDoc>> chunks) async {
    await Future.wait([
      for (final chunk in chunks)
        () async {
          for (final d in chunk) {
            await _box!.put(d.id, _map(d));
            await _box!.flush();
          }
        }(),
    ]);
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
  Future<int> iterateAll() async => _box!.values.length;

  @override
  Future<void> bulkUpdate(List<BenchDoc> docs) async => _box!.putAll({
        for (final d in docs)
          d.id: _map(BenchDoc(
              id: d.id, name: d.name, score: d.score + 1, payload: d.payload)),
      });

  @override
  Future<void> bulkDelete(List<int> ids) async => _box!.deleteAll(ids);

  @override
  Future<void> close() async {
    await _box?.close();
    _box = null;
    await Hive.close();
  }

  @override
  Set<Workload> get supported => Workload.values.toSet();
}
