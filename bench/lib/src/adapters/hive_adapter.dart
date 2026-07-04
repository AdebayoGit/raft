import 'package:hive/hive.dart';

import '../adapter.dart';
import '../model.dart';

/// Drives Hive — a pure-Dart NoSQL box store — through its idiomatic API.
///
/// Records are stored as plain maps keyed by their integer id.
class HiveAdapter implements DbAdapter {
  Box<Map>? _box;

  @override
  String get name => 'Hive';

  @override
  String get version => '2.2.3 (pure Dart)';

  @override
  String get durabilityNote =>
      'Buffered: writes append to the box file; no fsync per commit even on '
      'flush(). Fast but not crash-durable per write.';

  @override
  Future<Availability> probe() async => const Availability.available();

  @override
  Future<void> openFresh(String dir) async {
    Hive.init(dir);
    _box = await Hive.openBox<Map>('bench');
    await _box!.clear();
  }

  Map<String, Object> _map(BenchDoc d) => {
        'id': d.id,
        'name': d.name,
        'score': d.score,
        'payload': d.payload,
      };

  @override
  Future<void> bulkWrite(List<BenchDoc> docs) async {
    await _box!.putAll({for (final d in docs) d.id: _map(d)});
  }

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
  Future<int> readMany(List<int> ids) async => pointReads(ids);

  @override
  bool get supportsCachedReads => false;

  @override
  Future<int> cachedPointReads(List<int> ids) =>
      throw UnsupportedError('no correctness-preserving cache mode');

  @override
  Future<int> iterateAll() async {
    var count = 0;
    for (final _ in _box!.values) {
      count++;
    }
    return count;
  }

  @override
  Future<void> bulkUpdate(List<BenchDoc> docs) async {
    await _box!.putAll({
      for (final d in docs)
        d.id: _map(BenchDoc(
            id: d.id, name: d.name, score: d.score + 1, payload: d.payload)),
    });
  }

  @override
  Future<void> bulkDelete(List<int> ids) async {
    await _box!.deleteAll(ids);
  }

  @override
  Future<void> close() async {
    await _box?.close();
    _box = null;
    await Hive.close();
  }

  @override
  Set<Workload> get supported => Workload.values.toSet();
}
