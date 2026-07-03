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

/// Drives Isar (community fork) through its idiomatic transactional API.
class IsarAdapter implements DbAdapter {
  Isar? _isar;

  @override
  String get name => 'Isar';

  @override
  String get version => 'isar_community 3.1.x';

  @override
  String get durabilityNote =>
      'Native mmap engine; write transactions are durable. Runs on the calling '
      'isolate.';

  @override
  Future<Availability> probe() async => const Availability.available();

  @override
  Future<void> openFresh(String dir) async {
    _isar = await Isar.open([IsarDocSchema], directory: dir, name: 'bench');
    await _isar!.writeTxn(() => _isar!.isarDocs.clear());
  }

  IsarDoc _entity(BenchDoc d) =>
      IsarDoc(id: d.id, name: d.name, score: d.score, payload: d.payload);

  @override
  Future<void> bulkWrite(List<BenchDoc> docs) async {
    final entities = docs.map(_entity).toList();
    await _isar!.writeTxn(() => _isar!.isarDocs.putAll(entities));
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
    final entities = docs
        .map((d) => IsarDoc(
            id: d.id, name: d.name, score: d.score + 1, payload: d.payload))
        .toList();
    await _isar!.writeTxn(() => _isar!.isarDocs.putAll(entities));
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
}
