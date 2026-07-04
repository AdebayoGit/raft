import 'package:path/path.dart' as p;
import 'package:raft_bench/raft_bench_core.dart';
import 'package:realm/realm.dart';

part 'realm_adapter.realm.dart';

/// Realm model mirroring [BenchDoc]. Realm generates `RealmDoc` from this.
@RealmModel()
class _RealmDoc {
  @PrimaryKey()
  late int id;
  late String name;
  late int score;
  late String payload;
}

/// Drives Realm through its write-transaction API.
class RealmAdapter implements DbAdapter {
  Realm? _realm;

  @override
  String get name => 'Realm';

  @override
  String get version => 'realm 20.x';

  @override
  String get durabilityNote =>
      'C++ core, MVCC; write() transactions are durable (commit fsyncs).';

  @override
  Future<Availability> probe() async => const Availability.available();

  @override
  Future<void> openFresh(String dir) async {
    final config = Configuration.local(
      [RealmDoc.schema],
      path: p.join(dir, 'bench.realm'),
    );
    _realm = Realm(config);
    _realm!.write(() => _realm!.deleteAll<RealmDoc>());
  }

  RealmDoc _obj(BenchDoc d) => RealmDoc(d.id, d.name, d.score, d.payload);

  @override
  Future<void> bulkWrite(List<BenchDoc> docs) async {
    _realm!.write(() => _realm!.addAll(docs.map(_obj), update: true));
  }

  @override
  Future<void> durableWrites(List<BenchDoc> docs) async {
    for (final d in docs) {
      _realm!.write(() => _realm!.add(_obj(d), update: true));
    }
  }

  @override
  Future<void> concurrentDurableWrites(List<List<BenchDoc>> chunks) async {
    await Future.wait([
      for (final chunk in chunks)
        () async {
          for (final d in chunk) {
            _realm!.write(() => _realm!.add(_obj(d), update: true));
          }
        }(),
    ]);
  }

  @override
  Future<int> pointReads(List<int> ids) async {
    var found = 0;
    for (final id in ids) {
      if (_realm!.find<RealmDoc>(id) != null) found++;
    }
    return found;
  }

  @override
  Future<int> readMany(List<int> ids) async {
    var found = 0;
    for (final id in ids) {
      if (_realm!.find<RealmDoc>(id) != null) found++;
    }
    return found;
  }

  @override
  bool get supportsCachedReads => false;

  @override
  Future<int> cachedPointReads(List<int> ids) =>
      throw UnsupportedError('no correctness-preserving cache mode');

  @override
  Future<int> iterateAll() async => _realm!.all<RealmDoc>().length;

  @override
  Future<void> bulkUpdate(List<BenchDoc> docs) async {
    _realm!.write(() {
      for (final d in docs) {
        final obj = _realm!.find<RealmDoc>(d.id);
        if (obj != null) obj.score = d.score + 1;
      }
    });
  }

  @override
  Future<void> bulkDelete(List<int> ids) async {
    _realm!.write(() {
      for (final id in ids) {
        final obj = _realm!.find<RealmDoc>(id);
        if (obj != null) _realm!.delete(obj);
      }
    });
  }

  @override
  Future<void> close() async {
    _realm?.close();
    _realm = null;
  }

  @override
  Set<Workload> get supported => Workload.values.toSet();
}
