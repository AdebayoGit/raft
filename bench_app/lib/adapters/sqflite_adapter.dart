import 'package:path/path.dart' as p;
import 'package:raft_bench/raft_bench_core.dart';
import 'package:sqflite/sqflite.dart';

/// SQLite via the `sqflite` plugin — the on-device Flutter SQLite path. Every
/// call crosses the plugin's platform channel, so single-op latencies include
/// that dispatch (as any real sqflite app pays).
class SqfliteAppAdapter implements DbAdapter {
  Database? _db;

  @override
  String get name => 'SQLite (sqflite)';

  @override
  String get version => 'sqflite plugin';

  @override
  String get durabilityNote =>
      'WAL, synchronous=FULL, fullfsync=ON. Per-call platform-channel dispatch '
      'inflates single-op latency.';

  @override
  Future<Availability> probe() async => const Availability.available();

  @override
  Future<void> openFresh(String dir) async {
    final path = p.join(dir, 'bench.db');
    _db = await openDatabase(path, version: 1, onCreate: (db, _) async {
      await db.execute(
        'CREATE TABLE bench(id INTEGER PRIMARY KEY, name TEXT NOT NULL, '
        'score INTEGER NOT NULL, payload TEXT NOT NULL)',
      );
    });
    await _db!.execute('PRAGMA journal_mode=WAL');
    await _db!.execute('PRAGMA synchronous=FULL');
    await _db!.execute('PRAGMA fullfsync=ON');
  }

  Map<String, Object?> _row(BenchDoc d) =>
      {'id': d.id, 'name': d.name, 'score': d.score, 'payload': d.payload};

  @override
  Future<void> bulkWrite(List<BenchDoc> docs) async {
    await _db!.transaction((txn) async {
      final batch = txn.batch();
      for (final d in docs) {
        batch.insert('bench', _row(d));
      }
      await batch.commit(noResult: true);
    });
  }

  @override
  Future<void> durableWrites(List<BenchDoc> docs) async {
    for (final d in docs) {
      await _db!.insert('bench', _row(d));
    }
  }

  @override
  Future<void> concurrentDurableWrites(List<List<BenchDoc>> chunks) async {
    await Future.wait([
      for (final chunk in chunks)
        () async {
          for (final d in chunk) {
            await _db!.insert('bench', _row(d));
          }
        }(),
    ]);
  }

  @override
  Future<int> pointReads(List<int> ids) async {
    var found = 0;
    for (final id in ids) {
      final rows =
          await _db!.query('bench', where: 'id = ?', whereArgs: [id], limit: 1);
      if (rows.isNotEmpty) found++;
    }
    return found;
  }

  @override
  Future<int> readMany(List<int> ids) async {
    var found = 0;
    for (var i = 0; i < ids.length; i += 500) {
      final chunk = ids.sublist(i, (i + 500).clamp(0, ids.length));
      final marks = List.filled(chunk.length, '?').join(',');
      final rows = await _db!.query('bench',
          where: 'id IN ($marks)', whereArgs: chunk);
      found += rows.length;
    }
    return found;
  }

  @override
  bool get supportsCachedReads => false;

  @override
  Future<int> cachedPointReads(List<int> ids) =>
      throw UnsupportedError('no correctness-preserving cache mode');

  @override
  Future<int> iterateAll() async => (await _db!.query('bench')).length;

  @override
  Future<void> bulkUpdate(List<BenchDoc> docs) async {
    await _db!.transaction((txn) async {
      final batch = txn.batch();
      for (final d in docs) {
        batch.update('bench', {'score': d.score + 1},
            where: 'id = ?', whereArgs: [d.id]);
      }
      await batch.commit(noResult: true);
    });
  }

  @override
  Future<void> bulkDelete(List<int> ids) async {
    await _db!.transaction((txn) async {
      final batch = txn.batch();
      for (final id in ids) {
        batch.delete('bench', where: 'id = ?', whereArgs: [id]);
      }
      await batch.commit(noResult: true);
    });
  }

  @override
  Future<void> close() async {
    await _db?.close();
    _db = null;
  }

  @override
  Set<Workload> get supported => Workload.values.toSet();
}
