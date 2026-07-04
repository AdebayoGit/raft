import 'package:path/path.dart' as p;
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

import '../adapter.dart';
import '../model.dart';

/// Drives SQLite via `sqflite_common_ffi` (the same engine `sqflite` uses on a
/// device, driven through the standalone-Dart FFI factory).
///
/// Schema: `bench(id INTEGER PRIMARY KEY, name TEXT, score INTEGER, payload
/// TEXT)`. Durability is pinned to WAL + `synchronous=FULL` + `fullfsync=ON`
/// so its durable-write group flushes to stable storage exactly like raft's.
class SqliteAdapter implements DbAdapter {
  static var _initialised = false;
  Database? _db;

  @override
  String get name => 'SQLite (sqflite_ffi)';

  @override
  String get version => 'bundled sqlite via sqflite_common_ffi';

  @override
  String get durabilityNote =>
      'WAL, synchronous=FULL, fullfsync=ON — durable group fsyncs per commit, '
      'matched to raft.';

  @override
  Future<Availability> probe() async {
    try {
      if (!_initialised) {
        sqfliteFfiInit();
        _initialised = true;
      }
      databaseFactory = databaseFactoryFfi;
      return const Availability.available();
    } catch (e) {
      return Availability.unavailable('sqflite_ffi init failed: $e');
    }
  }

  @override
  Future<void> openFresh(String dir) async {
    final path = p.join(dir, 'bench.db');
    _db = await databaseFactory.openDatabase(path);
    await _db!.execute('PRAGMA journal_mode=WAL');
    await _db!.execute('PRAGMA synchronous=FULL');
    await _db!.execute('PRAGMA fullfsync=ON');
    await _db!.execute('PRAGMA checkpoint_fullfsync=ON');
    await _db!.execute(
      'CREATE TABLE bench('
      'id INTEGER PRIMARY KEY, name TEXT NOT NULL, '
      'score INTEGER NOT NULL, payload TEXT NOT NULL)',
    );
  }

  @override
  Future<void> bulkWrite(List<BenchDoc> docs) async {
    await _db!.transaction((txn) async {
      final batch = txn.batch();
      for (final d in docs) {
        batch.insert('bench', {
          'id': d.id,
          'name': d.name,
          'score': d.score,
          'payload': d.payload,
        });
      }
      await batch.commit(noResult: true);
    });
  }

  @override
  Future<void> durableWrites(List<BenchDoc> docs) async {
    // Autocommit: each insert is its own durable transaction.
    for (final d in docs) {
      await _db!.insert('bench', {
        'id': d.id,
        'name': d.name,
        'score': d.score,
        'payload': d.payload,
      });
    }
  }

  @override
  Future<void> concurrentDurableWrites(List<List<BenchDoc>> chunks) async {
    // sqflite serialises statements on the database internally; concurrent
    // clients interleave at the API level — its real concurrent behaviour.
    await Future.wait([
      for (final chunk in chunks)
        () async {
          for (final d in chunk) {
            await _db!.insert('bench', {
              'id': d.id,
              'name': d.name,
              'score': d.score,
              'payload': d.payload,
            });
          }
        }(),
    ]);
  }

  @override
  Future<int> pointReads(List<int> ids) async {
    var found = 0;
    for (final id in ids) {
      final rows = await _db!.query(
        'bench',
        columns: ['id', 'name', 'score', 'payload'],
        where: 'id = ?',
        whereArgs: [id],
        limit: 1,
      );
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
  Future<int> iterateAll() async {
    final rows = await _db!.query('bench');
    return rows.length;
  }

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
