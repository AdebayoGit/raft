import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:path_provider/path_provider.dart';
import 'package:raft_db_flutter/raft_db_flutter.dart';

/// A plain Dart class — no annotations, no codegen, no base class.
class Todo {
  Todo({required this.id, required this.title, required this.done});
  final int id;
  final String title;
  final bool done;
}

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  late RaftDb db;
  late String dbPath;

  setUpAll(() async {
    final dir = await getApplicationDocumentsDirectory();
    dbPath = '${dir.path}${Platform.pathSeparator}raft_integration_test';
    db = await RaftDb.open(dbPath);
  });

  tearDownAll(() async {
    await db.close();
  });

  final key = Uint8List.fromList(utf8.encode('test_key'));
  final value = Uint8List.fromList(utf8.encode('test_value'));

  testWidgets('put and get round-trip', (tester) async {
    await db.put(key, value);
    final result = await db.get(key);
    expect(result, isNotNull);
    expect(utf8.decode(result!), 'test_value');
  });

  testWidgets('get returns null for missing key', (tester) async {
    final missing = Uint8List.fromList(utf8.encode('no_such_key'));
    final result = await db.get(missing);
    expect(result, isNull);
  });

  testWidgets('delete removes a key', (tester) async {
    await db.put(key, value);
    await db.delete(key);
    final result = await db.get(key);
    expect(result, isNull);
  });

  testWidgets('put overwrites existing value', (tester) async {
    final updated = Uint8List.fromList(utf8.encode('updated_value'));
    await db.put(key, value);
    await db.put(key, updated);
    final result = await db.get(key);
    expect(utf8.decode(result!), 'updated_value');
  });

  testWidgets('operations on closed db throw StateError', (tester) async {
    final dir = await getApplicationDocumentsDirectory();
    final closedPath = '${dir.path}${Platform.pathSeparator}raft_closed_test';
    final closedDb = await RaftDb.open(closedPath);
    await closedDb.close();

    expect(() => closedDb.get(key), throwsStateError);
    expect(() => closedDb.put(key, value), throwsStateError);
    expect(() => closedDb.delete(key), throwsStateError);
    expect(() => closedDb.close(), throwsStateError);
  });

  // ── Typed collections (no codegen) ───────────────────────────────────

  RaftCollection<Todo> todos(String name) => db.collection<Todo>(
    name: name,
    id: (t) => t.id,
    encode: (t, w) => w
      ..string('title', t.title)
      ..boolean('done', t.done),
    decode: (r) =>
        Todo(id: r.id, title: r.string('title'), done: r.boolean('done')),
  );

  testWidgets('typed collection put/get round-trip', (tester) async {
    final coll = todos('todos_rt');
    coll.put(Todo(id: 1, title: 'ship raft', done: false));
    final loaded = coll.get(1);
    expect(loaded, isNotNull);
    expect(loaded!.title, 'ship raft');
    expect(loaded.done, isFalse);
    expect(coll.get(999), isNull);
  });

  testWidgets('putAll commits a batch atomically and all() scans it', (
    tester,
  ) async {
    final coll = todos('todos_batch');
    coll.putAll([
      for (var i = 1; i <= 50; i++) Todo(id: i, title: 'task $i', done: false),
    ]);
    expect(coll.count(), 50);
    final all = coll.all();
    expect(all.length, 50);
    expect(all.first.id, 1);
    expect(all.last.id, 50);
  });

  testWidgets('getMany hydrates a list in one crossing', (tester) async {
    final coll = todos('todos_many');
    coll.putAll([
      for (var i = 1; i <= 10; i++) Todo(id: i, title: 't$i', done: i.isEven),
    ]);
    final picked = coll.getMany([2, 999, 5, 9]);
    expect(picked.map((t) => t.id), [2, 5, 9]);
  });

  testWidgets('getCached serves hot reads and invalidates on write', (
    tester,
  ) async {
    final coll = todos('todos_cache');
    coll.put(Todo(id: 7, title: 'before', done: false));
    expect(coll.getCached(7)!.title, 'before');
    // Overwrite through the same collection: the shared generation
    // counter bumps and the cache must refetch.
    coll.put(Todo(id: 7, title: 'after', done: true));
    expect(coll.getCached(7)!.title, 'after');
    // Delete invalidates too.
    coll.delete(7);
    expect(coll.getCached(7), isNull);
  });

  testWidgets('deleteAll removes a batch in one commit', (tester) async {
    final coll = todos('todos_delall');
    coll.putAll([
      for (var i = 1; i <= 20; i++) Todo(id: i, title: 't', done: false),
    ]);
    coll.deleteAll([for (var i = 1; i <= 20; i++) i]);
    expect(coll.count(), 0);
  });

  testWidgets('collections with different names do not collide', (
    tester,
  ) async {
    final a = todos('coll_a');
    final b = todos('coll_b');
    a.put(Todo(id: 1, title: 'in-a', done: false));
    b.put(Todo(id: 1, title: 'in-b', done: true));
    expect(a.get(1)!.title, 'in-a');
    expect(b.get(1)!.title, 'in-b');
  });

  testWidgets('watch fires on writes to the collection', (tester) async {
    final coll = todos('todos_watch');
    final events = <MutationEvent>[];
    final sub = coll.watch().listen(events.add);
    // Give the native subscription a beat to register.
    await Future<void>.delayed(const Duration(milliseconds: 100));
    coll.put(Todo(id: 1, title: 'observe me', done: false));
    await Future<void>.delayed(const Duration(milliseconds: 300));
    await sub.cancel();
    expect(events, isNotEmpty);
    expect(events.first.docId, 1);
  });

  testWidgets('collection on closed db throws StateError', (tester) async {
    final dir = await getApplicationDocumentsDirectory();
    final closedPath = '${dir.path}${Platform.pathSeparator}raft_coll_closed';
    final closedDb = await RaftDb.open(closedPath);
    await closedDb.close();

    expect(
      () => closedDb.collection<Todo>(
        name: 'x',
        id: (t) => t.id,
        encode: (t, w) => w.string('title', t.title),
        decode: (r) => Todo(id: r.id, title: r.string('title'), done: false),
      ),
      throwsStateError,
    );
  });
}
