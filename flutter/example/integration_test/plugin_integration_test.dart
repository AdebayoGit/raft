import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:path_provider/path_provider.dart';
import 'package:raft_db_flutter/raft_db_flutter.dart';

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
    final closedPath =
        '${dir.path}${Platform.pathSeparator}raft_closed_test';
    final closedDb = await RaftDb.open(closedPath);
    await closedDb.close();

    expect(() => closedDb.get(key), throwsStateError);
    expect(() => closedDb.put(key, value), throwsStateError);
    expect(() => closedDb.delete(key), throwsStateError);
    expect(() => closedDb.close(), throwsStateError);
  });

  // ── RaftCollection ──────────────────────────────────────────────────

  RaftCollection<Map<String, dynamic>> jsonCollection(String name) {
    return db.collection<Map<String, dynamic>>(
      name: name,
      serialize: (doc) =>
          Uint8List.fromList(utf8.encode(jsonEncode(doc))),
      deserialize: (bytes) =>
          jsonDecode(utf8.decode(bytes)) as Map<String, dynamic>,
    );
  }

  testWidgets('collection put and get round-trip', (tester) async {
    final users = jsonCollection('users_rt');
    await users.put('1', {'name': 'Alice', 'age': 30});
    final loaded = await users.get('1');
    expect(loaded, isNotNull);
    expect(loaded!['name'], 'Alice');
    expect(loaded['age'], 30);
  });

  testWidgets('collection get returns null for missing id', (tester) async {
    final users = jsonCollection('users_missing');
    final loaded = await users.get('does-not-exist');
    expect(loaded, isNull);
  });

  testWidgets('collection delete removes document', (tester) async {
    final users = jsonCollection('users_del');
    await users.put('42', {'name': 'Bob'});
    await users.delete('42');
    expect(await users.get('42'), isNull);
  });

  testWidgets('collections with different names do not collide',
      (tester) async {
    final users = jsonCollection('coll_a');
    final orders = jsonCollection('coll_b');
    await users.put('1', {'name': 'Alice'});
    await orders.put('1', {'item': 'Book'});

    final u = await users.get('1');
    final o = await orders.get('1');
    expect(u!['name'], 'Alice');
    expect(o!['item'], 'Book');
  });

  testWidgets('collection on closed db throws StateError', (tester) async {
    final dir = await getApplicationDocumentsDirectory();
    final closedPath =
        '${dir.path}${Platform.pathSeparator}raft_coll_closed';
    final closedDb = await RaftDb.open(closedPath);
    await closedDb.close();

    expect(
      () => closedDb.collection<Map<String, dynamic>>(
        name: 'x',
        serialize: (m) => Uint8List(0),
        deserialize: (b) => <String, dynamic>{},
      ),
      throwsStateError,
    );
  });
}
