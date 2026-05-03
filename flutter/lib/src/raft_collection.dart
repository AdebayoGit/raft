import 'dart:convert';
import 'dart:typed_data';

import 'raft_db.dart';

/// A typed, collection-scoped wrapper around [RaftDb].
///
/// All keys are automatically prefixed with `<name>:` so multiple
/// collections coexist in one database without colliding. The caller
/// supplies [serialize] / [deserialize] functions, so the collection
/// stays storage-agnostic — encode with `jsonEncode`, msgpack, protobuf,
/// or your own format, whatever fits.
///
/// ```dart
/// class User {
///   User({required this.id, required this.name});
///   final String id;
///   final String name;
///
///   Map<String, dynamic> toJson() => {'id': id, 'name': name};
///   factory User.fromJson(Map<String, dynamic> j) =>
///       User(id: j['id'] as String, name: j['name'] as String);
/// }
///
/// final db = await RaftDb.open(path);
/// final users = RaftCollection<User>(
///   db: db,
///   name: 'users',
///   serialize: (u) => Uint8List.fromList(utf8.encode(jsonEncode(u.toJson()))),
///   deserialize: (b) => User.fromJson(jsonDecode(utf8.decode(b))),
/// );
///
/// await users.put('1', User(id: '1', name: 'Alice'));
/// final alice = await users.get('1'); // User?
/// await users.delete('1');
/// ```
class RaftCollection<T> {
  /// Creates a collection wrapper. [name] is used as the key prefix.
  RaftCollection({
    required RaftDb db,
    required this.name,
    required Uint8List Function(T document) serialize,
    required T Function(Uint8List bytes) deserialize,
  })  : _db = db,
        _serialize = serialize,
        _deserialize = deserialize,
        _prefix = Uint8List.fromList(utf8.encode('$name:'));

  /// The underlying database handle.
  final RaftDb _db;

  /// The collection name used as a key prefix.
  final String name;

  final Uint8List Function(T document) _serialize;
  final T Function(Uint8List bytes) _deserialize;
  final Uint8List _prefix;

  /// Insert or update a document by [id].
  ///
  /// Throws [RaftDbException] on native failure.
  Future<void> put(String id, T document) {
    return _db.put(_scopedKey(id), _serialize(document));
  }

  /// Retrieve a document by [id]. Returns `null` if no document with
  /// that id exists.
  ///
  /// Throws [RaftDbException] on native failure (other than not-found).
  Future<T?> get(String id) async {
    final bytes = await _db.get(_scopedKey(id));
    if (bytes == null) return null;
    return _deserialize(bytes);
  }

  /// Delete a document by [id]. Deleting a non-existent id is a no-op.
  ///
  /// Throws [RaftDbException] on native failure.
  Future<void> delete(String id) {
    return _db.delete(_scopedKey(id));
  }

  /// The full prefix bytes (`<name>:`) — exposed for callers that want
  /// to bridge the collection to a future prefix-scoped observer.
  Uint8List get prefix => Uint8List.fromList(_prefix);

  Uint8List _scopedKey(String id) =>
      Uint8List.fromList(utf8.encode('$name:$id'));
}
