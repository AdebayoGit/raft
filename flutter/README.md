# raft_db — Flutter

> Mobile-native embedded database for Flutter, with a Dart API over the Raft Rust core.

[![pub](https://img.shields.io/pub/v/raft_db.svg)](https://pub.dev/packages/raft_db)

Offline-first key-value and document storage with reactive queries, optimistic transactions, and LSM-tree durability — all without a SQL parser, all on the device.

## Install

```yaml
dependencies:
  raft_db: ^0.1.0
```

```bash
flutter pub get
```

The plugin ships native binaries for Android (.so), iOS (.xcframework), macOS, Linux, and Windows. No extra Gradle / CocoaPods config required.

## Quickstart

```dart
import 'dart:convert';
import 'package:path_provider/path_provider.dart';
import 'package:raft_db/raft_db.dart';

Future<void> main() async {
  final dir = await getApplicationDocumentsDirectory();
  final db = await RaftDb.open('${dir.path}/raft');

  // Key-value writes
  await db.put(
    utf8.encode('user:1'),
    utf8.encode(jsonEncode({'name': 'Alice', 'age': 30})),
  );

  // Reads
  final raw = await db.get(utf8.encode('user:1'));
  if (raw != null) {
    final user = jsonDecode(utf8.decode(raw));
    print('Loaded: $user');
  }

  // Deletes
  await db.delete(utf8.encode('user:1'));

  // Always close when you are done
  await db.close();
}
```

All native calls are dispatched through `Isolate.run` — the calling isolate is never blocked by I/O.

## API

### `RaftDb.open(path)`

Open or create a database at `path`. The directory is created on first use.

```dart
final db = await RaftDb.open('/data/user/0/com.example/files/myapp.raft');
```

### `db.put(key, value)`

Insert or update. Both `key` and `value` are `Uint8List`. Use `utf8.encode` for strings or `jsonEncode + utf8.encode` for structured data.

### `db.get(key)`

Returns the value, or `null` if the key does not exist. Uses a two-phase read (size query → exact-size buffer) so there are no fixed-size truncation surprises.

### `db.delete(key)`

Deleting a non-existent key is a no-op (a tombstone is written).

### `db.close()`

Flushes any pending writes and releases the native handle. After `close()`, further calls throw `StateError`.

## Errors

All native errors are thrown as `RaftDbException` with a message and a numeric code. The codes match the C `RftError` enum:

| Code | Meaning                |
| ---- | ---------------------- |
| 1    | Null-pointer argument  |
| 2    | Invalid UTF-8          |
| 3    | I/O / storage error    |
| 4    | Key not found          |
| 5    | Buffer too small       |
| 6    | Invalid JSON           |
| 7    | Transaction conflict   |
| 8    | Invalid handle         |
| 9    | Unknown subscription   |

## Roadmap

The current Dart layer wraps the v0.1.0 KV surface. The richer document / query / transaction / observer FFI is exposed in the Rust core (see `core/include/raft.h`) and the Dart bindings are next on the queue.

## Example app

See [`example/`](example/) for a runnable Flutter app that exercises the full API.

## License

Apache-2.0 OR MIT.
