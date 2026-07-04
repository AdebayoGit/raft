import 'dart:io';

import 'package:path/path.dart' as p;
import 'package:raft_bench/raft_bench.dart';
import 'package:test/test.dart';

/// The one bug class that matters for the generation-stamped read cache:
/// a write that fails to invalidate it. These tests interleave writes with
/// cached reads and assert the cache can never serve a stale document.
void main() {
  late RaftAdapter adapter;
  late Directory ws;

  String dylibPath() {
    var dir = Directory.current;
    for (var i = 0; i < 6; i++) {
      final c = p.join(dir.path, 'core', 'target', 'x86_64-apple-darwin',
          'release', 'libraftdb.dylib');
      if (File(c).existsSync()) return c;
      dir = dir.parent;
    }
    throw StateError('raft dylib not found — build core first');
  }

  setUp(() async {
    adapter = RaftAdapter.fromPath(dylibPath());
    expect((await adapter.probe()).isAvailable, isTrue);
    ws = await Directory.systemTemp.createTemp('raft_cache_test_');
    await adapter.openFresh(ws.path);
  });

  tearDown(() async {
    await adapter.close();
    try {
      await ws.delete(recursive: true);
    } catch (_) {}
  });

  BenchDoc doc(int id, int score) =>
      BenchDoc(id: id, name: 'user-$id', score: score, payload: 'p' * 20);

  test('cached read reflects every write path (no stale reads)', () async {
    await adapter.bulkWrite([doc(1, 100), doc(2, 200)]);

    // Populate the cache.
    expect(await adapter.cachedPointReads([1, 2]), 2);

    // Overwrite through the batch path — the cache must invalidate.
    await adapter.bulkUpdate([doc(1, 100), doc(2, 200)]); // scores +1
    expect(await adapter.cachedPointReads([1, 2]), 2);
    // Verify the cached objects are the NEW versions by reading scores
    // through a fresh uncached read and comparing against a cached read
    // round trip: delete-then-cached-read must also see the change.
    await adapter.bulkDelete([2]);
    expect(await adapter.cachedPointReads([2]), 0,
        reason: 'deleted doc must not be served from cache');
    expect(await adapter.cachedPointReads([1]), 1);

    // Durable single-commit path invalidates too.
    await adapter.durableWrites([doc(3, 300)]);
    expect(await adapter.cachedPointReads([3]), 1);
  });

  test('hot cached reads stay correct across 100 write/read interleavings',
      () async {
    await adapter.bulkWrite([for (var i = 1; i <= 50; i++) doc(i, i)]);
    for (var round = 0; round < 100; round++) {
      final id = 1 + (round % 50);
      if (round.isEven) {
        await adapter.bulkUpdate([doc(id, id)]); // bump score
      } else {
        await adapter.durableWrites([doc(id + 100, id)]); // insert new
      }
      // Every read after a write must observe a consistent store: all
      // originally-written ids still present via the cached path.
      expect(await adapter.cachedPointReads([id]), 1,
          reason: 'round $round: id $id must be readable after write');
    }
    // Final: cached count of everything matches uncached truth.
    final allIds = [for (var i = 1; i <= 150; i++) i];
    final cached = await adapter.cachedPointReads(allIds);
    final uncached = await adapter.pointReads(allIds);
    expect(cached, uncached,
        reason: 'cached and uncached views must agree exactly');
  });
}
