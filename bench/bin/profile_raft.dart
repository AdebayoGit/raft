import 'dart:convert';
import 'dart:io';

import 'package:path/path.dart' as p;
import 'package:raft_bench/raft_bench.dart';

/// Phase-0 micro-profile: where does raft bulk_write / point_read time go?
/// Splits Dart-side JSON encoding from the FFI+engine cost so Phase 1/2 work
/// is aimed at the real bottleneck.
Future<void> main() async {
  final root = _repoRoot();
  final dylib = p.join(root, 'core', 'target', 'x86_64-apple-darwin',
      'release', 'libraftdb.dylib');
  final dataset = Dataset(const BenchConfig(recordCount: 10000));
  final docs = dataset.docs;

  // 1. Pure Dart JSON encode cost (the adapter's _docJson path).
  final sw = Stopwatch()..start();
  var bytes = 0;
  for (final d in docs) {
    final obj = {
      'id': d.id,
      'fields': {
        'name': {'String': d.name},
        'score': {'Int': d.score},
        'payload': {'String': d.payload},
      },
    };
    bytes += utf8.encode(jsonEncode(obj)).length;
  }
  sw.stop();
  stdout.writeln('json_encode 10k docs: ${sw.elapsedMicroseconds}µs '
      '($bytes bytes total)');

  // 1b. Binary batch encode alone (the Dart share of bulkWrite).
  final swB = Stopwatch()..start();
  final batch = RaftCodec.encodeBatch(docs);
  swB.stop();
  stdout.writeln('binary_encode 10k:    ${swB.elapsedMicroseconds}µs '
      '(${batch.length} bytes)');

  // 2. Full bulkWrite via the adapter (one put_many FFI call).
  final adapter = RaftAdapter.fromPath(dylib);
  final probe = await adapter.probe();
  if (!probe.isAvailable) {
    stderr.writeln('raft unavailable: ${probe.reason}');
    exit(1);
  }
  final ws = await Directory.systemTemp.createTemp('raft_prof_');
  try {
    await adapter.openFresh(ws.path);
    final sw2 = Stopwatch()..start();
    await adapter.bulkWrite(docs);
    sw2.stop();
    stdout.writeln('bulkWrite total:      ${sw2.elapsedMicroseconds}µs');

    // 3. Point reads: two-phase get cost.
    final sw3 = Stopwatch()..start();
    final found = await adapter.pointReads(dataset.readOrder);
    sw3.stop();
    stdout.writeln('pointReads 10k:       ${sw3.elapsedMicroseconds}µs '
        '($found found)');

    // 4. iterate_all (list_ids + N gets) for the scan-API comparison.
    final sw4 = Stopwatch()..start();
    final n = await adapter.iterateAll();
    sw4.stop();
    stdout.writeln('iterateAll:           ${sw4.elapsedMicroseconds}µs ($n docs)');
  } finally {
    await adapter.close();
    try {
      await ws.delete(recursive: true);
    } catch (_) {}
  }

  stdout.writeln('\ndylib size: ${File(dylib).lengthSync()} bytes');
}

String _repoRoot() {
  var dir = Directory.current;
  for (var i = 0; i < 6; i++) {
    if (File(p.join(dir.path, 'CLAUDE.md')).existsSync()) return dir.path;
    dir = dir.parent;
  }
  return Directory.current.path;
}
