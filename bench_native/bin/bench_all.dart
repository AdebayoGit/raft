import 'dart:io';

import 'package:args/args.dart';
import 'package:path/path.dart' as p;
import 'package:raft_bench/raft_bench.dart';
import 'package:raft_bench_native/adapters/isar_adapter.dart';
import 'package:raft_bench_native/adapters/objectbox_adapter.dart';
import 'package:raft_bench_native/adapters/unavailable_adapter.dart';

/// Headless all-engine runner: Raft + SQLite + Hive + Isar + ObjectBox, plus a
/// placeholder Realm entry (its native lib needs a network download blocked in
/// this sandbox — benchmark Realm via the Flutter app on a device).
///
/// Writes the canonical report into ../bench/results so it supersedes the
/// three-engine run, and renders HTML.
Future<void> main(List<String> argv) async {
  final parser = ArgParser()
    ..addFlag('smoke', negatable: false)
    ..addOption('records')
    ..addOption('durable')
    ..addOption('reads');
  final args = parser.parse(argv);

  final config = args['smoke'] as bool
      ? BenchConfig.smoke
      : BenchConfig(
          recordCount: int.tryParse(args['records'] as String? ?? '') ?? 10000,
          durableCount: int.tryParse(args['durable'] as String? ?? '') ?? 500,
          readCount: int.tryParse(args['reads'] as String? ?? '') ?? 10000,
        );

  final repoRoot = _repoRoot();
  final dylib = _dylib(repoRoot);

  final adapters = <DbAdapter>[
    RaftAdapter.fromPath(dylib),
    SqliteAdapter(),
    HiveAdapter(),
    IsarAdapter(),
    ObjectBoxAdapter(),
    UnavailableAdapter(
      'Realm',
      'native lib requires a network download blocked in this sandbox — '
          'benchmark via the Flutter app (flutter test integration_test -d <device>)',
      durabilityNote: 'C++ MVCC core; write() commits are durable.',
    ),
  ];

  final workspace = await Directory.systemTemp.createTemp('raft_bench_all_');
  stdout.writeln('== raft all-engine benchmark ==');
  stdout.writeln('records=${config.recordCount} durable=${config.durableCount} '
      'reads=${config.readCount}');
  stdout.writeln('raft dylib: $dylib\n');

  final harness = Harness(
    config: config,
    workspace: workspace.path,
    onProgress: (e, ph, m) => stdout.writeln('  [$e/$ph] $m'),
  );

  BenchReport report;
  try {
    report = await harness.run(adapters);
  } finally {
    try {
      await workspace.delete(recursive: true);
    } catch (_) {}
  }

  final outDir = Directory(p.join(repoRoot, 'bench', 'results'));
  await outDir.create(recursive: true);
  final stamp = report.timestamp.replaceAll(RegExp(r'[:.]'), '-');
  await File(p.join(outDir.path, 'latest.json')).writeAsString(report.toJsonString());
  await File(p.join(outDir.path, 'latest.csv')).writeAsString(report.toCsv());
  await File(p.join(outDir.path, 'latest.md')).writeAsString(report.toMarkdown());
  await File(p.join(outDir.path, 'latest.html')).writeAsString(report.toHtml());
  await File(p.join(outDir.path, 'all6-$stamp.json')).writeAsString(report.toJsonString());

  stdout.writeln('\n${report.toMarkdown()}');
  stdout.writeln('Artefacts -> ${outDir.path}/latest.{json,csv,md,html}');
}

String _repoRoot() {
  var dir = Directory.current;
  for (var i = 0; i < 6; i++) {
    if (File(p.join(dir.path, 'CLAUDE.md')).existsSync() &&
        Directory(p.join(dir.path, 'core')).existsSync()) {
      return dir.path;
    }
    final parent = dir.parent;
    if (parent.path == dir.path) break;
    dir = parent;
  }
  return Directory.current.path;
}

String _dylib(String root) {
  final candidates = [
    p.join(root, 'core', 'target', 'x86_64-apple-darwin', 'release', 'libraftdb.dylib'),
    p.join(root, 'core', 'target', 'aarch64-apple-darwin', 'release', 'libraftdb.dylib'),
    p.join(root, 'core', 'target', 'release', 'libraftdb.dylib'),
  ];
  for (final c in candidates) {
    if (File(c).existsSync()) return c;
  }
  return candidates.first;
}
