import 'dart:io';

import 'package:args/args.dart';
import 'package:path/path.dart' as p;
import 'package:raft_bench/raft_bench.dart';

/// Headless cross-database benchmark runner.
///
/// ```
/// dart run bin/bench.dart --smoke
/// dart run bin/bench.dart --records 10000 --out results
/// ```
Future<void> main(List<String> argv) async {
  final parser = ArgParser()
    ..addFlag('smoke',
        negatable: false, help: 'Quick small run (1k records).')
    ..addOption('records', help: 'Record count (bulk groups).')
    ..addOption('durable', help: 'Record count (durable-writes group).')
    ..addOption('reads', help: 'Point-read count.')
    ..addOption('payload', help: 'Payload bytes per record.')
    ..addOption('out', defaultsTo: 'results', help: 'Output directory.')
    ..addOption('raft-dylib', help: 'Path to libraftdb.dylib/.so/.dll.')
    ..addMultiOption('engines',
        help: 'Subset of engines to run (raft, sqlite, hive). Default: all.')
    ..addFlag('help', abbr: 'h', negatable: false);

  final args = parser.parse(argv);
  if (args['help'] as bool) {
    stdout.writeln('raft cross-database benchmark\n');
    stdout.writeln(parser.usage);
    return;
  }

  final config = args['smoke'] as bool
      ? BenchConfig.smoke
      : BenchConfig(
          recordCount: _int(args['records']) ?? 10000,
          durableCount: _int(args['durable']) ?? 500,
          readCount: _int(args['reads']) ?? 10000,
          payloadBytes: _int(args['payload']) ?? 100,
        );

  final repoRoot = _findRepoRoot();
  final dylib = (args['raft-dylib'] as String?) ?? _defaultDylib(repoRoot);

  final selected = (args['engines'] as List<String>).map((e) => e.toLowerCase());
  final all = <String, DbAdapter>{
    'raft': RaftAdapter.fromPath(dylib),
    'sqlite': SqliteAdapter(),
    'hive': HiveAdapter(),
  };
  final adapters = selected.isEmpty
      ? all.values.toList()
      : [
          for (final key in selected)
            if (all.containsKey(key)) all[key]!,
        ];
  if (adapters.isEmpty) {
    stderr.writeln('No known engines selected. Choose from: ${all.keys.join(", ")}');
    exit(2);
  }

  final workspace = await Directory.systemTemp.createTemp('raft_bench_');
  stdout.writeln('== raft cross-database benchmark ==');
  stdout.writeln('records=${config.recordCount} durable=${config.durableCount} '
      'reads=${config.readCount} payload=${config.payloadBytes}B');
  stdout.writeln('raft dylib: $dylib');
  stdout.writeln('workspace: ${workspace.path}\n');

  final harness = Harness(
    config: config,
    workspace: workspace.path,
    onProgress: (engine, phase, msg) =>
        stdout.writeln('  [$engine/$phase] $msg'),
  );

  BenchReport report;
  try {
    report = await harness.run(adapters);
  } finally {
    try {
      await workspace.delete(recursive: true);
    } catch (_) {}
  }

  // Write artefacts.
  final outDir = Directory(p.isAbsolute(args['out'] as String)
      ? args['out'] as String
      : p.join(repoRoot, 'bench', args['out'] as String));
  await outDir.create(recursive: true);
  final stamp = report.timestamp.replaceAll(RegExp(r'[:.]'), '-');
  await File(p.join(outDir.path, 'latest.json')).writeAsString(report.toJsonString());
  await File(p.join(outDir.path, 'latest.csv')).writeAsString(report.toCsv());
  await File(p.join(outDir.path, 'latest.md')).writeAsString(report.toMarkdown());
  await File(p.join(outDir.path, 'latest.html')).writeAsString(report.toHtml());
  await File(p.join(outDir.path, 'run-$stamp.json')).writeAsString(report.toJsonString());

  stdout.writeln('\n${report.toMarkdown()}');
  stdout.writeln('Artefacts written to ${outDir.path}/ (latest.json, latest.csv, latest.md)');
}

int? _int(Object? v) => v == null ? null : int.tryParse(v as String);

String _findRepoRoot() {
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

String _defaultDylib(String repoRoot) {
  final candidates = [
    p.join(repoRoot, 'core', 'target', 'x86_64-apple-darwin', 'release', 'libraftdb.dylib'),
    p.join(repoRoot, 'core', 'target', 'aarch64-apple-darwin', 'release', 'libraftdb.dylib'),
    p.join(repoRoot, 'core', 'target', 'release', 'libraftdb.dylib'),
    p.join(repoRoot, 'core', 'target', 'release', 'libraftdb.so'),
  ];
  for (final c in candidates) {
    if (File(c).existsSync()) return c;
  }
  return candidates.first;
}
