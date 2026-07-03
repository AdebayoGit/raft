import 'dart:convert';
import 'dart:io';

import 'package:path/path.dart' as p;
import 'package:raft_bench/raft_bench.dart';

/// Render an existing benchmark JSON into a self-contained HTML report without
/// re-running the benchmarks.
///
/// ```
/// dart run bin/report.dart                 # results/latest.json -> results/latest.html
/// dart run bin/report.dart path/to/run.json
/// ```
Future<void> main(List<String> argv) async {
  final input = argv.isNotEmpty ? argv.first : 'results/latest.json';
  final file = File(input);
  if (!file.existsSync()) {
    stderr.writeln('No results file at $input. Run `dart run bin/bench.dart` first.');
    exit(1);
  }
  final report =
      BenchReport.fromJson(jsonDecode(await file.readAsString()) as Map<String, dynamic>);
  final outPath = p.setExtension(input, '.html');
  await File(outPath).writeAsString(report.toHtml());
  stdout.writeln('Wrote $outPath');
}
