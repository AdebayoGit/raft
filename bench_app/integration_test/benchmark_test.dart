import 'dart:io';

import 'package:flutter/foundation.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:path/path.dart' as p;
import 'package:path_provider/path_provider.dart';
import 'package:raft_bench/raft_bench_core.dart';
import 'package:raft_bench_app/adapters/registry.dart';

/// Headless benchmark of every engine, runnable on a real device or macOS:
///
/// ```
/// flutter test integration_test/benchmark_test.dart -d macos
/// flutter test integration_test/benchmark_test.dart -d <deviceId>
/// ```
///
/// Writes `bench_result.json` into the app documents dir and echoes it between
/// BENCH_RESULT_JSON markers so a host script can capture the numbers.
void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets('benchmark all engines', (tester) async {
    // Standard config; override with --dart-define for smaller device runs.
    const records = int.fromEnvironment('RECORDS', defaultValue: 10000);
    const durable = int.fromEnvironment('DURABLE', defaultValue: 500);
    const reads = int.fromEnvironment('READS', defaultValue: 10000);

    final tmp = await getTemporaryDirectory();
    final ws = Directory(p.join(tmp.path, 'bench_it'));
    if (ws.existsSync()) ws.deleteSync(recursive: true);
    ws.createSync(recursive: true);

    final harness = Harness(
      config: const BenchConfig(
        recordCount: records,
        durableCount: durable,
        readCount: reads,
      ),
      workspace: ws.path,
      onProgress: (e, ph, m) => debugPrint('[$e/$ph] $m'),
    );

    final report = await harness.run([for (final e in engineRegistry) e.build()]);

    final outDir = await getApplicationDocumentsDirectory();
    final jsonPath = p.join(outDir.path, 'bench_result.json');
    File(jsonPath).writeAsStringSync(report.toJsonString());

    debugPrint('BENCH_RESULT_PATH=$jsonPath');
    debugPrint('BENCH_RESULT_JSON_BEGIN');
    for (final line in report.toJsonString().split('\n')) {
      debugPrint(line);
    }
    debugPrint('BENCH_RESULT_JSON_END');

    try {
      ws.deleteSync(recursive: true);
    } catch (_) {}

    expect(report.engines, isNotEmpty);
  }, timeout: const Timeout(Duration(minutes: 15)));
}
