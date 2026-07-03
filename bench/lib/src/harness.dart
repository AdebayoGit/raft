import 'dart:io';

import 'package:path/path.dart' as p;

import 'adapter.dart';
import 'model.dart';
import 'results.dart';

/// Progress callback: (engine, workload-or-phase, message).
typedef ProgressSink = void Function(String engine, String phase, String message);

/// Drives every adapter through every workload over one shared [Dataset].
class Harness {
  Harness({
    required this.config,
    required this.workspace,
    this.onProgress,
  }) : dataset = Dataset(config);

  final BenchConfig config;
  final Dataset dataset;

  /// A directory the harness fully owns; per-run stores live under it and are
  /// deleted between samples.
  final String workspace;

  final ProgressSink? onProgress;

  int _storeSeq = 0;

  void _log(String engine, String phase, String msg) =>
      onProgress?.call(engine, phase, msg);

  /// Run all [adapters] and return a full report.
  Future<BenchReport> run(List<DbAdapter> adapters) async {
    final engineReports = <EngineReport>[];
    for (final adapter in adapters) {
      engineReports.add(await _runEngine(adapter));
    }
    return BenchReport(
      timestamp: DateTime.now().toUtc().toIso8601String(),
      platform: '${Platform.operatingSystem} '
          '${Platform.operatingSystemVersion} '
          '(${Platform.version.split(' ').first})',
      config: config,
      engines: engineReports,
    );
  }

  Future<EngineReport> _runEngine(DbAdapter adapter) async {
    _log(adapter.name, 'probe', 'checking availability');
    final avail = await _guardProbe(adapter);
    if (!avail.isAvailable) {
      _log(adapter.name, 'probe', 'unavailable: ${avail.reason}');
      return EngineReport(
        engine: adapter.name,
        version: adapter.version,
        durabilityNote: adapter.durabilityNote,
        results: [
          for (final w in Workload.values)
            WorkloadResult(
              workload: w,
              status: ResultStatus.skipped,
              note: avail.reason,
            ),
        ],
      );
    }

    final results = <WorkloadResult>[];

    // Write-family groups measured with a fresh store per sample.
    results.add(await _measureWrite(adapter, Workload.bulkWrite));
    results.add(await _measureWrite(adapter, Workload.bulkUpdate));
    results.add(await _measureWrite(adapter, Workload.bulkDelete));
    results.add(await _measureWrite(adapter, Workload.durableWrites));

    // Read-family groups share one populated store.
    results.addAll(await _measureReads(adapter));

    return EngineReport(
      engine: adapter.name,
      version: adapter.version,
      durabilityNote: adapter.durabilityNote,
      results: results,
    );
  }

  Future<Availability> _guardProbe(DbAdapter adapter) async {
    try {
      return await adapter.probe();
    } catch (e) {
      return Availability.unavailable('probe failed: $e');
    }
  }

  Future<WorkloadResult> _measureWrite(DbAdapter adapter, Workload w) async {
    if (!adapter.supported.contains(w)) {
      return WorkloadResult(
        workload: w,
        status: ResultStatus.unsupported,
        note: 'not exposed by this engine\'s API',
      );
    }
    final isDurable = w == Workload.durableWrites;
    final docs = isDurable ? dataset.durableSubset : dataset.docs;
    final ids = docs.map((d) => d.id).toList(growable: false);
    final samples = <int>[];
    try {
      for (var s = 0; s < config.writeSamples; s++) {
        final dir = await _freshStore(adapter);
        try {
          await adapter.openFresh(dir);
          // Precondition: update/delete need the data present (untimed).
          if (w == Workload.bulkUpdate || w == Workload.bulkDelete) {
            await adapter.bulkWrite(docs);
          }
          final sw = Stopwatch()..start();
          switch (w) {
            case Workload.bulkWrite:
              await adapter.bulkWrite(docs);
            case Workload.durableWrites:
              await adapter.durableWrites(docs);
            case Workload.bulkUpdate:
              await adapter.bulkUpdate(docs);
            case Workload.bulkDelete:
              await adapter.bulkDelete(ids);
            case Workload.pointRead:
            case Workload.iterateAll:
              break; // unreachable in write path
          }
          sw.stop();
          samples.add(sw.elapsedMicroseconds);
          _log(adapter.name, w.id,
              'sample ${s + 1}/${config.writeSamples}: ${sw.elapsedMicroseconds}µs');
        } finally {
          await _safeClose(adapter);
          await _deleteStore(dir);
        }
      }
      return WorkloadResult(
        workload: w,
        status: ResultStatus.ok,
        samplesMicros: samples,
        opCount: docs.length,
      );
    } catch (e) {
      _log(adapter.name, w.id, 'error: $e');
      return WorkloadResult(
        workload: w,
        status: ResultStatus.error,
        samplesMicros: samples,
        opCount: docs.length,
        note: '$e',
      );
    }
  }

  Future<List<WorkloadResult>> _measureReads(DbAdapter adapter) async {
    final wantPoint = adapter.supported.contains(Workload.pointRead);
    final wantIter = adapter.supported.contains(Workload.iterateAll);
    final out = <WorkloadResult>[];

    final dir = await _freshStore(adapter);
    try {
      await adapter.openFresh(dir);
      await adapter.bulkWrite(dataset.docs); // untimed populate

      if (wantPoint) {
        out.add(await _sampleRead(
          adapter,
          Workload.pointRead,
          config.readCount,
          () => adapter.pointReads(dataset.readOrder),
        ));
      } else {
        out.add(WorkloadResult(
            workload: Workload.pointRead,
            status: ResultStatus.unsupported,
            note: 'unsupported'));
      }

      if (wantIter) {
        out.add(await _sampleRead(
          adapter,
          Workload.iterateAll,
          config.recordCount,
          () => adapter.iterateAll(),
        ));
      } else {
        out.add(WorkloadResult(
            workload: Workload.iterateAll,
            status: ResultStatus.unsupported,
            note: 'unsupported'));
      }
    } catch (e) {
      _log(adapter.name, 'reads', 'error: $e');
      out
        ..clear()
        ..add(WorkloadResult(
            workload: Workload.pointRead,
            status: ResultStatus.error,
            note: '$e'))
        ..add(WorkloadResult(
            workload: Workload.iterateAll,
            status: ResultStatus.error,
            note: '$e'));
    } finally {
      await _safeClose(adapter);
      await _deleteStore(dir);
    }
    return out;
  }

  Future<WorkloadResult> _sampleRead(
    DbAdapter adapter,
    Workload w,
    int opCount,
    Future<int> Function() op,
  ) async {
    final samples = <int>[];
    // One untimed warmup read to prime caches, then timed samples.
    await op();
    for (var s = 0; s < config.readSamples; s++) {
      final sw = Stopwatch()..start();
      final found = await op();
      sw.stop();
      samples.add(sw.elapsedMicroseconds);
      _log(adapter.name, w.id,
          'sample ${s + 1}/${config.readSamples}: ${sw.elapsedMicroseconds}µs ($found found)');
    }
    return WorkloadResult(
      workload: w,
      status: ResultStatus.ok,
      samplesMicros: samples,
      opCount: opCount,
    );
  }

  Future<String> _freshStore(DbAdapter adapter) async {
    final safe = adapter.name.replaceAll(RegExp(r'[^A-Za-z0-9]'), '_');
    final dir = p.join(workspace, '${safe}_${_storeSeq++}');
    final d = Directory(dir);
    if (await d.exists()) await d.delete(recursive: true);
    await d.create(recursive: true);
    return dir;
  }

  Future<void> _deleteStore(String dir) async {
    try {
      final d = Directory(dir);
      if (await d.exists()) await d.delete(recursive: true);
    } catch (_) {/* best-effort cleanup */}
  }

  Future<void> _safeClose(DbAdapter adapter) async {
    try {
      await adapter.close();
    } catch (_) {/* already closed or failed open */}
  }
}
