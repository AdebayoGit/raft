import 'dart:convert';

import 'package:collection/collection.dart';

import 'model.dart';

/// Status of one engine × workload measurement.
enum ResultStatus { ok, skipped, unsupported, error }

/// The outcome of measuring one engine against one workload.
class WorkloadResult {
  WorkloadResult({
    required this.workload,
    required this.status,
    this.samplesMicros = const [],
    this.opCount = 0,
    this.note,
  });

  final Workload workload;
  final ResultStatus status;

  /// Per-sample wall-clock durations, microseconds.
  final List<int> samplesMicros;

  /// Logical operations performed in one sample (e.g. record count).
  final int opCount;

  /// Reason for skipped/unsupported/error, or a durability caveat.
  final String? note;

  int? get medianMicros {
    if (samplesMicros.isEmpty) return null;
    final sorted = [...samplesMicros]..sort();
    return sorted[sorted.length ~/ 2];
  }

  int? get bestMicros =>
      samplesMicros.isEmpty ? null : samplesMicros.reduce((a, b) => a < b ? a : b);

  /// Throughput in ops/second from the median sample.
  double? get opsPerSec {
    final m = medianMicros;
    if (m == null || m == 0 || opCount == 0) return null;
    return opCount / (m / 1e6);
  }

  Map<String, dynamic> toJson() => {
    'workload': workload.id,
    'status': status.name,
    'opCount': opCount,
    'samplesMicros': samplesMicros,
    'medianMicros': medianMicros,
    'bestMicros': bestMicros,
    'opsPerSec': opsPerSec,
    if (note != null) 'note': note,
  };
}

/// All workload results for a single engine.
class EngineReport {
  EngineReport({
    required this.engine,
    required this.version,
    required this.durabilityNote,
    required this.results,
  });

  final String engine;
  final String version;
  final String durabilityNote;
  final List<WorkloadResult> results;

  WorkloadResult? forWorkload(Workload w) =>
      results.firstWhereOrNull((r) => r.workload == w);

  Map<String, dynamic> toJson() => {
    'engine': engine,
    'version': version,
    'durabilityNote': durabilityNote,
    'results': results.map((r) => r.toJson()).toList(),
  };
}

/// A complete benchmark run across all engines.
class BenchReport {
  BenchReport({
    required this.timestamp,
    required this.platform,
    required this.config,
    required this.engines,
  });

  final String timestamp;
  final String platform;
  final BenchConfig config;
  final List<EngineReport> engines;

  Map<String, dynamic> toJson() => {
    'timestamp': timestamp,
    'platform': platform,
    'config': config.toJson(),
    'engines': engines.map((e) => e.toJson()).toList(),
  };

  /// Rebuild a report from a decoded JSON map (as written by [toJson]).
  factory BenchReport.fromJson(Map<String, dynamic> j) {
    final cfg = j['config'] as Map<String, dynamic>;
    return BenchReport(
      timestamp: j['timestamp'] as String,
      platform: j['platform'] as String,
      config: BenchConfig(
        recordCount: cfg['recordCount'] as int,
        durableCount: cfg['durableCount'] as int,
        readCount: cfg['readCount'] as int,
        payloadBytes: cfg['payloadBytes'] as int,
        writeSamples: cfg['writeSamples'] as int,
        readSamples: cfg['readSamples'] as int,
        seed: cfg['seed'] as int,
      ),
      engines: (j['engines'] as List).map((e) {
        final em = e as Map<String, dynamic>;
        return EngineReport(
          engine: em['engine'] as String,
          version: em['version'] as String,
          durabilityNote: em['durabilityNote'] as String,
          results: (em['results'] as List).map((r) {
            final rm = r as Map<String, dynamic>;
            return WorkloadResult(
              workload: Workload.values.firstWhere((w) => w.id == rm['workload']),
              status: ResultStatus.values.firstWhere((s) => s.name == rm['status']),
              samplesMicros:
                  (rm['samplesMicros'] as List? ?? const []).cast<int>(),
              opCount: rm['opCount'] as int? ?? 0,
              note: rm['note'] as String?,
            );
          }).toList(),
        );
      }).toList(),
    );
  }

  String toJsonString() => const JsonEncoder.withIndent('  ').convert(toJson());

  /// Comma-separated values, one row per engine × workload.
  String toCsv() {
    final b = StringBuffer(
      'engine,version,workload,status,op_count,median_micros,best_micros,ops_per_sec,note\n',
    );
    for (final e in engines) {
      for (final r in e.results) {
        b.writeln(
          [
            _csv(e.engine),
            _csv(e.version),
            r.workload.id,
            r.status.name,
            r.opCount,
            r.medianMicros ?? '',
            r.bestMicros ?? '',
            r.opsPerSec?.toStringAsFixed(1) ?? '',
            _csv(r.note ?? ''),
          ].join(','),
        );
      }
    }
    return b.toString();
  }

  static String _csv(String s) =>
      s.contains(',') || s.contains('"') ? '"${s.replaceAll('"', '""')}"' : s;

  /// A human-readable Markdown report with one throughput table per workload.
  String toMarkdown() {
    final b = StringBuffer();
    b.writeln('# Flutter cross-database benchmark results\n');
    b.writeln('- **When:** $timestamp');
    b.writeln('- **Platform:** $platform');
    b.writeln(
      '- **Dataset:** ${config.recordCount} records × ~${config.payloadBytes}B payload, '
      'seed ${config.seed}',
    );
    b.writeln(
      '- **Samples:** ${config.writeSamples} write / ${config.readSamples} read '
      '(median reported)\n',
    );

    b.writeln('## Engines & durability\n');
    b.writeln('| Engine | Version | Write durability (as benchmarked) |');
    b.writeln('|---|---|---|');
    for (final e in engines) {
      b.writeln('| ${e.engine} | ${e.version} | ${e.durabilityNote} |');
    }
    b.writeln();

    for (final w in Workload.values) {
      b.writeln('## ${w.id} — ${w.description}\n');
      b.writeln('| Engine | Median | Throughput | Notes |');
      b.writeln('|---|---|---|---|');
      // Rank available engines by throughput, fastest first.
      final rows = engines
          .map((e) => (e, e.forWorkload(w)))
          .toList()
        ..sort((a, b) {
          final pa = a.$2?.opsPerSec ?? -1;
          final pb = b.$2?.opsPerSec ?? -1;
          return pb.compareTo(pa);
        });
      for (final (engine, r) in rows) {
        if (r == null) {
          b.writeln('| ${engine.engine} | — | — | not measured |');
          continue;
        }
        final median = r.medianMicros == null
            ? '—'
            : _fmtDuration(r.medianMicros!);
        final tput = r.opsPerSec == null
            ? '—'
            : '${_fmtNum(r.opsPerSec!)} op/s';
        final note = switch (r.status) {
          ResultStatus.ok => r.note ?? '',
          ResultStatus.skipped => 'skipped: ${r.note ?? ''}',
          ResultStatus.unsupported => 'N/A: ${r.note ?? 'unsupported'}',
          ResultStatus.error => 'error: ${r.note ?? ''}',
        };
        b.writeln('| ${engine.engine} | $median | $tput | $note |');
      }
      b.writeln();
    }
    return b.toString();
  }

  static String _fmtDuration(int micros) {
    if (micros >= 1000000) return '${(micros / 1e6).toStringAsFixed(2)} s';
    if (micros >= 1000) return '${(micros / 1e3).toStringAsFixed(2)} ms';
    return '$micros µs';
  }

  static String _fmtNum(double n) {
    if (n >= 1e6) return '${(n / 1e6).toStringAsFixed(2)} M';
    if (n >= 1e3) return '${(n / 1e3).toStringAsFixed(1)} K';
    return n.toStringAsFixed(0);
  }

  static const _palette = <String, String>{
    'raft-db': '#ff5a3c',
  };
  static const _fallbackPalette = [
    '#3b82f6',
    '#10b981',
    '#a855f7',
    '#f59e0b',
    '#ec4899',
    '#14b8a6',
  ];

  String _colorFor(String engine, int index) =>
      _palette[engine] ?? _fallbackPalette[index % _fallbackPalette.length];

  /// A self-contained, theme-aware HTML report with a throughput bar chart per
  /// workload. No external assets — safe to open directly in a browser.
  String toHtml() {
    final colors = <String, String>{};
    for (var i = 0; i < engines.length; i++) {
      colors[engines[i].engine] = _colorFor(engines[i].engine, i);
    }

    final b = StringBuffer();
    b.writeln('<!doctype html><html lang="en"><head><meta charset="utf-8">');
    b.writeln('<meta name="viewport" content="width=device-width, initial-scale=1">');
    b.writeln('<title>Raft cross-database benchmark</title>');
    b.writeln('<style>${_css()}</style></head><body><div class="wrap">');

    b.writeln('<header><h1>Raft — cross-database benchmark</h1>');
    b.writeln('<p class="sub">Each engine driven through its real Dart API over an '
        'identical dataset. Higher throughput is better.</p>');
    b.writeln('<div class="meta">');
    b.writeln('<span><b>When</b> ${_esc(timestamp)}</span>');
    b.writeln('<span><b>Platform</b> ${_esc(platform)}</span>');
    b.writeln('<span><b>Dataset</b> ${config.recordCount} records · '
        '~${config.payloadBytes}B payload · seed ${config.seed}</span>');
    b.writeln('<span><b>Samples</b> ${config.writeSamples} write / '
        '${config.readSamples} read (median)</span>');
    b.writeln('</div></header>');

    // Legend + durability.
    b.writeln('<section class="card"><h2>Engines &amp; write durability</h2>');
    b.writeln('<table><thead><tr><th>Engine</th><th>Version</th>'
        '<th>Write durability (as benchmarked)</th></tr></thead><tbody>');
    for (final e in engines) {
      final c = colors[e.engine]!;
      b.writeln('<tr><td><span class="dot" style="background:$c"></span>'
          '<b>${_esc(e.engine)}</b></td><td class="mut">${_esc(e.version)}</td>'
          '<td class="mut">${_esc(e.durabilityNote)}</td></tr>');
    }
    b.writeln('</tbody></table></section>');

    // One chart card per workload.
    for (final w in Workload.values) {
      final rows = engines.map((e) => (e, e.forWorkload(w))).toList()
        ..sort((a, b) =>
            (b.$2?.opsPerSec ?? -1).compareTo(a.$2?.opsPerSec ?? -1));
      final maxTput = rows
          .map((r) => r.$2?.opsPerSec ?? 0)
          .fold<double>(0, (m, v) => v > m ? v : m);

      b.writeln('<section class="card"><h2>${_esc(w.id)}</h2>');
      b.writeln('<p class="sub">${_esc(w.description)}</p><div class="chart">');
      for (final (engine, r) in rows) {
        final c = colors[engine.engine]!;
        final tput = r?.opsPerSec;
        final pct = (tput != null && maxTput > 0) ? (tput / maxTput * 100) : 0;
        final label = tput != null
            ? '${_fmtNum(tput)} op/s'
            : switch (r?.status) {
                ResultStatus.skipped => 'skipped',
                ResultStatus.unsupported => 'N/A',
                ResultStatus.error => 'error',
                _ => '—',
              };
        final median = r?.medianMicros != null
            ? _fmtDuration(r!.medianMicros!)
            : '';
        final note = (r?.note != null && r!.status != ResultStatus.ok)
            ? ' · ${_esc(r.note!)}'
            : '';
        b.writeln('<div class="row">'
            '<div class="name">${_esc(engine.engine)}</div>'
            '<div class="track"><div class="bar" style="width:${pct.toStringAsFixed(1)}%;'
            'background:$c"></div></div>'
            '<div class="val">$label<span class="mut"> $median$note</span></div>'
            '</div>');
      }
      b.writeln('</div></section>');
    }

    b.writeln('<section class="card note"><h2>How to read this honestly</h2><ul>'
        '<li><b>raft-db is the only verified-durable engine.</b> Its FFI always '
        'fsyncs (<code>SyncMode::Always</code>, <code>F_FULLFSYNC</code>); the '
        'durable-writes gap reflects real flushes to stable storage, not engine '
        'slowness.</li>'
        '<li><b>SQLite point-reads are throttled by the sqflite bridge</b>, not '
        'the engine — each call is an async round-trip. The raw engine (see the '
        'Rust harness) reads far faster; this is the real cost a Flutter app pays.</li>'
        '<li><b>Hive is buffered</b> — it does not fsync per commit, so its write '
        'numbers are not comparable to a durable engine\'s.</li>'
        '<li>Bulk groups run in one transaction; durable-writes commit one record '
        'at a time. Read groups run against a store populated once, median of '
        'several samples.</li>'
        '</ul></section>');

    b.writeln('<footer>Generated from results/latest.json · '
        'raft-db cross-database Flutter benchmark</footer>');
    b.writeln('</div></body></html>');
    return b.toString();
  }

  static String _esc(String s) => s
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;');

  static String _css() => '''
:root{--bg:#f6f7f9;--card:#fff;--fg:#14171a;--mut:#6b7280;--line:#e5e7eb;--track:#eef0f3;}
@media (prefers-color-scheme:dark){:root{--bg:#0e1116;--card:#161b22;--fg:#e6edf3;--mut:#9aa4b2;--line:#26303b;--track:#20262e;}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;}
.wrap{max-width:920px;margin:0 auto;padding:32px 20px 60px;}
h1{font-size:26px;margin:0 0 6px;letter-spacing:-.02em;}
h2{font-size:16px;margin:0 0 4px;letter-spacing:-.01em;}
.sub{color:var(--mut);margin:0 0 16px;font-size:13.5px;}
.meta{display:flex;flex-wrap:wrap;gap:8px 20px;margin-top:14px;font-size:13px;color:var(--mut);}
.meta b{color:var(--fg);font-weight:600;margin-right:4px;}
.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:18px 20px;margin:16px 0;}
table{width:100%;border-collapse:collapse;font-size:13.5px;}
th{text-align:left;color:var(--mut);font-weight:600;padding:6px 8px;border-bottom:1px solid var(--line);}
td{padding:8px;border-bottom:1px solid var(--line);vertical-align:top;}
tr:last-child td{border-bottom:none;}
.mut{color:var(--mut);}
.dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:8px;vertical-align:middle;}
.chart{display:flex;flex-direction:column;gap:9px;margin-top:12px;}
.row{display:grid;grid-template-columns:130px 1fr 210px;align-items:center;gap:12px;}
.name{font-size:13px;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.track{background:var(--track);border-radius:7px;height:20px;overflow:hidden;}
.bar{height:100%;border-radius:7px;min-width:2px;transition:width .5s ease;}
.val{font-size:12.5px;font-variant-numeric:tabular-nums;text-align:right;}
.note ul{margin:8px 0 0;padding-left:20px;font-size:13.5px;color:var(--fg);}
.note li{margin:6px 0;}
code{background:var(--track);padding:1px 5px;border-radius:5px;font-size:12px;}
footer{color:var(--mut);font-size:12px;text-align:center;margin-top:24px;}
@media (max-width:640px){.row{grid-template-columns:92px 1fr;}.val{grid-column:2;text-align:left;}}
''';
}
