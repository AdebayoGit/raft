import 'package:flutter/material.dart';
import 'package:raft_bench/raft_bench_core.dart';

/// Renders a [BenchReport] as a durability table plus one throughput bar chart
/// per workload (fastest engine first).
class ResultsView extends StatelessWidget {
  const ResultsView({super.key, required this.report});

  final BenchReport report;

  static const _raftColor = Color(0xFFFF5A3C);
  static const _palette = [
    Color(0xFF3B82F6),
    Color(0xFF10B981),
    Color(0xFFA855F7),
    Color(0xFFF59E0B),
    Color(0xFFEC4899),
    Color(0xFF14B8A6),
  ];

  Color _colorFor(String engine, int index) =>
      engine == 'raft-db' ? _raftColor : _palette[index % _palette.length];

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final colors = <String, Color>{};
    for (var i = 0; i < report.engines.length; i++) {
      colors[report.engines[i].engine] = _colorFor(report.engines[i].engine, i);
    }

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text('Results', style: theme.textTheme.headlineSmall),
        const SizedBox(height: 4),
        Text(report.platform, style: theme.textTheme.bodySmall),
        const SizedBox(height: 12),
        _durabilityCard(theme, colors),
        const SizedBox(height: 12),
        for (final w in Workload.values) _workloadCard(theme, colors, w),
      ],
    );
  }

  Widget _durabilityCard(ThemeData theme, Map<String, Color> colors) => Card(
        child: Padding(
          padding: const EdgeInsets.all(16),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text('Engines & write durability',
                  style: theme.textTheme.titleMedium),
              const SizedBox(height: 8),
              for (final e in report.engines)
                Padding(
                  padding: const EdgeInsets.symmetric(vertical: 4),
                  child: Row(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Container(
                        width: 10,
                        height: 10,
                        margin: const EdgeInsets.only(top: 4, right: 8),
                        decoration: BoxDecoration(
                          color: colors[e.engine],
                          shape: BoxShape.circle,
                        ),
                      ),
                      Expanded(
                        child: Column(
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            Text(e.engine,
                                style: theme.textTheme.bodyMedium?.copyWith(
                                    fontWeight: FontWeight.w600)),
                            Text(e.durabilityNote,
                                style: theme.textTheme.bodySmall),
                          ],
                        ),
                      ),
                    ],
                  ),
                ),
            ],
          ),
        ),
      );

  Widget _workloadCard(
      ThemeData theme, Map<String, Color> colors, Workload w) {
    final rows = report.engines.map((e) => (e, e.forWorkload(w))).toList()
      ..sort((a, b) =>
          (b.$2?.opsPerSec ?? -1).compareTo(a.$2?.opsPerSec ?? -1));
    final maxTput = rows
        .map((r) => r.$2?.opsPerSec ?? 0)
        .fold<double>(0, (m, v) => v > m ? v : m);

    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(w.id, style: theme.textTheme.titleMedium),
            Text(w.description, style: theme.textTheme.bodySmall),
            const SizedBox(height: 12),
            for (final (engine, r) in rows)
              _bar(theme, colors[engine.engine]!, engine.engine, r, maxTput),
          ],
        ),
      ),
    );
  }

  Widget _bar(ThemeData theme, Color color, String engine, WorkloadResult? r,
      double maxTput) {
    final tput = r?.opsPerSec;
    final frac = (tput != null && maxTput > 0) ? (tput / maxTput) : 0.0;
    final label = tput != null
        ? '${_fmtNum(tput)} op/s'
        : switch (r?.status) {
            ResultStatus.skipped => 'skipped',
            ResultStatus.unsupported => 'N/A',
            ResultStatus.error => 'error',
            _ => '—',
          };
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 5),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              Text(engine,
                  style: theme.textTheme.bodySmall
                      ?.copyWith(fontWeight: FontWeight.w600)),
              Text(label, style: theme.textTheme.bodySmall),
            ],
          ),
          const SizedBox(height: 3),
          ClipRRect(
            borderRadius: BorderRadius.circular(6),
            child: LinearProgressIndicator(
              value: frac == 0 ? null : frac,
              minHeight: 10,
              backgroundColor: theme.colorScheme.surfaceContainerHighest,
              color: color,
            ),
          ),
        ],
      ),
    );
  }

  static String _fmtNum(double n) {
    if (n >= 1e6) return '${(n / 1e6).toStringAsFixed(2)} M';
    if (n >= 1e3) return '${(n / 1e3).toStringAsFixed(1)} K';
    return n.toStringAsFixed(0);
  }
}
