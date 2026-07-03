import 'dart:io';

import 'package:flutter/material.dart';
import 'package:path/path.dart' as p;
import 'package:path_provider/path_provider.dart';
import 'package:raft_bench/raft_bench_core.dart';
import 'package:share_plus/share_plus.dart';

import '../adapters/registry.dart';
import 'results_view.dart';

/// Dataset-size presets.
enum SizePreset {
  smoke('Smoke', 1000, 100, 1000),
  standard('Standard', 10000, 500, 10000),
  large('Large', 50000, 500, 20000);

  const SizePreset(this.label, this.records, this.durable, this.reads);
  final String label;
  final int records;
  final int durable;
  final int reads;

  BenchConfig get config => BenchConfig(
        recordCount: records,
        durableCount: durable,
        readCount: reads,
      );
}

class HomePage extends StatefulWidget {
  const HomePage({super.key});

  @override
  State<HomePage> createState() => _HomePageState();
}

class _HomePageState extends State<HomePage> {
  SizePreset _preset = SizePreset.standard;
  late final Map<String, bool> _selected = {
    for (final e in engineRegistry) e.key: e.enabledByDefault,
  };

  bool _running = false;
  String _phase = '';
  final List<String> _log = [];
  BenchReport? _report;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    return Scaffold(
      appBar: AppBar(
        title: const Text('Raft — cross-database benchmark'),
        actions: [
          if (_report != null && !_running)
            IconButton(
              tooltip: 'Export & share',
              onPressed: _export,
              icon: const Icon(Icons.ios_share),
            ),
        ],
      ),
      body: LayoutBuilder(
        builder: (context, constraints) => ListView(
          padding: const EdgeInsets.all(16),
          children: [
            _configCard(theme),
            const SizedBox(height: 12),
            _enginesCard(theme),
            const SizedBox(height: 16),
            FilledButton.icon(
              onPressed: _running ? null : _run,
              icon: _running
                  ? const SizedBox(
                      width: 18,
                      height: 18,
                      child: CircularProgressIndicator(strokeWidth: 2),
                    )
                  : const Icon(Icons.play_arrow),
              label: Text(_running ? 'Running…' : 'Run benchmark'),
            ),
            if (_running) ...[
              const SizedBox(height: 16),
              const LinearProgressIndicator(),
              const SizedBox(height: 8),
              Text(_phase, style: theme.textTheme.bodySmall),
              const SizedBox(height: 8),
              _logView(theme),
            ],
            if (_report != null) ...[
              const SizedBox(height: 20),
              ResultsView(report: _report!),
            ],
          ],
        ),
      ),
    );
  }

  Widget _configCard(ThemeData theme) => Card(
        child: Padding(
          padding: const EdgeInsets.all(16),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text('Dataset size', style: theme.textTheme.titleMedium),
              const SizedBox(height: 8),
              SegmentedButton<SizePreset>(
                segments: [
                  for (final s in SizePreset.values)
                    ButtonSegment(value: s, label: Text(s.label)),
                ],
                selected: {_preset},
                onSelectionChanged: _running
                    ? null
                    : (s) => setState(() => _preset = s.first),
              ),
              const SizedBox(height: 8),
              Text(
                '${_preset.records} records · durable group ${_preset.durable} · '
                '${_preset.reads} reads',
                style: theme.textTheme.bodySmall,
              ),
            ],
          ),
        ),
      );

  Widget _enginesCard(ThemeData theme) => Card(
        child: Padding(
          padding: const EdgeInsets.all(8),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Padding(
                padding: const EdgeInsets.fromLTRB(8, 8, 8, 0),
                child: Text('Engines', style: theme.textTheme.titleMedium),
              ),
              for (final e in engineRegistry)
                CheckboxListTile(
                  dense: true,
                  title: Text(e.key),
                  value: _selected[e.key],
                  onChanged: _running
                      ? null
                      : (v) => setState(() => _selected[e.key] = v ?? false),
                ),
            ],
          ),
        ),
      );

  Widget _logView(ThemeData theme) => Container(
        height: 140,
        width: double.infinity,
        padding: const EdgeInsets.all(10),
        decoration: BoxDecoration(
          color: theme.colorScheme.surfaceContainerHighest,
          borderRadius: BorderRadius.circular(8),
        ),
        child: ListView(
          reverse: true,
          children: [
            for (final line in _log.reversed.take(50))
              Text(line,
                  style: theme.textTheme.bodySmall
                      ?.copyWith(fontFamily: 'monospace', fontSize: 11)),
          ],
        ),
      );

  Future<void> _run() async {
    final chosen = engineRegistry.where((e) => _selected[e.key] == true).toList();
    if (chosen.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Select at least one engine.')),
      );
      return;
    }

    setState(() {
      _running = true;
      _report = null;
      _log.clear();
      _phase = 'Preparing…';
    });

    final tmp = await getTemporaryDirectory();
    final workspace = Directory(p.join(tmp.path, 'raft_bench_run'));
    if (workspace.existsSync()) {
      await workspace.delete(recursive: true);
    }
    await workspace.create(recursive: true);

    final harness = Harness(
      config: _preset.config,
      workspace: workspace.path,
      onProgress: (engine, phase, msg) {
        if (!mounted) return;
        setState(() {
          _phase = '$engine · $phase';
          _log.add('[$engine/$phase] $msg');
        });
      },
    );

    BenchReport? report;
    try {
      report = await harness.run([for (final e in chosen) e.build()]);
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context)
            .showSnackBar(SnackBar(content: Text('Run failed: $e')));
      }
    } finally {
      try {
        await workspace.delete(recursive: true);
      } catch (_) {}
    }

    if (!mounted) return;
    setState(() {
      _running = false;
      _report = report;
      _phase = 'Done';
    });
  }

  Future<void> _export() async {
    final report = _report;
    if (report == null) return;
    final dir = await getApplicationDocumentsDirectory();
    final stamp = report.timestamp.replaceAll(RegExp(r'[:.]'), '-');
    final base = p.join(dir.path, 'raft_bench_$stamp');
    final files = <XFile>[
      await _write('$base.json', report.toJsonString()),
      await _write('$base.csv', report.toCsv()),
      await _write('$base.md', report.toMarkdown()),
      await _write('$base.html', report.toHtml()),
    ];
    if (!mounted) return;
    await Share.shareXFiles(files, subject: 'Raft benchmark results');
  }

  Future<XFile> _write(String path, String content) async {
    await File(path).writeAsString(content);
    return XFile(path);
  }
}
