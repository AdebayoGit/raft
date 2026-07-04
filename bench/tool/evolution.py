#!/usr/bin/env python3
"""Render bench/results/evolution.html — raft-db's optimization snapshots
(baseline → batch FFI → binary core) against current competitor numbers.

Usage: python3 tool/evolution.py [baseline.json phase1.json current.json]
Defaults to the canonical 2026-07-03 snapshots + results/latest.json.
"""
import json
import sys
from pathlib import Path

RESULTS = Path(__file__).resolve().parent.parent / 'results'

DEFAULTS = [
    RESULTS / 'all6-2026-07-03T17-51-32-726815Z.json',   # pre-optimization baseline
    RESULTS / 'all6-2026-07-03T21-54-58-369865Z.json',   # + batch FFI
    RESULTS / 'latest.json',                             # current
]

WORKLOADS = [
    ('bulk_write', 'Insert 10k records in one transaction'),
    ('durable_writes', 'Insert 500 records, one durable commit each'),
    ('concurrent_durable', 'Durable commits from 4 concurrent writers'),
    ('point_read', 'Read 10k records by primary key'),
    ('point_read_cached', 'Cached by-key reads (generation-invalidated)'),
    ('read_many', 'Fetch 10k records by id in one batch call'),
    ('iterate_all', 'Read every record (full scan)'),
    ('bulk_update', 'Update every record in one transaction'),
    ('bulk_delete', 'Delete every record in one transaction'),
]
COMPETITORS = [('SQLite (sqflite_ffi)', '#3b82f6'), ('Hive', '#10b981'), ('Isar', '#a855f7')]
RAFT_LABELS = ['raft-db · baseline', 'raft-db · +batch FFI', 'raft-db · current']
RAFT_COLORS = ['#ffb4a6', '#ff7e63', '#ff5a3c']

CSS = """
:root{--bg:#f6f7f9;--card:#fff;--fg:#14171a;--mut:#6b7280;--line:#e5e7eb;--track:#eef0f3;}
@media (prefers-color-scheme:dark){:root{--bg:#0e1116;--card:#161b22;--fg:#e6edf3;--mut:#9aa4b2;--line:#26303b;--track:#20262e;}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;}
.wrap{max-width:940px;margin:0 auto;padding:32px 20px 60px;}
h1{font-size:26px;margin:0 0 6px;letter-spacing:-.02em;}h2{font-size:16px;margin:0 0 2px;}
.sub{color:var(--mut);margin:0 0 8px;font-size:13.5px;}
.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:18px 20px;margin:16px 0;}
.chart{display:flex;flex-direction:column;gap:8px;margin-top:12px;}
.row{display:grid;grid-template-columns:190px 1fr 130px;align-items:center;gap:12px;}
.name{font-size:12.5px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.track{background:var(--track);border-radius:6px;height:17px;overflow:hidden;}
.bar{height:100%;border-radius:6px;min-width:2px;}
.val{font-size:12.5px;font-variant-numeric:tabular-nums;text-align:right;}
.delta{color:var(--mut);font-size:11.5px;margin-left:4px;}
.raft .name{color:#ff5a3c;}
.legend{display:flex;flex-wrap:wrap;gap:8px 18px;font-size:12.5px;color:var(--mut);margin-top:10px;}
.dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:6px;vertical-align:middle;}
.badge{display:inline-block;font-size:11px;font-weight:700;padding:1px 8px;border-radius:10px;background:#ff5a3c1e;color:#ff5a3c;margin-left:8px;vertical-align:middle;}
footer{color:var(--mut);font-size:12px;text-align:center;margin-top:24px;}
"""


def ops(report, engine, workload):
    for e in report['engines']:
        if e['engine'] == engine:
            for r in e['results']:
                if r['workload'] == workload and r.get('opsPerSec'):
                    return r['opsPerSec']
    return None


def fmt(n):
    if n is None:
        return '—'
    if n >= 1e6:
        return f'{n / 1e6:.2f} M'
    if n >= 1e3:
        return f'{n / 1e3:.1f} K'
    return f'{n:.0f}'


def main():
    paths = [Path(a) for a in sys.argv[1:4]] or DEFAULTS
    reports = [json.loads(p.read_text()) for p in paths]
    baseline, _, current = reports

    # Plain-language headline: one number per everyday operation.
    GLANCE = [('Reads /sec', 'point_read'), ('Writes /sec', 'bulk_write'),
              ('Updates /sec', 'bulk_update'), ('Deletes /sec', 'bulk_delete')]
    glance_rows = []
    engines = [('raft-db', '#ff5a3c')] + COMPETITORS
    for name, color in engines:
        cells = ''.join(f'<td>{fmt(ops(current, name, wl))}</td>' for _, wl in GLANCE)
        glance_rows.append(
            f'<tr><td><span class="dot" style="background:{color}"></span>'
            f'<b>{name}</b></td>{cells}</tr>')
    glance = (
        '<section class="card"><h2>At a glance (current)</h2>'
        '<p class="sub">One number per everyday operation — higher is better.</p>'
        '<table style="width:100%;border-collapse:collapse;font-size:13.5px">'
        '<thead><tr><th style="text-align:left;padding:6px 8px">Engine</th>'
        + ''.join(f'<th style="text-align:left;padding:6px 8px">{h}</th>' for h, _ in GLANCE)
        + '</tr></thead><tbody>'
        + ''.join(glance_rows)
        + '</tbody></table></section>')

    sections = [glance]
    for wl, desc in WORKLOADS:
        entries = [
            (RAFT_LABELS[i], RAFT_COLORS[i], ops(rep, 'raft-db', wl), True)
            for i, rep in enumerate(reports)
        ]
        entries += [(n, c, ops(current, n, wl), False) for n, c in COMPETITORS]
        mx = max((v for _, _, v, _ in entries if v), default=1)
        base_v, final_v = entries[0][2], entries[2][2]
        delta = f'{final_v / base_v:.1f}×' if base_v and final_v else ''
        durable_best = max(
            (v for v in (final_v, ops(current, 'SQLite (sqflite_ffi)', wl),
                         ops(current, 'Isar', wl)) if v),
            default=0,
        )
        badge = (' <span class="badge">#1 durable</span>'
                 if final_v and final_v >= durable_best else '')
        bars = []
        for label, color, v, is_raft in entries:
            pct = (v / mx * 100) if v else 0
            cls = 'row raft' if is_raft else 'row'
            d = (f'<span class="delta">({delta} vs baseline)</span>'
                 if is_raft and label.endswith('current') and delta else '')
            bars.append(
                f'<div class="{cls}"><div class="name">{label}</div>'
                f'<div class="track"><div class="bar" style="width:{pct:.1f}%;'
                f'background:{color}"></div></div>'
                f'<div class="val">{fmt(v)}{d}</div></div>')
        sections.append(
            f'<section class="card"><h2>{wl}{badge}</h2>'
            f'<p class="sub">{desc}</p>'
            f'<div class="chart">{"".join(bars)}</div></section>')

    html = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>raft-db optimization evolution</title><style>{CSS}
td{{padding:6px 8px;border-bottom:1px solid var(--line);}}</style></head><body><div class="wrap">
<header><h1>raft-db — optimization evolution</h1>
<p class="sub">Same machine, same honest workloads. Three raft snapshots
(baseline → batch FFI + binary boundary codec → binary internal persistence,
zero-clone scan, group commit under concurrency) against current competitor
numbers. Higher is better; op/s.</p>
<div class="legend">
<span><span class="dot" style="background:#ffb4a6"></span>raft baseline</span>
<span><span class="dot" style="background:#ff7e63"></span>raft + batch FFI</span>
<span><span class="dot" style="background:#ff5a3c"></span>raft current</span>
<span><span class="dot" style="background:#3b82f6"></span>SQLite (sqflite_ffi)</span>
<span><span class="dot" style="background:#10b981"></span>Hive (buffered — not crash-durable)</span>
<span><span class="dot" style="background:#a855f7"></span>Isar</span>
</div></header>
{''.join(sections)}
<section class="card"><h2>How to read this honestly</h2><p class="sub" style="margin-top:8px">
raft-db and Isar are the only engines here verified to flush every durable commit (~19 ms
F_FULLFSYNC on this SSD bounds any durable engine to ~52 sequential commits/s — hardware,
not software). Hive is a RAM map with lazy persistence: unbeatable at reads, not
crash-durable at writes. In concurrent_durable, raft's group commit merges overlapping
fsyncs while Isar's global writer lock serialises them. Every engine materialises full
records in read workloads (Isar via findAll, not count).</p></section>
<footer>Generated by bench/tool/evolution.py from results snapshots</footer>
</div></body></html>"""

    out = RESULTS / 'evolution.html'
    out.write_text(html)
    print(f'wrote {out} ({len(html)} bytes)')


if __name__ == '__main__':
    main()
