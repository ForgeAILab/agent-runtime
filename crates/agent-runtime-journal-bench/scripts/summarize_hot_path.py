#!/usr/bin/env python3
"""Aggregate frozen hot_path summaries without correcting instrumentation costs."""
import argparse
import collections
import hashlib
import json
import math
import statistics
from pathlib import Path


def stats(values):
    ordered = sorted(values)
    return dict(samples=len(values), mean=statistics.fmean(ordered), median=statistics.median(ordered),
                p95=ordered[math.ceil(.95 * len(ordered)) - 1],
                minimum=ordered[0], maximum=ordered[-1])


def stage(op, label):
    return op['stages'].get(label, {}).get('nanos', 0) / 1e6


def measurements(case):
    operations = case['operations']
    turn = next(op for op in reversed(operations) if op['boundary'] == 'turn')
    result = {label: value['nanos'] / 1e6 for label, value in turn['stages'].items()}
    result['last_turn'] = turn['wall_nanos'] / 1e6
    for boundary in ('seed_bootstrap', 'initial_open', 'cold_root', 'warm_root',
                     'cold_resume', 'warm_resume', 'first_import', 'shutdown_drain',
                     'extension_probe', 'legacy_resume'):
        matching = [op['wall_nanos'] / 1e6 for op in operations if op['boundary'] == boundary]
        if matching:
            result[boundary] = sum(matching)
    for key, value in turn.get('attribution_nanos', {}).items():
        result['additive_' + key] = value / 1e6
    if turn.get('pack_write_bytes') is not None:
        result['last_turn_pack_write_bytes'] = turn['pack_write_bytes']
        result['collection_pack_write_bytes'] = turn['stages'].get('bench.collect.pack_write_bytes', {}).get('bytes', 0)
        result['complete_workload_pack_write_bytes'] = sum(op.get('pack_write_bytes') or 0 for op in operations if op['boundary'] in ('initial_open','turn','writer_reopen','writer_shutdown','persist','shutdown_drain'))
    result['legacy_last_turn'] = next(op['wall_nanos'] / 1e6 for op in reversed(operations)
                                      if op['boundary'] == 'legacy_turn')
    for label in ('rt.mod.collect', 'rt.gate.hold', 'rt.gate.wait'):
        result['max_' + label] = max(op['stages'].get(label, {}).get('max_nanos', 0)
                                      for op in operations) / 1e6
    result['writer_outside_engine'] = stage(turn, 'rt.writer.commit_domain') - stage(turn, 'rt.mod.commit')
    result['writer_direct_additive'] = sum(stage(turn, label) for label in
        ('rt.writer.preparation', 'rt.writer.commit_submission', 'rt.writer.candidate_publication'))
    result['writer_other'] = result['writer_outside_engine'] - result['writer_direct_additive']
    result['run_remainder_outside_engine'] = (result['last_turn'] - stage(turn, 'rt.mod.collect')
        - stage(turn, 'rt.storage.post_terminal') - stage(turn, 'rt.mod.commit'))
    result['other_run_outside_writer'] = result['run_remainder_outside_engine'] - result['writer_outside_engine']
    result['last_turn_with_reopen'] = result['last_turn'] + sum(op['wall_nanos']/1e6 for op in operations
        if op['boundary'] == 'writer_reopen' and op['turn'] == turn['turn'])
    result['complete_workload'] = sum(op['wall_nanos']/1e6 for op in operations if op['boundary'] in
        ('initial_open','turn','writer_reopen','writer_shutdown','persist','shutdown_drain'))
    imported = next(op for op in operations if op['boundary'] == 'first_import')
    for label in ('rt.writer.preparation','rt.writer.open_domain','rt.runtime.restore_context',
                  'rt.writer.snapshot_cached','rt.writer.manifest_cached','rt.mod.commit'):
        result['import_' + label] = stage(imported, label)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directories', nargs='+', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    rows = []
    receipts = {}
    for directory in args.directories:
        path = directory / 'summary.json'
        if path.exists():
            data = path.read_bytes()
            receipts[str(path)] = hashlib.sha256(data).hexdigest()
            rows.extend(json.loads(data))
    if not rows:
        parser.error('No completed summary.json cases found')
    groups = collections.defaultdict(list)
    fixture_bytes = collections.defaultdict(set)
    for row in rows:
        key = (row['fixture'], row['n'], row['tools'], row['context'], row['policy'],
               row['observer'], row['collection'], row['memo_budget'],row.get('profile_revision','p0-v3'))
        groups[key].append(measurements(row))
        fixture_bytes[key[:3]].add(row['fixture_fingerprint'])
    assert all(len(values) == 1 for values in fixture_bytes.values()), 'Fixture bytes changed'
    summaries = []
    for key, members in groups.items():
        labels = sorted(set().union(*(member.keys() for member in members)))
        summaries.append(dict(zip(('fixture','n','tools','context','policy','observer','collection','memo_budget','profile_revision'), key),
            metrics={label: stats([member.get(label, 0) for member in members]) for label in labels}))
    overhead = []
    for row in summaries:
        if not row['observer']:
            continue
        paired = next((other for other in summaries if not other['observer'] and
            all(row[field] == other[field] for field in ('fixture','n','tools','context','policy','collection','memo_budget','profile_revision'))), None)
        if paired:
            ratio = row['metrics']['last_turn']['median'] / paired['metrics']['last_turn']['median'] - 1
            overhead.append(dict(fixture=row['fixture'], n=row['n'], tools=row['tools'], context=row['context'],
                median_fraction=ratio, investigation_required=ratio > .02, calibration_subtracted=False))
    args.output.mkdir(parents=True, exist_ok=True)
    payload = dict(summary_sources_sha256=receipts, groups=summaries, observer_controls=overhead,
        units='milliseconds unless named bytes/count', inclusive_stages=True,
        calibration_subtracted=False, adapter='runtime reference; not Forge sqlx/AEAD or L2',
        fixture_fingerprints=[dict(fixture=k[0],n=k[1],tools=k[2],sha256=next(iter(v))) for k,v in fixture_bytes.items()])
    (args.output/'baseline.json').write_text(json.dumps(payload, indent=2)+'\n')
    with (args.output/'baseline.jsonl').open('w') as file:
        for row in summaries:
            file.write(json.dumps(row)+'\n')
    lines = ['# Frozen runtime baseline', '', 'Times are uncorrected milliseconds. Tables use medians except max_ rows, which show the absolute maximum across every sample. Nested timers are inclusive; only explicitly additive writer totals exclude engine commits.',
             'C last-turn-with-reopen and complete-workload totals include the separate writer boundaries. Maxima include all recorded operation scopes, including import and cleanup.', '']
    for fixture in ('A','B','C','C-plain-supplement'):
        selected = [row for row in summaries if row['fixture']==fixture and row['observer']
                    and row['context']=='matrix' and row['policy']=='conservative' and row['collection']]
        if not selected:
            continue
        version=max(row['profile_revision'] for row in selected)
        selected=[row for row in selected if row['profile_revision']==version]
        lines += [f'## {fixture} ({version})', '', '| Stage (ms) | 100 P | 100 T | 1000 P | 1000 T | 4000 P | 4000 T |',
                  '| --- | ---: | ---: | ---: | ---: | ---: | ---: |']
        columns = [next((r for r in selected if r['n']==n and r['tools']==tools), None)
                   for n in (100,1000,4000) for tools in (False,True)]
        labels = sorted(set().union(*(r['metrics'].keys() for r in selected)))
        for label in labels:
            cells = [f"{r['metrics'][label]['maximum' if label.startswith('max_') else 'median']:.3f}" if r and label in r['metrics'] else '—' for r in columns]
            lines.append('| '+label+' | '+' | '.join(cells)+' |')
        lines += ['', 'Every group’s sample count, p95, minimum and maximum are in baseline.json. A zero timer means no invocation in that boundary; work/memory reasons remain in the source summaries.', '']
    lines += ['## Observer controls', '', '| Fixture | N | Shape | Context | Enabled median overhead | >2% investigation |',
              '| --- | ---: | --- | --- | ---: | --- |']
    for control in overhead:
        lines.append(f"| {control['fixture']} | {control['n']} | {'tool' if control['tools'] else 'plain'} | {control['context']} | {100*control['median_fraction']:.2f}% | {control['investigation_required']} |")
    (args.output/'baseline.md').write_text('\n'.join(lines)+'\n')
    print(json.dumps(dict(cases=len(rows),groups=len(summaries),controls=len(overhead))))


if __name__ == '__main__':
    main()
