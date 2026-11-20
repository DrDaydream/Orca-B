#!/usr/bin/env python3
"""Serial A1 acceptance on an already installed EC2 testbed.

The matrix and burst rounding match figure-tools/run_matrix.py (v3.5), but this
campaign has its own directory and common-clock metrics. Planning and reporting
use only the standard library; execution invokes the repository's fab remote.
No create, install, start, stop, destroy or figure-tool modification is performed.
"""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import shutil
import signal
from statistics import fmean
import subprocess
import sys
import time
import uuid


PROTOCOLS = ('Orca-A', 'Orca-B')
NODES = (10, 20, 35)
RATES = (10000, 30000, 50000, 70000, 90000)
LOCK = Path('/tmp/five-protocol-figure-matrix.lock')
LATENCY_LIMIT_MS = 3000
NODE_PARAMETERS = dict(header_size=20, max_header_delay=200, gc_depth=50,
                       sync_retry_delay=10000, sync_retry_nodes=3,
                       batch_size=500000, max_batch_delay=200)


def now():
    return datetime.now(timezone.utc).isoformat()


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True,
                                    allow_nan=False) + '\n', encoding='utf-8')
    temporary.replace(path)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def cases(*, duration=60, warmup=10, drain=10, header_size=20):
    for name, value, minimum in (('duration', duration, 1), ('warmup', warmup, 0),
                                  ('drain', drain, 0), ('header_size', header_size, 1)):
        if type(value) is not int or value < minimum:
            raise ValueError(f'{name} must be an integer >= {minimum}')
    result = []
    for protocol in PROTOCOLS:
        for nodes in NODES:
            faults = (nodes - 1) // 3
            for nominal in RATES:
                quantum = 20 * nodes
                rate = ((nominal + quantum // 2) // quantum) * quantum
                result.append(dict(protocol=protocol, mode='A1', fault_model='crash-stop',
                                   nodes=nodes, nominal_rate_tps=nominal, input_rate_tps=rate,
                                   offered_rate_tps=rate // nodes * (nodes - faults),
                                   byzantine=faults, byzantine_ids=list(range(nodes - faults, nodes)),
                                   active_launch_ids=list(range(nodes - faults)),
                                   duration_s=duration, warmup_s=warmup, drain_s=drain,
                                   header_size_bytes=header_size, seed=7, workers=1,
                                   tx_size=512, repetition=1))
    return result


def case_key(case):
    return f"{case['protocol']}/n{case['nodes']}/r{case['input_rate_tps']}"


def command(case):
    options = dict(nodes=case['nodes'], rate=case['input_rate_tps'],
                   workers=1, tx_size=512, duration=case['duration_s'],
                   warmup=case['warmup_s'], drain=case['drain_s'],
                   header_size=case['header_size_bytes'], common_clock='true', runs=1, faults=0,
                   adversary='A1', byzantine=case['byzantine'], seed=7,
                   start_round=1, attack_rounds=0, delay_ms=200,
                   max_pending_messages=4096, max_pending_bytes=67108864,
                   bullshark_alternating='false')
    return ['fab', 'remote', *[part for key, value in options.items()
                             for part in ('--' + key.replace('_', '-'), str(value))]]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def finite(value, label, *, positive=False):
    require(type(value) in (int, float) and math.isfinite(value)
            and (value > 0 if positive else value >= 0), f'Invalid {label}: {value}')
    return value


def count(value, label, *, positive=False):
    require(type(value) is int and value >= (1 if positive else 0), f'Invalid {label}: {value}')
    return value


def close(actual, expected, label):
    finite(actual, label)
    require(math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-9),
            f'{label} differs from replica data: {actual} != {expected}')


def validate_plan(report, case):
    plan = report.get('plan', {})
    require(report.get('schema') == plan.get('schema') == 2, 'Common-clock schema 2 required')
    run_id = plan.get('run_id')
    require(isinstance(run_id, str) and bool(run_id) and report.get('run_id') == run_id,
            'Report and plan run_id must match')
    for key in ('load_us', 'start_us', 'end_us', 'drain_end_us'):
        count(plan.get(key), key, positive=True)
    require(plan['start_us'] - plan['load_us'] == case['warmup_s'] * 1000000,
            'Plan warmup differs from requested warmup')
    require(plan['end_us'] - plan['start_us'] == case['duration_s'] * 1000000,
            'Plan measurement duration differs from requested duration')
    require(plan['drain_end_us'] - plan['end_us'] == case['drain_s'] * 1000000,
            'Plan drain differs from requested drain')
    return plan


def replica_rows(window, expected, label):
    rows = window.get('replicas')
    require(isinstance(rows, list), f'{label} replica data missing')
    ids = [row.get('replica') for row in rows]
    require(all(type(i) is int for i in ids) and sorted(ids) == expected,
            f'{label} must include every active replica exactly once; got {ids}, expected {expected}')
    return sorted(rows, key=lambda row: row['replica'])


def evaluate(report, case):
    """Reject invalid populations/evidence; return a latency decision plus censoring data."""
    plan = validate_plan(report, case)
    active = case['active_launch_ids']
    require(active, 'No active replicas')
    commit = report.get('commit_window', {})
    release = report.get('release_window', {})
    require(commit.get('status') == 'available' and commit.get('coverage_complete') is True
            and commit.get('throughput_coverage_complete') is True,
            'Commit-window instrumentation or evidence coverage is incomplete')
    for key, expected in [('window_start_us', plan['start_us']), ('window_end_us', plan['end_us']),
                          ('window_seconds', case['duration_s'])]:
        require(commit.get(key) == expected, f'Commit-window {key} differs from plan')
    require(commit.get('expected_replicas') == commit.get('replicas_with_samples') == len(active),
            'Commit-window expected replica count differs or a replica has zero samples')
    require(release.get('status') == 'available'
            and release.get('source_pairs') == release.get('instrumented_source_pairs') == len(active),
            'Release-window instrumentation is incomplete')
    for key, expected in [('window_start_us', plan['start_us']), ('window_end_us', plan['end_us']),
                          ('commit_cutoff_us', plan['drain_end_us'])]:
        require(release.get(key) == expected, f'Release-window {key} differs from plan')
    commit_rows = replica_rows(commit, active, 'Commit-window')
    release_rows = replica_rows(release, active, 'Release-window')
    released = count(release.get('released_samples'), 'released samples', positive=True)
    details = []
    for c, r in zip(commit_rows, release_rows):
        ident = c['replica']
        require(c.get('coverage_complete') is True and c.get('throughput_coverage_complete') is True,
                f'Incomplete commit evidence at replica {ident}')
        matched = count(c.get('matched_samples'), f'replica {ident} matched samples', positive=True)
        require(c.get('selected_samples') == matched
                and c.get('missing_release_samples') == c.get('missing_ingress_samples') == c.get('invalid_time_samples') == 0
                and c.get('missing_batch_sizes') == [],
                f'Commit-window evidence diagnostics are incomplete at replica {ident}')
        batches = count(c.get('final_batches'), f'replica {ident} final batches', positive=True)
        final_bytes = count(c.get('final_bytes'), f'replica {ident} final bytes', positive=True)
        tps = finite(c.get('final_tps'), f'replica {ident} TPS', positive=True)
        close(tps, final_bytes / case['tx_size'] / case['duration_s'], 'Replica TPS')
        distribution = c.get('release_to_final', {})
        require(distribution.get('count') == matched, f'Unpaired E2E samples at replica {ident}')
        latency = finite(distribution.get('mean_ms'), f'replica {ident} commit-window E2E')
        require(r.get('released_samples') == released, f'Release cohort differs at replica {ident}')
        completed = count(r.get('completed_samples'), f'replica {ident} completed samples', positive=True)
        unfinished = count(r.get('unfinished_samples'), f'replica {ident} unfinished samples')
        require(completed + unfinished == released, f'Release counts do not balance at replica {ident}')
        require(r.get('paired_samples') == completed
                and r.get('release_to_final', {}).get('count') == completed,
                f'Incomplete release-window E2E pairs at replica {ident}')
        require(r.get('missing_ingress_samples') == r.get('invalid_time_samples') == 0,
                f'Invalid time ordering or missing ingress at replica {ident}')
        not_received = count(r.get('not_received_samples'), f'replica {ident} not-received samples')
        require(not_received <= released, f'Invalid not-received count at replica {ident}')
        ratio = finite(r.get('completion_ratio'), f'replica {ident} completion ratio')
        close(ratio, completed / released, 'Completion ratio')
        release_latency = finite(r['release_to_final'].get('mean_ms'), f'replica {ident} release-window E2E')
        details.append(dict(replica=ident, commit_e2e_ms=latency, final_tps=tps,
                            final_batches=batches, final_bytes=final_bytes,
                            commit_matched_samples=matched, release_e2e_ms=release_latency,
                            released_samples=released, completed_samples=completed,
                            unfinished_samples=unfinished, not_received_samples=not_received,
                            completion_ratio=ratio))
    latency = fmean(r['commit_e2e_ms'] for r in details)
    tps = fmean(r['final_tps'] for r in details)
    release_latency = fmean(r['release_e2e_ms'] for r in details)
    close(commit.get('mean_replica_release_to_final_ms'), latency, 'Native aggregate E2E')
    close(commit.get('mean_replica_final_tps'), tps, 'Native aggregate TPS')
    require(commit.get('matched_samples') == sum(r['commit_matched_samples'] for r in details),
            'Native aggregate matched samples differs from replicas')
    passed = latency < LATENCY_LIMIT_MS
    return dict(passed=passed, run_id=plan['run_id'], replicas=len(active),
                metric='sample release -> final ordered commit',
                aggregation='unweighted mean across ALL active replica means',
                primary_population='sampled transactions finally committed in [start_us,end_us)',
                limit_ms=LATENCY_LIMIT_MS, commit_e2e_ms_mean=latency,
                commit_e2e_ms_max=max(r['commit_e2e_ms'] for r in details),
                commit_matched_samples_sum=sum(r['commit_matched_samples'] for r in details),
                commit_final_tps_mean=tps, offered_rate_tps=case['offered_rate_tps'],
                final_tps_over_offered=tps / case['offered_rate_tps'],
                release_e2e_ms_mean=release_latency,
                release_completed_samples_sum=sum(r['completed_samples'] for r in details),
                release_unfinished_samples_sum=sum(r['unfinished_samples'] for r in details),
                release_not_received_samples_sum=sum(r['not_received_samples'] for r in details),
                release_samples_per_replica=released,
                release_completion_ratio_min=min(r['completion_ratio'] for r in details),
                release_completion_ratio_mean=fmean(r['completion_ratio'] for r in details),
                window_disagreement=passed != (release_latency < LATENCY_LIMIT_MS),
                interpretation='Latency is conditional on observed completions. Unfinished samples are '
                               'censored at drain_end_us; no minimum completion-ratio or TPS target is imposed.',
                replica_details=details)


def manifest(directory):
    return {p.relative_to(directory).as_posix(): sha(p) for p in sorted(directory.rglob('*'))
            if p.is_file() and p != directory / 'SHA256.json'}


def seal(directory):
    save(directory / 'SHA256.json', manifest(directory))


def verify_seal(directory):
    require(read(directory / 'SHA256.json') == manifest(directory),
            f'Archive file set or hashes changed: {directory}')


def git(bench, *args):
    return subprocess.check_output(['git', *args], cwd=bench, text=True).strip()


def identity(bench):
    """Bind resumption to settings, native build and effective Python source bytes."""
    root = bench.parent
    commit = git(bench, 'rev-parse', 'HEAD')
    dirty = subprocess.run(['git', 'diff', '--quiet', 'HEAD', '--', '*.rs', 'Cargo.toml',
                            '**/Cargo.toml', 'Cargo.lock', '.cargo', 'rust-toolchain',
                            'rust-toolchain.toml'], cwd=root, check=False).returncode
    require(dirty == 0, 'Native sources/build configuration differ from HEAD; commit and install first')
    marker = root / 'target/release/.orca-build-commit'
    require(marker.is_file() and marker.read_text().strip() == commit,
            'Installed controller build marker differs from HEAD; complete fab install first')
    source = [bench / 'fabfile.py', *sorted((bench / 'benchmark').glob('*.py'))]
    binaries = {}
    for name in ('node', 'benchmark_client'):
        path = root / 'target/release' / name
        require(path.is_file() and os.access(path, os.X_OK), f'Missing installed executable: {path}')
        binaries[name] = sha(path)
    settings = read(bench / 'settings.json')
    require(settings.get('repo', {}).get('name') in PROTOCOLS, 'settings.json must name an Orca repository')
    return dict(controller_commit=commit, build_marker=marker.read_text().strip(),
                binaries_sha256=binaries, settings_sha256=sha(bench / 'settings.json'),
                source_sha256={p.relative_to(bench).as_posix(): sha(p) for p in source},
                runner_sha256=sha(Path(__file__)),
                environment={k: v for k, v in sorted(os.environ.items())
                             if k.startswith(('ORCA_', 'BENCH_'))
                             or k in ('FIGURE_SCOPE', 'TOKIO_WORKER_THREADS', 'RUSTUP_TOOLCHAIN')})


def open_campaign(output, plan, variant):
    path = output / 'campaign.json'
    if path.is_file():
        campaign = read(path)
        require(campaign.get('schema') == 1 and campaign.get('cases') == plan
                and campaign.get('variant') == variant,
                'Existing campaign parameters/variant differ; choose a new --output directory')
        return campaign
    require(not output.exists() or not any(output.iterdir()),
            'A new campaign requires an empty --output directory; existing archives are never adopted')
    campaign = dict(schema=1, created_utc=now(), variant=variant, cases=plan, identities={},
                    metric='sample release -> final ordered commit; fixed common commit window',
                    criterion='all 30 valid cases, each unweighted active-replica E2E mean < 3000 ms',
                    completion_ratio_threshold=None, tps_threshold=None,
                    toolkit_reference='figure-tools/run_matrix.py v3.5 rates/rounding, A1 crash population')
    save(path, campaign)
    return campaign


def validate_snapshot(snapshot, case, plan):
    expected_protocol = case['protocol'].lower()
    for key, value in [('protocol', expected_protocol), ('nodes', case['nodes']),
                        ('rate', case['input_rate_tps']), ('run', 1), ('fault_model', 'crash-stop'),
                        ('active_launch_ids', case['active_launch_ids']),
                        ('inactive_launch_ids', case['byzantine_ids']),
                        ('offered_rate_tps', case['offered_rate_tps']), ('measurement_window', plan)]:
        require(snapshot.get(key) == value, f'Native snapshot {key} differs from requested case/clock')
    params = snapshot.get('benchmark_parameters', {})
    for key, value in [('nodes', [case['nodes']]), ('rate', [case['input_rate_tps']]),
                        ('duration', case['duration_s']), ('workers', 1), ('tx_size', 512),
                        ('faults', 0), ('runs', 1), ('collocate', True),
                        ('common_clock', True)]:
        require(params.get(key) == value, f'Native benchmark parameter {key} differs from requested case')
    for key in ('warmup', 'drain'):
        require(params.get(key, 0) == case[key + '_s'], f'Native {key} differs from requested case')
    node = {**NODE_PARAMETERS, 'header_size': case['header_size_bytes']}
    require(snapshot.get('node_parameters') == node, 'Native node parameters differ from the explicit preset')
    adversary = snapshot.get('adversary', {})
    for key, value in [('mode', 'silent'), ('fault_model', 'crash-stop'), ('seed', 7),
                        ('start_round', 1), ('rounds', 0), ('byzantine_ids', case['byzantine_ids']),
                        ('bullshark_alternating', False)]:
        require(adversary.get(key) == value, f'Native A1 parameter {key} differs')
    authorities = snapshot.get('launch_authorities', [])
    require(len(authorities) == case['nodes'] and len(set(authorities)) == case['nodes'],
            'Native committee authority identities are incomplete')
    if case['protocol'] == 'Orca-B':
        require(snapshot.get('aba_scheduling') == 'A5-only', 'Orca-B A1 must retain A5-only ABA scheduling')


def log_files(directory, case):
    groups = []
    active = case['active_launch_ids']
    for kind in ('client', 'primary', 'worker'):
        expected = {f'{kind}-{i}.log' if kind == 'primary' else f'{kind}-{i}-0.log' for i in active}
        paths = sorted(directory.glob(f'{kind}-*.log'))
        require({p.name for p in paths} == expected, f'{kind} logs must cover every active replica exactly once')
        groups.append(paths)
    return groups


def validate_client_configuration(archive, case, snapshot, committee):
    expected_rate = str(case['input_rate_tps'] // case['nodes'])
    expected_size = str(snapshot['benchmark_parameters']['tx_size'])
    for node in case['active_launch_ids']:
        authority = snapshot['launch_authorities'][node]
        worker = committee[authority].get('workers', {}).get('0', {})
        address = worker.get('transactions')
        require(isinstance(address, str) and address, f'Missing committee worker target for launch ID {node}')
        path = archive / 'logs' / f'client-{node}-0.log'
        declarations = path.read_text(encoding='utf-8').splitlines()
        for label, expected, description in (
                ('Transactions rate: ', expected_rate + ' tx/s', 'configured rate'),
                ('Transactions size: ', expected_size + ' B', 'configured size'),
                ('Node address: ', address, 'worker target')):
            values = [line.split(label, 1)[1] for line in declarations if label in line]
            require(values == [expected],
                    f'{path.name} {description}: expected one {expected!r}, found {values!r}')


def reparse(archive, case, plan):
    """Use the archived native parser, with no Fabric/AWS import or mutable code dependency."""
    paths = log_files(archive / 'logs', case)
    texts = [[p.read_text(encoding='utf-8', errors='strict') for p in group] for group in paths]
    require(not any(re.search(r'panicked at|thread .* panicked', text)
                    for group in texts for text in group), 'A process panicked; inspect raw logs')
    module_spec = importlib.util.spec_from_file_location('archived_a1_measurements', archive / 'measurement_parser.py')
    module = importlib.util.module_from_spec(module_spec)
    # Avoid writing __pycache__ into an immutable archive during reporting.
    previous = sys.dont_write_bytecode
    try:
        sys.dont_write_bytecode = True
        module_spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = previous
    measure = module.Measurements(*texts)
    measure.replica_logs = [p.name for p in paths[1]]
    measure.source_logs = [dict(client=c.name, worker=w.name) for c, w in zip(paths[0], paths[2])]
    report = measure.clock_metrics(plan)
    ids = [int(p.stem.split('-')[1]) for p in paths[1]]
    for window in ('release_window', 'commit_window'):
        rows = report[window]['replicas']
        require(len(rows) == len(ids), 'Native parser omitted replicas')
        for row, ident in zip(rows, ids):
            row['replica'] = ident
    return report


def sole(directory, pattern):
    matches = list(directory.glob(pattern))
    require(len(matches) == 1, f'Expected exactly one {pattern} in {directory}; got {len(matches)}')
    return matches[0]


def audit_archive(archive, case, expected_identity):
    verify_seal(archive)
    meta = read(archive / 'metadata.json')
    require(meta.get('status') == 'complete' and meta.get('returncode') == 0,
            f'Attempt did not complete successfully: {archive}')
    require(meta.get('case') == case and meta.get('identity') == expected_identity,
            f'Attempt identity/configuration differs: {archive}')
    require(sha(archive / 'measurement_parser.py') == expected_identity['source_sha256']['benchmark/measurements.py'],
            'Archived measurement parser differs from campaign identity')
    require(sha(archive / 'settings.json') == expected_identity['settings_sha256'],
            'Archived settings differ from campaign identity')
    require(sha(archive / 'acceptance_runner.py') == expected_identity['runner_sha256'],
            'Archived acceptance runner differs from campaign identity')
    native = read(sole(archive / 'native-results', '*.comparison.json'))
    plan = read(archive / 'logs/measurement-window.json')
    require(native.get('plan') == plan and meta.get('run_id') == plan.get('run_id'),
            'Native report, raw logs and attempt run_id differ')
    validate_plan(native, case)
    require(meta['started_us'] <= plan['load_us'] < plan['drain_end_us'] <= meta['finished_us'],
            'Native clock window lies outside this attempt; refusing stale evidence')
    snapshot = read(sole(archive / 'native-results', '*.parameters.json'))
    validate_snapshot(snapshot, case, plan)
    require(read(archive / 'public-config/parameters.json') == snapshot['node_parameters'],
            'Uploaded public node parameters differ from snapshot')
    # Committee.print sorts its JSON map; the snapshot separately retains launch order.
    committee = read(archive / 'public-config/committee.json')['authorities']
    require(set(committee) == set(snapshot['launch_authorities']),
            'Uploaded committee differs from snapshot')
    expected_logs = {p.name: sha(p) for group in log_files(archive / 'logs', case) for p in group}
    validate_client_configuration(archive, case, snapshot, committee)
    require(meta.get('fresh_log_sha256') == expected_logs, 'Raw log freshness record differs')
    require(all(meta.get('previous_log_sha256', {}).get(name) != digest
                for name, digest in expected_logs.items()), 'Unchanged raw logs are stale')
    result = evaluate(native, case)
    recalculated = evaluate(reparse(archive, case, plan), case)
    require(result == recalculated, 'Native summary does not match reparsed raw logs')
    require(read(archive / 'acceptance.json') == result, 'Stored acceptance decision differs from evidence')
    return result


def selected_archive(output, case, selected):
    attempt = selected.get('attempt')
    require(isinstance(attempt, str) and Path(attempt).name == attempt and attempt.startswith('attempt-'),
            'Invalid selected attempt path')
    return output / case_key(case) / attempt


def build_report(output, campaign):
    rows, missing, invalid, run_ids = [], 0, 0, set()
    for case in campaign['cases']:
        directory = output / case_key(case)
        selected_path = directory / 'selected.json'
        row = dict(case=case, status='missing', passed=False,
                   attempt_count=len(list(directory.glob('attempt-*/metadata.json'))))
        if not selected_path.is_file():
            missing += 1
        else:
            try:
                selected = read(selected_path)
                archive = selected_archive(output, case, selected)
                ident = campaign.get('identities', {}).get(case['protocol'])
                require(ident is not None, 'Campaign protocol build identity is missing')
                measurement = audit_archive(archive, case, ident)
                require(read(archive / 'metadata.json') == selected, 'Selected pointer differs from immutable attempt')
                require(measurement['run_id'] not in run_ids, 'Duplicate native run_id across cases')
                run_ids.add(measurement['run_id'])
                row.update(status='valid', archive=str(archive), selected_attempt=archive.name, **measurement)
            except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
                invalid += 1
                row.update(status='invalid', error=str(error))
        rows.append(row)
    complete = not missing and not invalid and len(rows) == len(campaign['cases'])
    return dict(schema=1, generated_utc=now(), variant=campaign.get('variant'),
                complete=complete, passed=complete and all(row['passed'] for row in rows),
                planned_cases=len(rows), missing_cases=missing, invalid_cases=invalid,
                valid_cases=sum(row['status'] == 'valid' for row in rows),
                passing_cases=sum(row['passed'] for row in rows), rows=rows,
                criterion='E2E release -> final ordered commit in [start,end); '
                          'unweighted mean across every active replica < 3000 ms',
                limitation='Completion-conditional sampled latency; report release-cohort unfinished '
                           'samples and throughput. No minimum completion ratio or TPS target is imposed.')


def write_report(output, campaign):
    report = build_report(output, campaign)
    save(output / 'acceptance-report.json', report)
    lines = [f"A1 acceptance: {'PASS' if report['passed'] else 'NOT PASSED'}",
             f"Variant: {report['variant']}",
             f"Full matrix: {report['valid_cases']}/{report['planned_cases']} valid; "
             f"{report['passing_cases']} under 3000 ms; {report['missing_cases']} missing; {report['invalid_cases']} invalid.",
             report['criterion'], report['limitation'], '',
             '| Protocol | N | Nominal/input/offered TPS | Commit E2E mean/max ms | Release E2E mean ms | Final TPS | Completed/unfinished sample-replica pairs | Min completion | Result |',
             '|---|---:|---:|---:|---:|---:|---:|---:|---|']
    errors = []
    for row in report['rows']:
        c = row['case']
        prefix = f"| {c['protocol']} | {c['nodes']} | {c['nominal_rate_tps']}/{c['input_rate_tps']}/{c['offered_rate_tps']} |"
        if row['status'] == 'valid':
            result = 'PASS' if row['passed'] else 'FAIL >=3000 ms'
            if row['window_disagreement']:
                result += '; WINDOW DISAGREEMENT'
            if row['attempt_count'] > 1:
                result += f"; {row['attempt_count']} attempts"
            lines.append(prefix + f" {row['commit_e2e_ms_mean']:.3f}/{row['commit_e2e_ms_max']:.3f} | "
                         f"{row['release_e2e_ms_mean']:.3f} | {row['commit_final_tps_mean']:.3f} | "
                         f"{row['release_completed_samples_sum']}/{row['release_unfinished_samples_sum']} | "
                         f"{row['release_completion_ratio_min']:.2%} | {result} |")
        else:
            lines.append(prefix + f" — | — | — | — | — | {row['status'].upper()} |")
            if row.get('error'):
                errors.append(f"{case_key(c)}: {row['error']}")
    if errors:
        lines += ['', 'Invalid evidence:', '', *[f'- {error}' for error in errors]]
    (output / 'acceptance-report.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')
    print('\n'.join(lines[:5]), flush=True)
    return report


def file_state(directory):
    return {p.name: dict(sha256=sha(p), size=p.stat().st_size)
            for p in directory.glob('*') if p.is_file()}


def archive_evidence(bench, archive, before):
    """Copy downloaded evidence even on failure; save only this run's appended summaries."""
    destination = archive / 'native-results'
    destination.mkdir(exist_ok=True)
    for name, state in file_state(bench / 'results').items():
        if state == before.get(name):
            continue
        source = bench / 'results' / name
        if name.endswith(('.txt', '.jsonl')) and name in before:
            payload = source.read_bytes()
            prefix = payload[:before[name]['size']]
            require(hashlib.sha256(prefix).hexdigest() == before[name]['sha256'],
                    f'Native append-only summary was replaced during run: {name}')
            (destination / name).write_bytes(payload[len(prefix):])
        else:
            shutil.copy2(source, destination / name)
    public = archive / 'public-config'
    public.mkdir(exist_ok=True)
    for source, name in [('.committee.json', 'committee.json'), ('.parameters.json', 'parameters.json')]:
        if (bench / source).is_file():
            shutil.copy2(bench / source, public / name)


def all_run_ids(output):
    ids = set()
    for path in output.glob('Orca-*/n*/r*/attempt-*/metadata.json'):
        value = read(path).get('run_id')
        if value:
            ids.add(value)
    return ids


def run_case(case, bench, output, expected_identity, retry_failed=False, rerun_failed=False):
    directory = output / case_key(case)
    directory.mkdir(parents=True, exist_ok=True)
    selected_path = directory / 'selected.json'
    selected = None
    if selected_path.exists():
        selected = read(selected_path)
        require(selected.get('case') == case and selected.get('identity') == expected_identity,
                f'Cannot resume a different identity/configuration: {directory}')
        if selected.get('status') == 'complete':
            prior = audit_archive(selected_archive(output, case, selected), case, expected_identity)
            require(selected == read(selected_archive(output, case, selected) / 'metadata.json'),
                    'Selected pointer differs from attempt metadata')
            if prior['passed'] or not rerun_failed:
                print(f"REVALIDATED {case_key(case)}: {'PASS' if prior['passed'] else 'FAIL'}", flush=True)
                return
        else:
            require(retry_failed, f'Previous attempt incomplete/failed: {directory}; inspect it, then use --retry-failed')
    known_ids = all_run_ids(output)
    attempt = 'attempt-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ') + '-' + uuid.uuid4().hex[:8]
    archive = directory / attempt
    archive.mkdir()
    raw_logs = archive / 'logs'
    raw_logs.mkdir()
    cmd = command(case)
    before = file_state(bench / 'results')
    previous_logs = {}
    if selected:
        previous_logs = selected.get('fresh_log_sha256', {})
    meta = dict(schema=1, status='running', attempt=attempt, case=case,
                identity=expected_identity, command=cmd, cwd=str(bench),
                started_utc=now(), started_us=time.time_ns() // 1000,
                previous_log_sha256=previous_logs,
                previous_attempt=selected['attempt'] if selected else None,
                selection_reason=('initial' if selected is None else
                                  'explicit_latency_repeat' if selected['status'] == 'complete' else
                                  'retry_failed_or_interrupted'),
                controller_git_status=git(bench, 'status', '--porcelain'), returncode=None)
    shutil.copy2(bench / 'settings.json', archive / 'settings.json')
    shutil.copy2(bench / 'benchmark/measurements.py', archive / 'measurement_parser.py')
    shutil.copy2(Path(__file__), archive / 'acceptance_runner.py')
    save(archive / 'metadata.json', meta)
    save(selected_path, meta)
    (archive / 'command.txt').write_text(f'cd {shlex.quote(str(bench))}\n{shlex.join(cmd)}\n', encoding='utf-8')
    process = None
    try:
        require(identity(bench) == expected_identity, 'Controller identity changed before this attempt')
        print(f'RUN {case_key(case)}: {shlex.join(cmd)}', flush=True)
        with (archive / 'console.log').open('w', encoding='utf-8') as console:
            environment = {**os.environ, 'ORCA_BENCHMARK_LOGS_DIRECTORY': str(raw_logs.resolve())}
            process = subprocess.Popen(cmd, cwd=bench, stdin=subprocess.DEVNULL, env=environment,
                                       stdout=console, stderr=subprocess.STDOUT, start_new_session=True)
            # Finite controller timeout includes upload, readiness, arm and download overhead.
            process.wait(timeout=case['warmup_s'] + case['duration_s'] + case['drain_s'] + 900)
            meta['returncode'] = process.returncode
        meta['finished_us'] = time.time_ns() // 1000
        archive_evidence(bench, archive, before)
        require(meta['returncode'] == 0, f'fab remote exited {meta["returncode"]}; inspect console.log')
        require(identity(bench) == expected_identity, 'Controller identity changed during this attempt')
        native = read(sole(archive / 'native-results', '*.comparison.json'))
        plan = read(archive / 'logs/measurement-window.json')
        meta['run_id'] = plan.get('run_id')
        require(meta['run_id'] and meta['run_id'] not in known_ids, 'Native run_id was already used; stale attempt')
        require(native.get('plan') == plan, 'Native comparison and raw clock plan differ')
        meta['fresh_log_sha256'] = {p.name: sha(p) for group in log_files(archive / 'logs', case) for p in group}
        save(archive / 'acceptance.json', evaluate(native, case))
        meta['status'] = 'complete'
        meta['finished_utc'] = now()
        save(archive / 'metadata.json', meta)
        seal(archive)
        result = audit_archive(archive, case, expected_identity)
        print(f"{'PASS' if result['passed'] else 'FAIL'} {case_key(case)}: "
              f"E2E={result['commit_e2e_ms_mean']:.3f} ms; TPS={result['commit_final_tps_mean']:.3f}; "
              f"min release completion={result['release_completion_ratio_min']:.2%}; "
              f"unfinished pairs={result['release_unfinished_samples_sum']}", flush=True)
    except BaseException as error:
        if process is not None and process.poll() is None:
            # Keep the controller lock until Fabric exits. SIGINT gives native cleanup a chance.
            os.killpg(process.pid, signal.SIGINT)
            try:
                process.wait(timeout=90)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            meta['returncode'] = process.returncode
        meta.update(status='interrupted' if isinstance(error, KeyboardInterrupt) else 'failed',
                    error=str(error), finished_us=time.time_ns() // 1000, finished_utc=now())
        try:
            archive_evidence(bench, archive, before)
        except Exception as archive_error:
            meta['archive_error'] = str(archive_error)
        raise
    finally:
        meta.setdefault('finished_us', time.time_ns() // 1000)
        meta.setdefault('finished_utc', now())
        save(archive / 'metadata.json', meta)
        seal(archive)
        save(selected_path, meta)


@contextmanager
def controller_lock():
    import fcntl
    with LOCK.open('a') as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError('Mysticeti/figure matrix or another acceptance run is active; '
                             'wait for completion before using this testbed') from error
        yield


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('/home/ubuntu/experiments'),
                        help='Parent directory containing Orca-A/benchmark and Orca-B/benchmark')
    parser.add_argument('--output', type=Path, required=True,
                        help='Separate acceptance campaign directory; never reuse a figure-matrix output')
    parser.add_argument('--protocol', choices=('all', *PROTOCOLS), default='all',
                        help='Execution filter; full campaign always contains both protocols')
    parser.add_argument('--nodes', choices=NODES, type=int,
                        help='Execution filter for an existing installed testbed; full campaign still has 30 cases')
    parser.add_argument('--duration', type=int, default=60)
    parser.add_argument('--warmup', type=int, default=10)
    parser.add_argument('--drain', type=int, default=10)
    parser.add_argument('--header-size', type=int, default=20)
    parser.add_argument('--variant', default='common-clock', help='Provenance label; changes require a new output')
    parser.add_argument('--dry-run', '--plan', action='store_true', help='Print commands; no writes, cloud calls or build required')
    parser.add_argument('--report-only', action='store_true', help='Revalidate all archived cases offline and write reports')
    parser.add_argument('--retry-failed', action='store_true', help='Run a new attempt for interrupted/invalid native runs')
    parser.add_argument('--rerun-latency-failures', action='store_true', help='Explicitly replace a valid >=3000 ms attempt; retain old evidence')
    args = parser.parse_args(argv)
    output = args.output.expanduser().resolve()
    try:
        plan = cases(duration=args.duration, warmup=args.warmup, drain=args.drain, header_size=args.header_size)
        chosen = [c for c in plan if (args.protocol == 'all' or c['protocol'] == args.protocol)
                  and (args.nodes is None or c['nodes'] == args.nodes)]
        if args.dry_run:
            print('Full acceptance: 30 cases; subset execution cannot establish overall PASS.')
            print(f'Window warmup/measure/drain={args.warmup}/{args.duration}/{args.drain}s; '
                  f'header={args.header_size}B; variant={args.variant}; '
                  f'output={args.output.as_posix()}')
            print(f'Serial execution shares {LOCK.as_posix()}; start only after the existing matrix finishes.')
            for case in chosen:
                bench = PurePosixPath(args.root.as_posix()) / case['protocol'] / 'benchmark'
                print(f"\n# {case_key(case)}; nominal={case['nominal_rate_tps']}; "
                      f"offered={case['offered_rate_tps']}; active={len(case['active_launch_ids'])}")
                print(f'cd {shlex.quote(str(bench))}\n{shlex.join(command(case))}')
            return 0
        if args.report_only:
            campaign = read(output / 'campaign.json')
            require(campaign.get('schema') == 1 and len(campaign.get('cases', [])) == 30,
                    'Invalid acceptance campaign; expected all 30 cases')
            first = campaign['cases'][0]
            require(campaign['cases'] == cases(duration=first['duration_s'], warmup=first['warmup_s'],
                                              drain=first['drain_s'], header_size=first['header_size_bytes']),
                    'Campaign planned case set differs from the full acceptance matrix')
            return 0 if write_report(output, campaign)['passed'] else 1
        require(sys.platform == 'linux', 'Execute on the Linux EC2 controller; --dry-run and --report-only are portable')
        require(shutil.which('fab') is not None, 'fab is missing; activate the configured benchmark environment')
        def interrupted(signum, frame):
            raise KeyboardInterrupt(f'signal {signum}')
        signal.signal(signal.SIGTERM, interrupted)
        with controller_lock():
            campaign = open_campaign(output, plan, args.variant)
            try:
                for protocol in dict.fromkeys(c['protocol'] for c in chosen):
                    bench = (args.root / protocol / 'benchmark').resolve()
                    ident = identity(bench)
                    require(read(bench / 'settings.json')['repo']['name'] == protocol,
                            f'{protocol} settings.json points at another repository')
                    prior = campaign['identities'].get(protocol)
                    require(prior is None or prior == ident,
                            f'{protocol} source/build/settings changed; start a new --output campaign')
                    campaign['identities'][protocol] = ident
                    save(output / 'campaign.json', campaign)
                    for case in (c for c in chosen if c['protocol'] == protocol):
                        run_case(case, bench, output, ident, args.retry_failed, args.rerun_latency_failures)
            finally:
                report = write_report(output, campaign)
        return 0 if report['passed'] else 1
    except KeyboardInterrupt:
        print('Interrupted. Evidence retained; verify the configured testbed is idle before retrying.', file=sys.stderr)
        return 130
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(f'Acceptance error: {error}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
