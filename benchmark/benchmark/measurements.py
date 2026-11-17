"""Observational latency/traffic statistics, independent of benchmark runners.

Sample IDs are scoped by the matched client/worker log pair. Commit maps are
kept per replica. No send timestamp or rule-ready timestamp substitutes for
a missing release, ingress, or final ordered commit event.
"""
from datetime import datetime, timezone
from math import ceil, isfinite
import re
from statistics import mean


_TIME = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})')
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_VERSION = 'Benchmark latency instrumentation version 1'


def _unix_us(text):
    match = _TIME.search(text)
    if not match:
        return None
    dt = datetime.fromisoformat(match.group().replace('Z', '+00:00')) - _EPOCH
    return (dt.days * 86400 + dt.seconds) * 1000000 + dt.microseconds


def _samples(log, kind):
    result = {}
    for ident, stamp in re.findall(
            rf'Benchmark sample {kind} id (\d+) at_us (\d+)', log):
        ident, stamp = int(ident), int(stamp)
        result[ident] = min(result.get(ident, stamp), stamp)
    return result


def _distribution(values):
    ordered = sorted(values)
    n = len(ordered)
    percentile = lambda p: ordered[ceil(p * n) - 1] / 1000 if n else None
    return {
        'count': n,
        'mean_ms': mean(ordered) / 1000 if n else None,
        'p50_ms': percentile(.50),
        'p95_ms': percentile(.95),
        'p99_ms': percentile(.99),
    }


class Measurements:
    def __init__(self, clients, primaries, workers):
        if len(clients) != len(workers):
            raise ValueError('Measurement statistics require one client log per worker log')
        self.sources = []
        self.starts = []
        self.last_log_us = 0
        self.final = []
        self.aba = []
        self.replica_logs = [f'primary-log-{i}' for i in range(len(primaries))]
        self.source_logs = [{'client': f'client-log-{i}', 'worker': f'worker-log-{i}'}
                            for i in range(len(clients))]
        for log in clients + primaries + workers:
            for stamp in _TIME.finditer(log):
                self.last_log_us = max(self.last_log_us, _unix_us(stamp.group()))
        for client, worker in zip(clients, workers):
            start = next((_unix_us(line) for line in client.splitlines()
                          if 'Start sending transactions' in line), None)
            if start is not None:
                self.starts.append(start)
            release = _samples(client, 'released')
            ingress = _samples(worker, 'ingress')
            batches = {}
            for batch, ident in re.findall(
                    r'Batch (?:Ok\()?([^()\s]+)\)? contains sample tx (\d+)', worker):
                batches.setdefault(int(ident), set()).add(batch)
            for stamps in (release, ingress):
                self.last_log_us = max(self.last_log_us, max(stamps.values(), default=0))
            self.sources.append({
                'release': release, 'ingress': ingress, 'batches': batches,
                'supported': (_VERSION in client or bool(release))
                             and (_VERSION in worker or bool(ingress)),
            })
        for primary in primaries:
            commits = {}
            for line in primary.splitlines():
                match = re.search(r'\bCommitted B\d+\([^)]*\) -> (\S+)', line)
                if not match:
                    continue
                explicit = re.search(r' @ \d+ commit (\d+)\b', line)
                micros = re.search(r'\bcommit_us (\d+)\b', line)
                # Orca's explicit commit field is FINAL; @ alone is rule-ready.
                stamp = (int(micros.group(1)) if micros else
                         int(explicit.group(1)) * 1000 if explicit else _unix_us(line))
                if stamp is None:
                    continue
                batch = match.group(1)
                commits[batch] = min(commits.get(batch, stamp), stamp)
            self.final.append(commits)
            self.last_log_us = max(self.last_log_us, max(commits.values(), default=0))
            self.aba.append([tuple(map(int, values)) for values in re.findall(
                r'Benchmark ABA bytes at_us (\d+) elapsed_us (\d+) sent (\d+) received (\d+)',
                primary)])
            self.last_log_us = max(self.last_log_us, max((s[0] for s in self.aba[-1]), default=0))

    def metrics(self, duration=None, warmup=0, drain=0):
        for label, value in (('warmup', warmup), ('drain', drain)):
            if not isfinite(value) or value < 0:
                raise ValueError(f'{label} must be nonnegative and finite')
        if duration is not None and (not isfinite(duration) or duration <= 0):
            raise ValueError('Measurement duration must be positive and finite')
        first = min(self.starts, default=min(
            (stamp for source in self.sources for stamp in source['release'].values()),
            default=0))
        start = first + round(warmup * 1000000)
        # The observed mode includes the final logged release, even if the
        # client then blocks and no subsequent event is available.
        end = (start + round(duration * 1000000) if duration is not None
               else max(start, self.last_log_us + 1))
        cutoff = end + round(drain * 1000000)
        cohort = [(source_id, ident, stamp)
                  for source_id, source in enumerate(self.sources)
                  for ident, stamp in source['release'].items()
                  if start <= stamp < end]
        supported = sum(source['supported'] for source in self.sources)
        status = ('available' if supported == len(self.sources) and self.sources
                  else 'partial' if supported else 'unavailable')
        replicas, pooled = [], [[], [], []]
        for replica, final in enumerate(self.final):
            values = [[], [], []]
            completed = not_received = missing_ingress = invalid = 0
            for source_id, ident, released in cohort:
                source = self.sources[source_id]
                received = source['ingress'].get(ident)
                if received is None or received > cutoff:
                    not_received += 1
                committed = min((final[batch] for batch in source['batches'].get(ident, ())
                                 if batch in final), default=None)
                if committed is None or committed > cutoff:
                    continue
                completed += 1
                if received is None:
                    missing_ingress += 1
                    continue
                if not released <= received <= committed:
                    invalid += 1
                    continue
                for sample_list, value in zip(values, (committed - released,
                                                       committed - received,
                                                       received - released)):
                    sample_list.append(value)
            row = {
                'replica': replica, 'replica_log': self.replica_logs[replica],
                'released_samples': len(cohort),
                'completed_samples': completed,
                'unfinished_samples': len(cohort) - completed,
                'not_received_samples': not_received,
                'missing_ingress_samples': missing_ingress,
                'invalid_time_samples': invalid,
                'paired_samples': len(values[0]),
                'completion_ratio': completed / len(cohort) if cohort else None,
            }
            for name, samples, all_samples in zip(
                    ('release_to_final', 'ingress_to_final', 'release_to_ingress'), values, pooled):
                row[name] = _distribution(samples)
                all_samples.extend(samples)
            replicas.append(row)
        result = {
            'schema_version': 1, 'status': status,
            'window_basis': 'configured_duration' if duration is not None else 'observed_log_span',
            'window_start_us': start, 'window_end_us': end, 'commit_cutoff_us': cutoff,
            'window_seconds': (end - start) / 1000000,
            'last_observed_log_us': self.last_log_us,
            'source_pairs': len(self.sources), 'instrumented_source_pairs': supported,
            'source_logs': self.source_logs,
            'released_samples': len(cohort),
            'replica_aggregation': 'pooled paired transaction-replica observations; no earliest-replica merge',
            'replicas': replicas,
            'pooled': {name: _distribution(samples) for name, samples in zip(
                ('release_to_final', 'ingress_to_final', 'release_to_ingress'), pooled)},
            'aba': self._aba_metrics(start, end),
        }
        return result

    def _aba_metrics(self, start, end):
        if not any(self.aba):
            return None
        replicas = []
        for replica, samples in enumerate(self.aba):
            row = {
                'replica': replica, 'replica_log': self.replica_logs[replica],
                'status': 'insufficient_samples',
                'window_start_us': None, 'window_end_us': None, 'window_seconds': None,
                'sent_bytes': None, 'received_bytes': None,
                'sent_bytes_per_second': None, 'received_bytes_per_second': None,
            }
            invalid_counter = any(b[1] < a[1] or b[2] < a[2] or b[3] < a[3]
                                  for a, b in zip(samples, samples[1:]))
            invalid_clock = any(b[0] < a[0] for a, b in zip(samples, samples[1:]))
            selected = [s for s in samples if start <= s[0] <= end]
            if invalid_counter:
                row['status'] = 'invalid_counter'
            elif invalid_clock:
                row['status'] = 'invalid_wall_clock'
            elif len(selected) >= 2 and selected[-1][1] > selected[0][1]:
                first, last = selected[0], selected[-1]
                seconds = (last[1] - first[1]) / 1000000
                sent, received = last[2] - first[2], last[3] - first[3]
                row.update(status='available', window_start_us=first[0], window_end_us=last[0],
                           window_seconds=seconds, sent_bytes=sent, received_bytes=received,
                           sent_bytes_per_second=sent / seconds,
                           received_bytes_per_second=received / seconds)
            replicas.append(row)
        complete = all(r['status'] == 'available' for r in replicas)
        return {
            'unit': 'byte/s',
            'byte_scope': 'serialized signed ABA envelope + 4-byte frame prefix, per peer and successful retry; excludes ACK, TCP/IP, self-delivery, incomplete/failed writes',
            'window_basis': 'first/last complete counter snapshots inside the measurement window; elapsed_us denominator includes idle time',
            'replicas': replicas,
            'all_replicas_available': complete,
            # Replica windows are reported explicitly; never add Tx and Rx.
            'sum_replica_sent_bytes_per_second': sum(r['sent_bytes_per_second'] for r in replicas) if complete else None,
            'mean_replica_sent_bytes_per_second': mean(r['sent_bytes_per_second'] for r in replicas) if complete else None,
            'sum_replica_received_bytes_per_second': sum(r['received_bytes_per_second'] for r in replicas) if complete else None,
        }


def format_measurements(metrics):
    display = lambda value: f'{value:,.3f}' if value is not None else 'N/A'
    lines = [
        ' + FINAL COMMIT MEASUREMENTS:',
        f" Instrumentation: {metrics['status']} ({metrics['instrumented_source_pairs']}/{metrics['source_pairs']} source pairs)",
        f" Window: {metrics['window_basis']}, {metrics['window_seconds']:.3f} s",
        ' Latency population: paired transaction-replica samples (pooled raw observations)',
    ]
    for label, key in (
            ('Release -> final commit (main)', 'release_to_final'),
            ('Ingress -> final commit (auxiliary)', 'ingress_to_final'),
            ('Release -> ingress (paired decomposition)', 'release_to_ingress')):
        row = metrics['pooled'][key]
        lines.append(f" {label}: mean {display(row['mean_ms'])} ms; P50 {display(row['p50_ms'])} ms; "
                     f"P95 {display(row['p95_ms'])} ms; P99 {display(row['p99_ms'])} ms; n={row['count']}")
    for row in metrics['replicas']:
        lines.append(f" Replica {row['replica']}: completed/released {row['completed_samples']}/{row['released_samples']}; "
                     f"unfinished {row['unfinished_samples']}; not received {row['not_received_samples']}; "
                     f"missing ingress {row['missing_ingress_samples']}; invalid clocks {row['invalid_time_samples']}")
    aba = metrics['aba']
    if aba is not None:
        lines.append(' ABA traffic: signed envelope + 4-byte framing; per-peer writes including successful retries')
        for row in aba['replicas']:
            lines.append(f" ABA replica {row['replica']}: send {display(row['sent_bytes_per_second'])} byte/s; "
                         f"receive {display(row['received_bytes_per_second'])} byte/s; "
                         f"window {display(row['window_seconds'])} s; {row['status']}")
        lines.append(f" ABA sum of replica send rates: {display(aba['sum_replica_sent_bytes_per_second'])} byte/s")
        lines.append(f" ABA mean replica send rate: {display(aba['mean_replica_sent_bytes_per_second'])} byte/s")
    return '\n'.join(lines) + '\n'
