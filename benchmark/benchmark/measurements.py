"""Observational latency/traffic statistics, independent of benchmark runners.

Sample IDs are scoped by the matched client/worker log pair. Commit maps are
kept per replica. No send timestamp or rule-ready timestamp substitutes for
a missing release, ingress, or final ordered commit event.
"""
from datetime import datetime, timezone
import json
from math import ceil, isfinite
import re
from statistics import mean


_TIME = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})')
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_VERSION = 'Benchmark latency instrumentation version 1'
_NARWHAL_BATCH_FORMAT = 'Benchmark worker batch format narwhal'
_NARWHAL_BATCH = re.compile(r'\bBatch (\S+) contains (?:(\d+) B|sample tx (\d+))$')
_FINAL_RECORD = re.compile(
    r'Committed B\d+\([^()\s]+\) -> (\S+) @ (\d+) commit (\d+) commit_us (\d+)'
)
_BATCH_OBSERVATION = re.compile(r'Benchmark batch (\S+) bytes (\d+) samples (\[.*\])')


def validate_clock_plan(plan):
    """Validate the shared absolute UTC plan without changing caller data."""
    fields = ('load_us', 'start_us', 'end_us', 'drain_end_us')
    if not isinstance(plan, dict) or set(plan) != {'schema', 'run_id', *fields}:
        raise ValueError('Clock plan requires schema, run_id, load_us, start_us, end_us, drain_end_us')
    if type(plan['schema']) is not int or plan['schema'] != 2:
        raise ValueError('Clock plan schema must be 2')
    if not isinstance(plan['run_id'], str) or not plan['run_id'].strip():
        raise ValueError('Clock plan run_id must be a nonempty string')
    if any(type(plan[key]) is not int or plan[key] < 0 for key in fields):
        raise ValueError('Clock plan timestamps must be nonnegative integer epoch microseconds')
    if not plan['load_us'] <= plan['start_us'] < plan['end_us'] <= plan['drain_end_us']:
        raise ValueError('Clock plan requires load_us <= start_us < end_us <= drain_end_us')
    return dict(plan)


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


def _sample_sequence(log, source_id):
    marker = 'Benchmark sample ID sequence'
    records = [line.partition(marker)[2] for line in log.splitlines() if marker in line]
    if not records:
        return 0, 1, 0  # Earlier clients encoded their burst counter directly.
    match = re.fullmatch(r' source (\d+) sources (\d+) mask (\d+)', records[0])
    if len(records) == 1 and match:
        rank, count, mask = map(int, match.groups())
        if rank < count < 1 << 64 and mask < 1 << 64:
            return rank, count, mask
    raise ValueError(f'Invalid sample ID sequence for source {source_id}')


def _narwhal_batch_observations(log):
    sizes, samples = {}, {}
    for line in log.splitlines():
        record = _NARWHAL_BATCH.search(line)
        if record is None:
            continue
        digest, size, sample = record.groups()
        if size is None:
            samples.setdefault(digest, []).append(int(sample))
        elif digest in sizes:
            raise ValueError(f'Ambiguous batch observation for {digest}')
        else:
            sizes[digest] = int(size)
    if samples.keys() - sizes.keys():
        raise ValueError('Missing batch size for Narwhal worker sample')
    return ((digest, size, samples.get(digest, [])) for digest, size in sizes.items())


def _orca_batch_observations(log, source_id):
    for line in log.splitlines():
        marker = line.find('Benchmark batch')
        if marker == -1:
            continue
        observation = _BATCH_OBSERVATION.fullmatch(line[marker:])
        if observation is None:
            raise ValueError(f'Malformed batch observation for source {source_id}')
        digest, size, samples = observation.groups()
        try:
            ids = json.loads(samples)
        except ValueError as error:
            raise ValueError(f'Malformed batch observation for {digest}') from error
        yield digest, int(size), ids


def aba_counter_samples(log):
    return [tuple(map(int, values)) for values in re.findall(
        r'Benchmark ABA bytes at_us (\d+) elapsed_us (\d+) sent (\d+) received (\d+)', log)]


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
        self.client_texts = tuple(clients)
        self.worker_texts = tuple(workers)
        self.primary_texts = tuple(primaries)
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
            self.aba.append(aba_counter_samples(primary))
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
        return self._release_metrics(start, end, cutoff,
                                     'configured_duration' if duration is not None else 'observed_log_span')

    def _release_metrics(self, start, end, cutoff, basis, exclusive_cutoff=False,
                         sources=None, finals=None):
        sources = self.sources if sources is None else sources
        finals = self.final if finals is None else finals
        cohort = [(source_id, ident, stamp)
                  for source_id, source in enumerate(sources)
                  for ident, stamp in source['release'].items()
                  if start <= stamp < end]
        supported = sum(source['supported'] for source in sources)
        status = ('available' if supported == len(sources) and sources
                  else 'partial' if supported else 'unavailable')
        replicas, pooled = [], [[], [], []]
        for replica, final in enumerate(finals):
            values = [[], [], []]
            completed = not_received = missing_ingress = invalid = 0
            for source_id, ident, released in cohort:
                source = sources[source_id]
                received = source['ingress'].get(ident)
                if received is None or received > cutoff or (exclusive_cutoff and received == cutoff):
                    not_received += 1
                committed = min((final[batch] for batch in source['batches'].get(ident, ())
                                 if batch in final), default=None)
                if committed is None or committed > cutoff or (exclusive_cutoff and committed == cutoff):
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
            'window_basis': basis,
            'window_start_us': start, 'window_end_us': end, 'commit_cutoff_us': cutoff,
            'window_seconds': (end - start) / 1000000,
            'last_observed_log_us': self.last_log_us,
            'source_pairs': len(sources), 'instrumented_source_pairs': supported,
            'source_logs': self.source_logs,
            'released_samples': len(cohort),
            'replica_aggregation': 'pooled paired transaction-replica observations; no earliest-replica merge',
            'replicas': replicas,
            'pooled': {name: _distribution(samples) for name, samples in zip(
                ('release_to_final', 'ingress_to_final', 'release_to_ingress'), pooled)},
            'aba': self._aba_metrics(start, end),
        }
        return result

    def clock_metrics(self, plan):
        """Fixed release cohort plus per-replica final commits in [start, end).

        The release cohort drains only until drain_end_us (exclusive). The
        commit cohort can include releases during warmup and uses their
        original timestamps. Replica means are averaged with equal weight;
        missing evidence never supplies zero latency or a successful grade.
        This method neither reads files nor modifies the plan or measurements.
        """
        plan = validate_clock_plan(plan)
        clock_sources, sources, batch_sizes, finals = self._clock_evidence(plan)
        start, end = plan['start_us'], plan['end_us']
        seconds = (end - start) / 1000000
        release = self._release_metrics(start, end, plan['drain_end_us'],
                                        'planned_release_window', exclusive_cutoff=True,
                                        sources=sources, finals=finals)
        tx_sizes = {int(value) for log in self.client_texts
                    for value in re.findall(r'Transactions size: (\d+)', log)}
        if len(tx_sizes) != 1 or min(tx_sizes) <= 0 or any(
                not re.search(r'Transactions size: (\d+)', log) for log in self.client_texts):
            raise ValueError('Clocked throughput requires equal positive transaction sizes in every client log')
        tx_size = next(iter(tx_sizes))
        if any(size % tx_size for size in batch_sizes.values()):
            raise ValueError('Batch bytes are not a whole number of fixed-size transactions')
        replicas = []
        pooled_release_to_final = []
        for replica, all_final in enumerate(finals):
            final = {batch: at for batch, at in all_final.items() if start <= at < end}
            values, selected, missing_release, missing_ingress, invalid = [], 0, 0, 0, 0
            for source in sources:
                for ident, batches in source['batches'].items():
                    committed = next((final[batch] for batch in batches if batch in final), None)
                    if committed is None:
                        continue
                    selected += 1
                    released, ingress = source['release'].get(ident), source['ingress'].get(ident)
                    if released is None:
                        missing_release += 1
                    if ingress is None:
                        missing_ingress += 1
                    if released is None or ingress is None:
                        continue
                    if not released <= ingress <= committed:
                        invalid += 1
                        continue
                    values.append(committed - released)
            pooled_release_to_final.extend(values)
            byte_count = sum(batch_sizes[batch] for batch in final)
            complete = (bool(values) and not (missing_release or missing_ingress or invalid)
                        and release['instrumented_source_pairs'] == len(self.sources))
            replicas.append({
                'replica': replica, 'replica_log': self.replica_logs[replica],
                'final_batches': len(final), 'final_bytes': byte_count,
                'final_transactions': byte_count / tx_size,
                'final_tps': byte_count / tx_size / seconds,
                'final_bps': byte_count / seconds,
                'missing_batch_sizes': [],
                'selected_samples': selected, 'matched_samples': len(values),
                'missing_release_samples': missing_release,
                'missing_ingress_samples': missing_ingress, 'invalid_time_samples': invalid,
                'coverage_complete': complete, 'throughput_coverage_complete': True,
                'release_to_final': _distribution(values),
            })
        complete = bool(replicas) and all(row['coverage_complete'] for row in replicas)
        throughput_complete = bool(replicas) and all(row['throughput_coverage_complete'] for row in replicas)
        matched = sum(row['matched_samples'] for row in replicas)
        selected = sum(row['selected_samples'] for row in replicas)
        pooled_complete = bool(matched) and matched == selected and release['status'] == 'available'
        commit = {
            'window_basis': 'planned_final_commit_window',
            'window_start_us': start, 'window_end_us': end, 'window_seconds': seconds,
            'replica_aggregation': 'arithmetic mean of per-replica means; no earliest-replica merge',
            'coverage_basis': 'selected transaction-replica samples have release, ingress, final and batch bytes; process uptime is verified by the runner',
            'expected_replicas': len(replicas),
            'replicas_with_samples': sum(row['matched_samples'] > 0 for row in replicas),
            'selected_samples': selected,
            'matched_samples': matched, 'coverage_complete': complete,
            'throughput_coverage_complete': throughput_complete,
            'status': 'available' if complete else 'partial' if matched else 'unavailable',
            'mean_replica_release_to_final_ms': mean(row['release_to_final']['mean_ms'] for row in replicas) if complete else None,
            'pooled_release_to_final': _distribution(pooled_release_to_final) if pooled_complete else None,
            'mean_replica_final_tps': mean(row['final_tps'] for row in replicas) if throughput_complete else None,
            'mean_replica_final_bps': mean(row['final_bps'] for row in replicas) if throughput_complete else None,
            'replicas': replicas,
        }
        return {'schema': 2, 'run_id': plan['run_id'], 'plan': plan,
                'clock_sources': clock_sources, 'release_window': release, 'commit_window': commit}

    def _clock_evidence(self, plan):
        clocks, sources, batch_sizes, finals = [], [], {}, []
        if not self.sources or not self.final:
            raise ValueError('Clocked measurements require client/worker pairs and primary logs')
        for log in self.client_texts + self.worker_texts + self.primary_texts:
            if re.search(r'\b(?:panicked|panic|Error|ERROR)\b', log):
                raise ValueError('Runtime failure in clocked measurement logs')
        for replica, log in enumerate(self.primary_texts):
            if not log.strip():
                raise ValueError(f'Missing primary log for replica {replica}')
            final = {}
            for line in log.splitlines():
                match = re.search(r'\bCommitted B', line)
                if match is None:
                    continue
                message = line[match.start():]
                if re.fullmatch(r'Committed B\d+\([^()\s]+\)', message):
                    continue  # A header summary carries no batch or timestamp.
                record = _FINAL_RECORD.fullmatch(message)
                if record is None:
                    raise ValueError(f'Malformed final commit record for replica {replica}')
                batch, _, milliseconds, microseconds = record.groups()
                stamp = int(microseconds)
                if stamp // 1000 != int(milliseconds):
                    raise ValueError(f'Inconsistent final commit timestamp for replica {replica}')
                final[batch] = min(final.get(batch, stamp), stamp)
            finals.append(final)
        for source_id, (client, worker, source) in enumerate(zip(
                self.client_texts, self.worker_texts, self.sources)):
            markers = {}
            for label in ('clock ready', 'clock armed', 'input started', 'input stopped'):
                lines = [line for line in client.splitlines() if f'Benchmark {label}' in line]
                if len(lines) != 1:
                    raise ValueError(f'Missing or ambiguous Benchmark {label} evidence for source {source_id}')
                markers[label] = lines[0].split(f'Benchmark {label}', 1)[1].strip()
            try:
                armed = validate_clock_plan(json.loads(markers['clock armed']))
            except (ValueError, TypeError) as error:
                raise ValueError(f'Invalid armed plan for source {source_id}: {error}') from error
            if armed != plan:
                raise ValueError(f'Armed clock plan does not match the requested plan for source {source_id}')
            started = re.fullmatch(r'at_us (\d+) lateness_us (\d+)', markers['input started'])
            stopped = re.fullmatch(r'at_us (\d+) released_samples (\d+)', markers['input stopped'])
            if not started or not stopped:
                raise ValueError(f'Invalid clock input timestamps for source {source_id}')
            started_us, lateness_us = map(int, started.groups())
            stopped_us, released_samples = map(int, stopped.groups())
            if (not plan['load_us'] <= started_us <= plan['load_us'] + 500000
                    or lateness_us != started_us - plan['load_us']):
                raise ValueError(f'Invalid or late input start for source {source_id}')
            if stopped_us < plan['end_us']:
                raise ValueError(f'Input stopped before the planned end for source {source_id}')
            if any(not started_us <= stamp < plan['end_us'] for stamp in source['release'].values()):
                raise ValueError(f'Sample release outside the planned input interval for source {source_id}')
            for log, kind in ((client, 'released'), (worker, 'ingress')):
                stamps = {}
                for line in log.splitlines():
                    if f'Benchmark sample {kind}' in line and not re.search(
                            rf'Benchmark sample {kind} id \d+ at_us \d+$', line):
                        raise ValueError(f'Malformed sample {kind} event for source {source_id}')
                for ident, at in re.findall(rf'Benchmark sample {kind} id (\d+) at_us (\d+)', log):
                    stamps.setdefault(int(ident), []).append(int(at))
                if any(len(values) != 1 for values in stamps.values()):
                    raise ValueError(f'Ambiguous sample {kind} timestamps for source {source_id}')
            rank, count, mask = _sample_sequence(client, source_id)
            if (len(source['release']) != released_samples
                    or any(ident != ((position * count + rank) ^ mask)
                           for position, ident in enumerate(source['release']))):
                raise ValueError(f'Incomplete sample release sequence for source {source_id}')
            batches = {}
            observations = (_narwhal_batch_observations(worker) if _NARWHAL_BATCH_FORMAT in worker
                            else _orca_batch_observations(worker, source_id))
            for batch, size, ids in observations:
                if (size <= 0 or any(type(ident) is not int or ident < 0 for ident in ids)):
                    raise ValueError(f'Invalid batch observation for {batch}')
                if batch in batch_sizes:
                    raise ValueError(f'Ambiguous batch observation for {batch}')
                batch_sizes[batch] = size
                for ident in ids:
                    if ident in batches:
                        raise ValueError(f'Ambiguous sample-to-batch mapping for source {source_id}')
                    batches[ident] = {batch}
            sources.append(dict(source, batches=batches))
            clocks.append(dict(source=source_id, **self.source_logs[source_id],
                               input_started_us=started_us, input_stopped_us=stopped_us,
                               lateness_us=lateness_us, released_samples=released_samples))
        relevant_batches = {batch for final in finals for batch, stamp in final.items()
                            if stamp < plan['drain_end_us']}
        missing = relevant_batches - batch_sizes.keys()
        if missing:
            raise ValueError(f'Missing batch observation for {len(missing)} final batches: {sorted(missing)[:10]}')
        return clocks, sources, batch_sizes, finals

    def _aba_metrics(self, start, end):
        return self.aba_metrics(self.aba, self.replica_logs, start, end)

    @staticmethod
    def aba_metrics(counters, replica_logs, start, end):
        if not any(counters):
            return None
        replicas = []
        for replica, samples in enumerate(counters):
            row = {
                'replica': replica, 'replica_log': replica_logs[replica],
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


def format_clock_metrics(metrics):
    display = lambda value: f'{value:,.3f}' if value is not None else 'N/A'
    plan, commit = metrics['plan'], metrics['commit_window']
    lines = [
        ' + CLOCKED FINAL COMMIT MEASUREMENTS:',
        f" Run ID: {metrics['run_id']}",
        f" Clocked measurement window: {commit['window_seconds']:.3f} s; "
        f"fixed drain {(plan['drain_end_us'] - plan['end_us']) / 1000000:.3f} s",
        f" UTC interval: [{plan['start_us']}, {plan['end_us']}) us; "
        f"release cohort final cutoff: {plan['drain_end_us']} us (exclusive)",
        ' End-to-end population: final commits in the measurement window; original client release, including warmup',
        ' Replica aggregation: arithmetic mean of per-replica means',
        f" End-to-end latency: {display(commit['mean_replica_release_to_final_ms'])} ms",
        f" End-to-end TPS: {display(commit['mean_replica_final_tps'])} tx/s (mean per replica)",
        f" Evidence: {commit['status']}; replicas with matched samples "
        f"{commit['replicas_with_samples']}/{commit['expected_replicas']}; "
        f"matched/selected observations {commit['matched_samples']}/{commit['selected_samples']}",
    ]
    for row in commit['replicas']:
        lines.append(f" Final-window replica {row['replica']}: mean "
                     f"{display(row['release_to_final']['mean_ms'])} ms; "
                     f"TPS {display(row['final_tps'])}; matched/selected "
                     f"{row['matched_samples']}/{row['selected_samples']}; "
                     f"missing release {row['missing_release_samples']}; "
                     f"missing ingress {row['missing_ingress_samples']}; "
                     f"invalid clocks {row['invalid_time_samples']}; "
                     f"coverage complete {row['coverage_complete']}")
    lines.append(' Release-window statistics (release cohort, fixed drain):')
    return '\n'.join(lines) + '\n' + format_measurements(metrics['release_window'])


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
