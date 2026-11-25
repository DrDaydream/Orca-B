# Copyright(C) Facebook, Inc. and its affiliates.
from datetime import datetime
from glob import glob
import json
from math import ceil, isfinite
from multiprocessing import Pool
from os.path import join
from pathlib import Path
from re import findall, search
from statistics import mean

from benchmark.utils import Print
from benchmark.adversary import MODES, crash_population
from benchmark.measurements import (Measurements, aba_counter_samples, format_measurements,
                                    format_clock_metrics, validate_clock_plan)


class ParseError(Exception):
    pass


def _logged_runtime_adversary(primaries):
    # Old runtime manifests stored the replica population. Their configure
    # records identify the mode and set size, but omit the individual IDs.
    previous = None
    for log in primaries:
        matches = findall(r'BENCH_ADVERSARY mode=(\w+) action=configure sender=\S+ fixed_byzantine=(\d+)', log)
        if len(matches) != 1 or (previous is not None and matches[0] != previous):
            return None, None
        previous = matches[0]
    if previous is None:
        return None, None
    raw_mode, count = previous
    return MODES.get(raw_mode.lower()), [] if int(count) == 0 else None


class LogParser:
    def __init__(self, clients, primaries, workers, faults=0, *,
                 primary_ids=None, excluded_primary_ids=()):
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.primary_ids = list(range(len(primaries))) if primary_ids is None else list(primary_ids)
        excluded = list(excluded_primary_ids)
        if (len(self.primary_ids) != len(primaries)
                or any(type(i) is not int or i < 0 for i in self.primary_ids + excluded)
                or len(set(self.primary_ids)) != len(self.primary_ids)
                or len(set(excluded)) != len(excluded)
                or not set(excluded) < set(self.primary_ids)):
            raise ParseError('Invalid primary IDs or silent replica exclusions')
        self.excluded_primary_ids = sorted(excluded)
        self.adversary_mode = 'unknown'
        self.byzantine_ids = None
        self.included_primary_ids = [i for i in self.primary_ids if i not in excluded]
        included = lambda values: [value for ident, value in zip(self.primary_ids, values)
                                   if ident not in excluded]

        self.crashed_primary_ids = []
        self.measurement_plan = None
        self.faults = faults
        if isinstance(faults, int):
            self.committee_size = len(primaries)
            self.workers =  len(workers) // len(primaries)
        else:
            self.committee_size = '?'
            self.workers = '?'

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse clients\' logs: {e}')
        self.size, self.rate, self.start, misses, self.sent_samples \
            = zip(*results)
        self.misses = sum(misses)

        # Parse the primaries logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries, primaries)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse nodes\' logs: {e}')
        proposals, commits, final_commits, header_proposals, header_commits, leader_ready, rule_orders, commit_rules, aba_durations, self.configs, primary_ips = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        # Keep proposal origins as transaction metadata, but only included
        # replicas may supply commit observations or rule/ABA statistics.
        self.commits = self._merge_results([x.items() for x in included(commits)])
        self.final_commits = self._merge_results(
            [x.items() for x in included(final_commits)]
        )
        self.header_proposals = self._merge_results([x.items() for x in header_proposals])
        self.header_commits = self._merge_tagged_results(included(header_commits))
        self.leader_ready = self._merge_results([x.items() for x in included(leader_ready)])
        self.rule_orders = self._merge_results([x.items() for x in included(rule_orders)])
        self.commit_rules = self._merge_commit_rules(included(commit_rules))
        # Keep every completed node-instance sample. Unfinished ABA instances
        # have no duration log and are intentionally excluded.
        self.aba_durations = [duration for values in included(aba_durations) for duration in values]
        # Silent authorities still run and may send ABA traffic.
        self.excluded_aba_counters = {
            ident: aba_counter_samples(log) for ident, log in zip(self.primary_ids, primaries)
            if ident in self.excluded_primary_ids
        }

        # Parse the workers logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers, workers)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse workers\' logs: {e}')
        sizes, self.received_samples, workers_ips = zip(*results)
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }

        # Observational statistics keep final commit times per replica.
        try:
            self.measurements = Measurements(clients, included(primaries), workers)
            self.measurements.replica_logs = [f'primary-{i}.log' for i in self.included_primary_ids]
        except ValueError as error:
            raise ParseError(str(error)) from error

        # Determine whether the primary and the workers are collocated.
        self.collocate = set(primary_ips) == set(workers_ips)

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

    def measurement_metrics(self, duration=None, warmup=0, drain=0):
        """Paired release/ingress -> final latency and optional ABA byte/s.

        Explicit duration uses [first client Start + warmup, Start + warmup
        + duration). Without it, the observed log span is labelled as such.
        No runner, workload, or protocol behavior is changed by this parser.
        """
        if self.measurement_plan is not None:
            # A saved clock plan is authoritative for every native report.
            return self.clock_metrics()['release_window']
        metrics = self.measurements.metrics(duration=duration, warmup=warmup, drain=drain)
        return self._measurement_population(metrics)

    def _measurement_population(self, metrics):
        metrics['included_primary_ids'] = self.included_primary_ids
        metrics['excluded_primary_ids'] = self.excluded_primary_ids
        metrics['crashed_primary_ids'] = self.crashed_primary_ids
        for row, ident in zip(metrics['replicas'], self.included_primary_ids):
            row['replica'] = ident
        if metrics['aba'] is not None:
            for row, ident in zip(metrics['aba']['replicas'], self.included_primary_ids):
                row['replica'] = ident
        average, total = self.aba_send_rates(metrics)
        size = self.size[0] if len(set(self.size)) == 1 and self.size[0] > 0 else None
        metrics['aba_committee'] = {
            'committee_size': self.committee_size,
            'total_sent_bytes_per_second': total,
            'mean_sent_bytes_per_second': average,
            'total_equivalent_tx_per_second': total / size if total is not None and size else None,
            'mean_equivalent_tx_per_second': average / size if average is not None and size else None,
        }
        return metrics

    def aba_send_rates(self, release):
        """Mean and total B/s require usable counters from every running authority."""
        aba = release['aba']
        if aba is None or not aba['all_replicas_available']:
            return None, None
        total = aba['sum_replica_sent_bytes_per_second']
        if self.excluded_aba_counters:
            silent = Measurements.aba_metrics(
                self.excluded_aba_counters.values(),
                [f'primary-{i}.log' for i in self.excluded_aba_counters],
                release['window_start_us'], release['window_end_us'])
            if silent is None or not silent['all_replicas_available']:
                return None, None
            total += silent['sum_replica_sent_bytes_per_second']
        if self.committee_size != len(self.primary_ids) + len(self.crashed_primary_ids):
            return None, None
        return total / self.committee_size, total

    def clock_metrics(self, plan=None):
        """Public planned windows using the included primary launch IDs."""
        saved = self.measurement_plan
        if plan is None:
            plan = saved
        elif saved is not None and plan != saved:
            raise ParseError('Requested clock plan does not match the saved measurement window')
        try:
            metrics = self.measurements.clock_metrics(plan)
        except ValueError as error:
            raise ParseError(f'Invalid clock measurements: {error}') from error
        self._measurement_population(metrics['release_window'])
        for row, ident in zip(metrics['commit_window']['replicas'], self.included_primary_ids):
            row['replica'] = ident
        for key in ('included_primary_ids', 'excluded_primary_ids', 'crashed_primary_ids'):
            metrics[key] = getattr(self, key)
        return metrics

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _merge_tagged_results(self, inputs):
        merged = {}
        for values in inputs:
            for digest, value in values.items():
                if digest not in merged or merged[digest][0] > value[0]:
                    merged[digest] = value
        return merged

    def _merge_commit_rules(self, inputs):
        merged = {}
        for values in inputs:
            for leader, value in values.items():
                previous = merged.get(leader)
                if previous is None:
                    merged[leader] = value
                elif previous[1] == 'skip' and value[1] == 'commit':
                    merged[leader] = value
                elif previous[1] == value[1] == 'commit' and value[0] < previous[0]:
                    merged[leader] = value
        return merged

    def _parse_clients(self, log):
        if search(r'(?:panicked|Error|ERROR)', log) is not None:
            raise ParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[?(\d{4}-\S+Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[?(\d{4}-\S+Z) .* sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}

        return size, rate, start, misses, samples

    def _parse_primaries(self, log):
        if search(r'(?:panicked|Error|ERROR)', log) is not None:
            raise ParseError('Primary(s) panicked')

        tmp = findall(r'\[?(\d{4}-\S+Z) .* Created B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        # Prefer the time when the carrying leader first completed its commit
        # rule. Legacy logs without this field remain parseable.
        tmp = findall(
            r'Committed B\d+\([^ ]+\) -> ([^ ]+=) @ (\d+)', log
        )
        tmp = [(d, int(t) / 1_000) for d, t in tmp]
        if not tmp:
            tmp = findall(r'\[?(\d{4}-\S+Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
            tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])

        # Ordered commit time: the predecessor constraint has been resolved
        # and the batch has entered the final commit sequence. Legacy logs use
        # their Committed log timestamp as the closest available equivalent.
        tmp = findall(
            r'Committed B\d+\([^ ]+\) -> ([^ ]+=) @ \d+ commit (\d+)',
            log,
        )
        tmp = [(d, int(t) / 1_000) for d, t in tmp]
        if not tmp:
            tmp = findall(
                r'\[?(\d{4}-\S+Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log
            )
            tmp = [(d, self._to_posix(t)) for t, d in tmp]
        final_commits = self._merge_results([tmp])

        tmp = findall(r'\[?(\d{4}-\S+Z) .* Header created round \d+ digest (\S+)', log)
        header_proposals = self._merge_results([[(d, self._to_posix(t)) for t, d in tmp]])
        tmp = findall(r'\[?(\d{4}-\S+Z) .* Header committed round \d+ digest (\S+) leader (true|false)', log)
        header_commits = {d: (self._to_posix(t), leader == 'true') for t, d, leader in tmp}
        outcomes = findall(
            r'Leader outcome round (\d+) digest (\S+) rule ([123]) '
            r'outcome (commit|skip) blocks (\d+) ready_at (\d+) '
            r'commit_at (\d+) headers (\S+)',
            log,
        )
        if outcomes:
            leader_ready = {
                digest: int(ready_at) / 1_000
                for _, digest, _, outcome, _, ready_at, _, _ in outcomes
                if outcome == 'commit' and digest != '-' and int(ready_at) > 0
            }
            ordered = []
            committed_headers = {}
            for _, _, _, outcome, _, _, commit_at, headers in outcomes:
                if outcome != 'commit' or headers == '-':
                    continue
                for item in headers.split(','):
                    digest, ordered_at, is_leader = item.rsplit('@', 2)
                    committed = int(commit_at) / 1_000
                    previous = committed_headers.get(digest)
                    value = (committed, is_leader == '1')
                    if previous is None or committed < previous[0]:
                        committed_headers[digest] = value
                    if is_leader == '0':
                        ordered.append((digest, int(ordered_at) / 1_000))
            header_commits = committed_headers
            rule_orders = self._merge_results([ordered])
            commit_rules = {
                int(round): (int(rule), outcome, int(blocks))
                for round, _, rule, outcome, blocks, _, _, _ in outcomes
            }
        else:
            # Preserve compatibility with logs generated before leader
            # summaries combined rule outcomes and ordering samples.
            tmp = findall(r'Leader commit-ready round \d+ digest (\S+) at (\d+)', log)
            leader_ready = {d: int(t) / 1_000 for d, t in tmp}
            tmp = findall(r'\[?(\d{4}-\S+Z) .* Header rule-ordered round \d+ digest (\S+)', log)
            rule_orders = self._merge_results(
                [[(d, self._to_posix(t)) for t, d in tmp]]
            )
            tmp = findall(
                r'Commit rule stats leader (\S+) rule ([123]) '
                r'outcome (commit|skip) blocks (\d+)',
                log,
            )
            commit_rules = {
                leader: (int(rule), outcome, int(blocks))
                for leader, rule, outcome, blocks in tmp
            }
        aba_durations = [int(value) for value in findall(r'ABA duration round \d+ ms (\d+)', log)]

        configs = {
            'header_size': int(
                search(r'Header size .* (\d+)', log).group(1)
            ),
            'max_header_delay': int(
                search(r'Max header delay .* (\d+)', log).group(1)
            ),
            'gc_depth': int(
                search(r'Garbage collection depth .* (\d+)', log).group(1)
            ),
            'sync_retry_delay': int(
                search(r'Sync retry delay .* (\d+)', log).group(1)
            ),
            'sync_retry_nodes': int(
                search(r'Sync retry nodes .* (\d+)', log).group(1)
            ),
            'batch_size': int(
                search(r'Batch size .* (\d+)', log).group(1)
            ),
            'max_batch_delay': int(
                search(r'Max batch delay .* (\d+)', log).group(1)
            ),
        }

        ip = search(r'booted on (?:/ip4/)?(\d+(?:\.\d+){3})', log).group(1)
        
        return proposals, commits, final_commits, header_proposals, header_commits, leader_ready, rule_orders, commit_rules, aba_durations, configs, ip

    def _parse_workers(self, log):
        if search(r'(?:panic|Error|ERROR)', log) is not None:
            raise ParseError('Worker(s) panicked')

        # Narwhal 3257759 workers print Ok(digest); primaries print digest.
        batch = r'Batch (?:Ok\()?([^()\s]+)\)? contains '
        tmp = findall(batch + r'(\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        tmp = findall(batch + r'sample tx (\d+)', log)
        samples = {int(s): d for d, s in tmp}

        ip = search(r'booted on (?:/ip4/)?(\d+(?:\.\d+){3})', log).group(1)

        return sizes, samples, ip

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _consensus_throughput(self):
        if self.commits.keys() - self.proposals.keys():
            return None, None, None
        return self._throughput_since(self.proposals.values())

    def _throughput_since(self, starts):
        if not self.commits:
            return 0, 0, 0
        if not starts:
            return None, None, None
        duration = max(self.commits.values()) - min(starts)
        if duration <= 0:
            return None, None, None
        if self.commits.keys() - self.sizes.keys():
            return None, None, duration
        bps = sum(self.sizes[digest] for digest in self.commits) / duration
        tps = bps / self.size[0] if len(set(self.size)) == 1 and self.size[0] > 0 else None
        return tps, bps, duration

    def _consensus_latency_samples(self):
        # One proposal-to-rule-ready observation per distinct committed batch.
        if self.commits.keys() - self.proposals.keys():
            return []
        samples = [ready - self.proposals[digest] for digest, ready in self.commits.items()]
        return samples if all(delay >= 0 for delay in samples) else []

    def _consensus_latency(self):
        samples = self._consensus_latency_samples()
        return mean(samples) if samples else 0

    def _final_consensus_latency(self):
        latency = [
            c - self.proposals[d]
            for d, c in self.final_commits.items()
            if d in self.proposals
        ]
        return mean(latency) if latency else 0

    def _end_to_end_throughput(self):
        return self._throughput_since(self.start)

    def _end_to_end_latency(self):
        latency = []
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.commits:
                    assert tx_id in sent  # We receive txs that we sent.
                    start = sent[tx_id]
                    end = self.commits[batch_id]
                    latency += [end-start]
        return mean(latency) if latency else 0

    def _final_end_to_end_latency(self):
        latency = []
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.final_commits:
                    assert tx_id in sent
                    latency += [self.final_commits[batch_id] - sent[tx_id]]
        return mean(latency) if latency else 0

    def comparison_metrics(self, duration):
        """First-replica final commits in [first client Start, Start + duration).

        Unlike the legacy SUMMARY, a commit plateau does not shorten this
        window. Unfinished samples are counted, never assigned zero latency.
        The caller must keep the processes alive for the entire window.
        """
        if not isfinite(duration) or duration <= 0:
            raise ValueError('Comparison duration must be positive and finite')
        if len(set(self.size)) != 1:
            raise ParseError('Comparison requires equal transaction sizes')
        if len(self.sent_samples) != len(self.received_samples):
            raise ParseError('Comparison requires one client log per worker log')
        start = min(self.start)
        end = start + duration
        commits = {d: t for d, t in self.final_commits.items() if start <= t < end}
        if any(d not in self.sizes or d not in self.proposals for d in commits):
            raise ParseError('Final commit is missing its batch size or proposal')
        consensus = [t - self.proposals[d] for d, t in commits.items()]
        sent_count, e2e = 0, []
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, sent_at in sent.items():
                if start <= sent_at < end:
                    sent_count += 1
                    batch = received.get(tx_id)
                    if batch in commits:
                        e2e.append(commits[batch] - sent_at)
        if any(t < 0 for t in consensus + e2e):
            raise ParseError('Negative final latency: check clocks and log completeness')
        e2e.sort()
        completed = len(e2e)
        transactions = sum(self.sizes[d] for d in commits) / self.size[0]
        return {
            'window_seconds': duration,
            'window_start': start,
            'final_batches': len(commits),
            'final_transactions': transactions,
            'final_tps': transactions / duration,
            'final_consensus_mean_ms': mean(consensus) * 1000 if consensus else None,
            'final_e2e_mean_ms': mean(e2e) * 1000 if e2e else None,
            'final_e2e_p95_ms': e2e[ceil(0.95 * completed) - 1] * 1000 if e2e else None,
            'sample_sent': sent_count,
            'sample_completed': completed,
            'sample_unfinished': sent_count - completed,
            'sample_completion_ratio': completed / sent_count if sent_count else None,
        }

    def print_comparison(self, duration, filename='comparison.json', *, verbose=True):
        from json import dump

        if self.measurement_plan is not None:
            metrics = self.clock_metrics()
            with open(filename, 'w', encoding='utf-8') as output:
                dump(metrics, output, indent=2, allow_nan=False)
                output.write('\n')
            if verbose:
                print(format_clock_metrics(metrics))
            return
        metrics = self.comparison_metrics(duration)
        metrics['measurement_stats'] = self.measurement_metrics(duration=duration)
        with open(filename, 'w') as output:
            dump(metrics, output, indent=2, allow_nan=False)
            output.write('\n')
        if not verbose:
            return
        print(format_measurements(metrics['measurement_stats']))
        display = lambda value: f'{value:,.1f}' if value is not None else 'N/A'
        print(
            f' FINAL ORDERED COMPARISON ({duration:g} s, first included replica):\n'
            f" Final TPS: {metrics['final_tps']:,.1f} tx/s\n"
            f" Final consensus mean: {display(metrics['final_consensus_mean_ms'])} ms\n"
            f" Final end-to-end mean: {display(metrics['final_e2e_mean_ms'])} ms\n"
            f" Final end-to-end p95: {display(metrics['final_e2e_p95_ms'])} ms\n"
            f" Samples completed/sent: {metrics['sample_completed']}/{metrics['sample_sent']}\n"
            f" Samples unfinished at cutoff: {metrics['sample_unfinished']}\n"
        )

    def _header_latency_stats(self):
        non_leaders, all_headers, leader_times = [], [], []
        for digest, (committed, is_leader) in self.header_commits.items():
            if digest not in self.header_proposals:
                continue
            latency = committed - self.header_proposals[digest]
            all_headers.append(latency)
            if not is_leader:
                non_leaders.append(latency)
            if is_leader:
                leader_times.append(committed)
        leaders = [
            ready - self.header_proposals[digest]
            for digest, ready in self.leader_ready.items()
            if digest in self.header_proposals
        ]
        rule_order = [t - self.header_proposals[d] for d, t in self.rule_orders.items() if d in self.header_proposals]
        leader_times.sort()
        intervals = [b - a for a, b in zip(leader_times, leader_times[1:])]
        avg = lambda values: mean(values) if values and min(values) >= 0 else None
        return avg(leaders), avg(non_leaders), avg(all_headers), avg(intervals), avg(rule_order)

    def _commit_rule_ratios(self):
        leader_total = len(self.commit_rules)
        block_total = sum(value[2] for value in self.commit_rules.values())
        categories = ((1, None), (2, None), (3, 'commit'), (3, 'skip'))
        leader_ratios, block_ratios = [], []
        for rule, outcome in categories:
            matches = lambda value: value[0] == rule and (outcome is None or value[1] == outcome)
            leaders = sum(matches(value) for value in self.commit_rules.values())
            blocks = sum(value[2] for value in self.commit_rules.values() if matches(value))
            leader_ratios.append(100 * leaders / leader_total if leader_total else None)
            block_ratios.append(100 * blocks / block_total if block_total else None)
        return leader_ratios, block_ratios

    def result(self):
        from benchmark.measurements import _distribution

        def display(value, digits=3):
            return f'{value:,.{digits}f}' if value is not None else 'N/A'

        def ids(values):
            if values is None:
                return 'unknown'
            return ','.join(map(str, values)) or 'none'

        consensus = _distribution(round(seconds * 1_000_000)
                                  for seconds in self._consensus_latency_samples())
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        clock_lines = []
        if self.measurement_plan is None:
            release = self.measurement_metrics()
            e2e_tps, e2e_bps, duration = self._end_to_end_throughput()
            e2e = release['pooled']['release_to_final']
            e2e_mean, e2e_p50, e2e_p99 = e2e['mean_ms'], e2e['p50_ms'], e2e['p99_ms']
        else:
            clock = self.clock_metrics()
            release, commit = clock['release_window'], clock['commit_window']
            e2e_tps, e2e_bps = commit['mean_replica_final_tps'], commit['mean_replica_final_bps']
            duration = commit['window_seconds']
            e2e_mean = commit['mean_replica_release_to_final_ms']
            e2e = commit['pooled_release_to_final']
            e2e_p50 = e2e['p50_ms'] if e2e is not None else None
            e2e_p99 = e2e['p99_ms'] if e2e is not None else None
            plan = clock['plan']
            clock_lines = [
                f" Run ID: {clock['run_id']}",
                f" Clocked measurement window: {duration:.3f} s; "
                f"fixed drain {(plan['drain_end_us'] - plan['end_us']) / 1_000_000:.3f} s",
                f" E2E evidence: {commit['status']} "
                f"({commit['matched_samples']}/{commit['selected_samples']} matched samples)",
            ]

        config = self.configs[0]
        headers = self._header_latency_stats()
        rule_leaders, rule_blocks = self._commit_rule_ratios()
        lines = [
            '', '-----------------------------------------', ' SUMMARY:',
            '-----------------------------------------', ' + CONFIG:',
            f' Faults: {self.faults} node(s)',
            f' Adversary mode: {self.adversary_mode}',
            f" Byzantine authority IDs: {self.byzantine_ids if self.byzantine_ids is not None else 'unknown'}",
            f' Crashed authority IDs: {ids(self.crashed_primary_ids)}',
            f' Committee size: {self.committee_size} node(s)',
            f' Commit replicas included: {len(self.included_primary_ids)} node(s)',
            f' Excluded A1 silent replica IDs: {ids(self.excluded_primary_ids)}',
            f' Worker(s) per node: {self.workers} worker(s)',
            f' Collocate primary and workers: {self.collocate}',
            f' Input rate: {sum(self.rate):,} tx/s',
            f' Transaction size: {self.size[0]:,} B',
            f' Execution time: {duration:g} s' if duration is not None else ' Execution time: N/A s',
            *clock_lines, '',
            f" Header size: {config['header_size']:,} B",
            f" Max header delay: {config['max_header_delay']:,} ms",
            f" GC depth: {config['gc_depth']:,} round(s)",
            f" Sync retry delay: {config['sync_retry_delay']:,} ms",
            f" Sync retry nodes: {config['sync_retry_nodes']:,} node(s)",
            f" batch size: {config['batch_size']:,} B",
            f" Max batch delay: {config['max_batch_delay']:,} ms",
            '', ' + RESULTS:',
            f' Consensus TPS: {display(consensus_tps)} tx/s',
            f' Consensus BPS: {display(consensus_bps)} B/s',
            f" Consensus latency: {display(consensus['mean_ms'])} ms",
            f" Consensus latency P50: {display(consensus['p50_ms'])} ms",
            f" Consensus latency P99: {display(consensus['p99_ms'])} ms",
        ]
        for label, seconds in zip(('Leader commit latency', 'Non-leader commit latency',
                                   'All committed headers latency', 'Leader commit interval',
                                   'Non-leader rule-order latency'), headers):
            lines.append(f' {label}: {display(seconds * 1000 if seconds is not None else None)} ms')
        for label, percentage in zip(('Rule 1 leader', 'Rule 2 leader', 'Rule 3 commit leader',
                                      'Rule 3 skip leader'), rule_leaders):
            lines.append(f' {label} ratio: {display(percentage, 2)}%')
        for rule, percentage in enumerate(rule_blocks[:3], start=1):
            lines.append(f' Rule {rule} block ratio: {display(percentage, 2)}%')
        for label, reducer in (('average', mean), ('maximum', max), ('minimum', min)):
            duration_ms = reducer(self.aba_durations) if self.aba_durations else None
            lines.append(f' ABA {label} duration: {display(duration_ms, 0)} ms')
        traffic = release['aba_committee']
        for label, prefix in (('ABA mean send rate per committee node', 'mean'),
                              ('ABA total send rate', 'total')):
            sent = traffic[f'{prefix}_sent_bytes_per_second']
            equivalent = traffic[f'{prefix}_equivalent_tx_per_second']
            lines.append(f' {label}: {display(sent)} B/s ({display(equivalent)} tx/s equivalent)')
        lines.extend([
            '',
            f' End-to-end TPS: {display(e2e_tps)} tx/s',
            f' End-to-end BPS: {display(e2e_bps)} B/s',
            f' End-to-end latency: {display(e2e_mean)} ms',
            f' End-to-end latency P50: {display(e2e_p50)} ms',
            f' End-to-end latency P99: {display(e2e_p99)} ms',
            '-----------------------------------------', '',
        ])
        return '\n'.join(lines)

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())
        # Append one machine-readable record per run; existing runners
        # can keep their filenames and repeated-run behavior.
        from json import dump
        with open(filename + '.metrics.jsonl', 'a', encoding='utf-8') as output:
            metrics = self.clock_metrics() if self.measurement_plan is not None else self.measurement_metrics()
            dump(metrics, output, allow_nan=False)
            output.write('\n')

    @classmethod
    def process(cls, directory, faults=0, adversary=None, committee_size=None):
        assert isinstance(directory, str)

        plan_path = Path(directory) / 'measurement-window.json'
        plan = None
        if plan_path.exists():
            try:
                plan = validate_clock_plan(json.loads(plan_path.read_text(encoding='utf-8')))
            except (OSError, ValueError, TypeError) as error:
                raise ParseError(f'Invalid measurement window: {error}') from error
        primary_files = sorted(glob(join(directory, 'primary-*.log')))
        primary_ids = []
        for filename in primary_files:
            match = search(r'^primary-(\d+)\.log$', Path(filename).name)
            if match is None:
                raise ParseError(f'Invalid primary log filename: {filename}')
            primary_ids.append(int(match.group(1)))
        population_path = Path(directory) / 'measurement-population.json'
        excluded = []
        mode_options = adversary
        try:
            crash = crash_population(directory, primary_ids, adversary, committee_size)
        except (OSError, ValueError, TypeError, KeyError) as error:
            raise ParseError(f'Invalid crash population: {error}') from error
        if crash is not None:
            mode_options = dict(mode='silent', byzantine_ids=crash['crashed_primary_ids'])
            faults = len(crash['crashed_primary_ids'])
        elif adversary is not None:
            if str(adversary.get('mode', '')).lower() in ('silent', 'a1'):
                excluded = adversary['byzantine_ids']
        elif population_path.exists():
            try:
                population = json.loads(population_path.read_text(encoding='utf-8'))
                if (population['schema_version'] != 1
                        or sorted(population['primary_ids']) != sorted(primary_ids)):
                    raise ValueError('Saved population does not match the primary logs')
                excluded = population['excluded_primary_ids']
                if not isinstance(excluded, list):
                    raise ValueError('Saved exclusions must be a list of launch IDs')
                mode_options = population.get('adversary')
                if mode_options is None and excluded:
                    mode_options = dict(mode='silent', byzantine_ids=excluded)
                if faults == '?' and sorted(primary_ids) == list(range(len(primary_ids))):
                    # A v1 manifest was written only for explicit runtime
                    # modes. Orca required faults=0 and launched all primaries.
                    faults = population.get('faults', 0)
            except (OSError, ValueError, TypeError, KeyError) as error:
                raise ParseError(f'Invalid measurement population: {error}') from error

        mode, ids = None, None
        if mode_options is not None:
            if not isinstance(mode_options, dict):
                raise ParseError('Invalid adversary configuration')
            mode = MODES.get(str(mode_options.get('mode', '')).lower())
            ids = mode_options.get('byzantine_ids')
            launch_ids = set(primary_ids) if crash is None else set(range(crash['committee_size']))
            if (mode is None or not isinstance(ids, list)
                    or any(type(i) is not int or i not in launch_ids for i in ids)
                    or len(set(ids)) != len(ids) or (mode == 'none' and ids)):
                raise ParseError('Invalid adversary mode or Byzantine launch IDs')
            if crash is None and set(excluded) != (set(ids) if mode == 'silent' else set()):
                raise ParseError('Adversary does not match the saved replica exclusions')
            ids = sorted(ids)

        client_files = sorted(glob(join(directory, 'client-*.log')))
        worker_files = sorted(glob(join(directory, 'worker-*.log')))
        if [x.rsplit('client-', 1)[1] for x in client_files] != [
            x.rsplit('worker-', 1)[1] for x in worker_files
        ]:
            raise ParseError('Client and worker log identities do not match')

        clients = []
        for filename in client_files:
            with open(filename, 'r') as f:
                clients += [f.read()]
        primaries = []
        for filename in primary_files:
            with open(filename, 'r') as f:
                primaries += [f.read()]
        workers = []
        for filename in worker_files:
            with open(filename, 'r') as f:
                workers += [f.read()]

        if plan is not None and not all((clients, primaries, workers)):
            raise ParseError('Missing client, primary, or worker logs for the clocked measurement')
        if plan is None and any('Benchmark clock ' in log for log in clients):
            raise ParseError('Clocked client logs require measurement-window.json')
        result = cls(clients, primaries, workers, faults=faults,
                     primary_ids=primary_ids, excluded_primary_ids=excluded)
        if mode is None and population_path.exists():
            mode, ids = _logged_runtime_adversary(primaries)
        if mode is not None:
            label = {'none': 'A0', 'silent': 'A1', 'targeted': 'A3', 'network': 'A5'}[mode]
            result.adversary_mode = f'{label} ({mode})'
            result.byzantine_ids = ids
        result.measurements.source_logs = [
            {'client': c, 'worker': w} for c, w in zip(client_files, worker_files)
        ]
        result.measurements.replica_logs = [path for ident, path in zip(primary_ids, primary_files)
                                           if ident not in result.excluded_primary_ids]
        result.measurement_plan = plan
        if crash is not None:
            result.crashed_primary_ids = crash['crashed_primary_ids']
            result.committee_size = crash['committee_size']
        if plan is not None:
            result.clock_metrics()  # Reject missing or inconsistent run evidence before reporting.
        if adversary is not None:
            population = crash or dict(schema_version=1, primary_ids=primary_ids,
                                       excluded_primary_ids=result.excluded_primary_ids, faults=faults,
                                       adversary=dict(mode=mode, byzantine_ids=ids))
            temporary = population_path.with_suffix('.json.tmp')
            temporary.write_text(json.dumps(population, indent=2) + '\n', encoding='utf-8')
            temporary.replace(population_path)
        return result
