# Copyright(C) Facebook, Inc. and its affiliates.
from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean

from benchmark.utils import Print


class ParseError(Exception):
    pass


class LogParser:
    def __init__(self, clients, primaries, workers, faults=0):
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

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
        proposals, commits, final_commits, header_proposals, header_commits, rule_orders, commit_rules, aba_durations, self.configs, primary_ips = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.commits = self._merge_results([x.items() for x in commits])
        self.final_commits = self._merge_results(
            [x.items() for x in final_commits]
        )
        self.header_proposals = self._merge_results([x.items() for x in header_proposals])
        self.header_commits = self._merge_tagged_results(header_commits)
        self.rule_orders = self._merge_results([x.items() for x in rule_orders])
        self.commit_rules = self._merge_commit_rules(commit_rules)
        # Keep every completed node-instance sample. Unfinished ABA instances
        # have no duration log and are intentionally excluded.
        self.aba_durations = [duration for values in aba_durations for duration in values]

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

        # Determine whether the primary and the workers are collocated.
        self.collocate = set(primary_ips) == set(workers_ips)

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

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
                merged.setdefault(leader, value)
        return merged

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[(.*Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}

        return size, rate, start, misses, samples

    def _parse_primaries(self, log):
        if search(r'(?:panicked|Error)', log) is not None:
            raise ParseError('Primary(s) panicked')

        tmp = findall(r'\[(.*Z) .* Created B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        # Prefer the time when the carrying leader first completed its commit
        # rule. Legacy logs without this field remain parseable.
        tmp = findall(
            r'Committed B\d+\([^ ]+\) -> ([^ ]+=) @ (\d+)', log
        )
        tmp = [(d, int(t) / 1_000) for d, t in tmp]
        if not tmp:
            tmp = findall(r'\[(.*Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
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
                r'\[(.*Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log
            )
            tmp = [(d, self._to_posix(t)) for t, d in tmp]
        final_commits = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Header created round \d+ digest (\S+)', log)
        header_proposals = self._merge_results([[(d, self._to_posix(t)) for t, d in tmp]])
        tmp = findall(r'\[(.*Z) .* Header committed round \d+ digest (\S+) leader (true|false)', log)
        header_commits = {d: (self._to_posix(t), leader == 'true') for t, d, leader in tmp}
        tmp = findall(r'\[(.*Z) .* Header rule-ordered round \d+ digest (\S+)', log)
        rule_orders = self._merge_results([[(d, self._to_posix(t)) for t, d in tmp]])
        tmp = findall(r'Commit rule stats leader (\S+) rule ([123]) outcome (commit|skip) blocks (\d+)', log)
        commit_rules = {leader: (int(rule), outcome, int(blocks)) for leader, rule, outcome, blocks in tmp}
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

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)
        
        return proposals, commits, final_commits, header_proposals, header_commits, rule_orders, commit_rules, aba_durations, configs, ip

    def _parse_workers(self, log):
        if search(r'(?:panic|Error)', log) is not None:
            raise ParseError('Worker(s) panicked')

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {int(s): d for d, s in tmp}

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        return sizes, samples, ip

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        latency = [c - self.proposals[d] for d, c in self.commits.items()]
        return mean(latency) if latency else 0

    def _final_consensus_latency(self):
        latency = [
            c - self.proposals[d]
            for d, c in self.final_commits.items()
            if d in self.proposals
        ]
        return mean(latency) if latency else 0

    def _end_to_end_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.start), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

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

    def _header_latency_stats(self):
        leaders, non_leaders, leader_times = [], [], []
        for digest, (committed, is_leader) in self.header_commits.items():
            if digest not in self.header_proposals:
                continue
            (leaders if is_leader else non_leaders).append(committed - self.header_proposals[digest])
            if is_leader:
                leader_times.append(committed)
        rule_order = [t - self.header_proposals[d] for d, t in self.rule_orders.items() if d in self.header_proposals]
        leader_times.sort()
        intervals = [b - a for a, b in zip(leader_times, leader_times[1:])]
        avg = lambda values: mean(values) if values else 0
        return avg(leaders), avg(non_leaders), avg(leaders + non_leaders), avg(intervals), avg(rule_order)

    def _commit_rule_ratios(self):
        leader_total = len(self.commit_rules)
        block_total = sum(value[2] for value in self.commit_rules.values())
        categories = ((1, None), (2, None), (3, 'commit'), (3, 'skip'))
        leader_ratios, block_ratios = [], []
        for rule, outcome in categories:
            matches = lambda value: value[0] == rule and (outcome is None or value[1] == outcome)
            leaders = sum(matches(value) for value in self.commit_rules.values())
            blocks = sum(value[2] for value in self.commit_rules.values() if matches(value))
            leader_ratios.append(100 * leaders / leader_total if leader_total else 0)
            block_ratios.append(100 * blocks / block_total if block_total else 0)
        return leader_ratios, block_ratios

    def result(self):
        header_size = self.configs[0]['header_size']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']

        consensus_latency = self._consensus_latency() * 1_000
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1_000
        leader_latency, non_leader_latency, all_header_latency, leader_interval, rule_order_latency = (x * 1_000 for x in self._header_latency_stats())
        rule_leaders, rule_blocks = self._commit_rule_ratios()
        aba_average = mean(self.aba_durations) if self.aba_durations else 0
        aba_maximum = max(self.aba_durations) if self.aba_durations else 0
        aba_minimum = min(self.aba_durations) if self.aba_durations else 0

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {self.faults} node(s)\n'
            f' Committee size: {self.committee_size} node(s)\n'
            f' Worker(s) per node: {self.workers} worker(s)\n'
            f' Collocate primary and workers: {self.collocate}\n'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            f' Header size: {header_size:,} B\n'
            f' Max header delay: {max_header_delay:,} ms\n'
            f' GC depth: {gc_depth:,} round(s)\n'
            f' Sync retry delay: {sync_retry_delay:,} ms\n'
            f' Sync retry nodes: {sync_retry_nodes:,} node(s)\n'
            f' batch size: {batch_size:,} B\n'
            f' Max batch delay: {max_batch_delay:,} ms\n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            f' Leader commit latency: {round(leader_latency):,} ms\n'
            f' Non-leader commit latency: {round(non_leader_latency):,} ms\n'
            f' All committed headers latency: {round(all_header_latency):,} ms\n'
            f' Leader commit interval: {round(leader_interval):,} ms\n'
            f' Non-leader rule-order latency: {round(rule_order_latency):,} ms\n'
            f' Rule 1 leader ratio: {rule_leaders[0]:.2f}%\n'
            f' Rule 2 leader ratio: {rule_leaders[1]:.2f}%\n'
            f' Rule 3 commit leader ratio: {rule_leaders[2]:.2f}%\n'
            f' Rule 3 skip leader ratio: {rule_leaders[3]:.2f}%\n'
            f' Rule 1 block ratio: {rule_blocks[0]:.2f}%\n'
            f' Rule 2 block ratio: {rule_blocks[1]:.2f}%\n'
            f' Rule 3 block ratio: {rule_blocks[2]:.2f}%\n'
            f' ABA average duration: {round(aba_average):,} ms\n'
            f' ABA maximum duration: {round(aba_maximum):,} ms\n'
            f' ABA minimum duration: {round(aba_minimum):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    @classmethod
    def process(cls, directory, faults=0):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        primaries = []
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            with open(filename, 'r') as f:
                primaries += [f.read()]
        workers = []
        for filename in sorted(glob(join(directory, 'worker-*.log'))):
            with open(filename, 'r') as f:
                workers += [f.read()]

        return cls(clients, primaries, workers, faults=faults)
