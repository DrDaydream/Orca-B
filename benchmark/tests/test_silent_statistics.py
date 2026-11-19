"""Silent replicas must not supply commit observations or hide unfinished work."""
from base64 import b64encode
from contextlib import ExitStack, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from benchmark.logs import LogParser, ParseError


EPOCH_US = 1794790800000000


def line(second, message):
    return f'2026-11-16T01:00:{second:06.3f}Z INFO node: {message}\n'


def write_logs(root, finals):
    count = len(finals)
    for ident, final in enumerate(finals):
        client = ''.join(line(0, text) for text in (
            'Transactions size: 512 B', 'Transactions rate: 1000 tx/s',
            'Start sending transactions', 'Benchmark latency instrumentation version 1'))
        client += line(.5, f'Benchmark sample released id 0 at_us {EPOCH_US + 500000}')
        client += line(.5, 'Sending sample transaction 0')
        worker = line(0, 'Worker booted on 127.0.0.1:3001')
        worker += line(.6, f'Benchmark sample ingress id 0 at_us {EPOCH_US + 600000}')
        worker += line(1, f'Batch batch{ident}= contains 512 B')
        worker += line(1, f'Batch batch{ident}= contains sample tx 0')
        primary = ''.join(line(0, text) for text in (
            'Header size set to 1000 B', 'Max header delay set to 200 ms',
            'Garbage collection depth set to 50', 'Sync retry delay set to 10000 ms',
            'Sync retry nodes set to 3', 'Batch size set to 500000 B',
            'Max batch delay set to 200 ms', 'Primary booted on 127.0.0.1:3000'))
        primary += line(.8, f'Created B1(author{ident}) -> batch{ident}=')
        if final is not None:
            for batch in range(count):
                stamp = EPOCH_US + int(final * 1000000)
                primary += line(final, f'Committed B1(author{batch}) -> batch{batch}= '
                                f'@ {stamp // 1000} commit {stamp // 1000} commit_us {stamp}')
        (root / f'client-{ident}-0.log').write_text(client)
        (root / f'worker-{ident}-0.log').write_text(worker)
        (root / f'primary-{ident}.log').write_text(primary)


class SilentStatisticsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        # Only avoid subprocess creation; every parser and aggregation is real.
        pool = patch('benchmark.logs.Pool')
        mock = pool.start()
        self.addCleanup(pool.stop)
        mock.return_value.__enter__.return_value.map.side_effect = (
            lambda function, values: list(map(function, values)))

    def parse(self, mode='silent', ids=(0, 1, 2)):
        return LogParser.process(str(self.root), faults=0,
                                 adversary={'mode': mode, 'byzantine_ids': list(ids)})

    def test_ten_nodes_exclude_silent_commits_from_both_latency_statistics(self):
        write_logs(self.root, [1, 1, 1, 4, 4, 4, 4, 4, 4, 4])
        parser = self.parse()
        metrics = parser.measurement_metrics(duration=100)
        self.assertEqual(metrics['pooled']['release_to_final']['mean_ms'], 3500)
        self.assertEqual(metrics['pooled']['release_to_final']['count'], 70)
        self.assertEqual(parser.comparison_metrics(100)['final_e2e_mean_ms'], 3500)
        self.assertEqual(metrics['excluded_primary_ids'], [0, 1, 2])
        self.assertEqual([r['replica'] for r in metrics['replicas']], list(range(3, 10)))
        self.assertEqual(metrics['source_pairs'], 10)
        self.assertEqual(parser.committee_size, 10)
        self.assertEqual(parser.workers, 1)
        self.assertIn('End-to-end latency: 3,500.000 ms', parser.result())
        self.assertIn('Excluded A1 silent replica IDs: 0,1,2', parser.result())

    def test_saved_population_prevents_silent_replica_from_lowering_latency(self):
        write_logs(self.root, [1, 4, 4, 4])
        (self.root / 'measurement-population.json').write_text(json.dumps({
            'schema_version': 1, 'primary_ids': [0, 1, 2, 3], 'excluded_primary_ids': [0]}))
        parser = LogParser.process(str(self.root), faults=0)
        self.assertEqual(parser.measurement_metrics(100)['pooled']['release_to_final']['mean_ms'], 3500)

    def test_reparse_and_saved_comparison_keep_the_same_population(self):
        write_logs(self.root, [1, 1, 1, 4, 4, 4, 4, 4, 4, 4])
        parser = self.parse()
        again = LogParser.process(str(self.root), faults='?')
        self.assertEqual(again.measurement_metrics()['excluded_primary_ids'], [0, 1, 2])
        with redirect_stdout(io.StringIO()):
            parser.print_comparison(100, str(self.root / 'comparison.json'))
        comparison = json.loads((self.root / 'comparison.json').read_text())
        self.assertEqual(comparison['final_e2e_mean_ms'], 3500)
        self.assertEqual(comparison['measurement_stats']['excluded_primary_ids'], [0, 1, 2])

    def test_only_silent_replicas_commit_does_not_count_as_honest_completion(self):
        write_logs(self.root, [1, None, None, None])
        parser = self.parse(ids=(0,))
        metrics = parser.measurement_metrics(duration=100)
        self.assertIsNone(metrics['pooled']['release_to_final']['mean_ms'])
        for row in metrics['replicas']:
            self.assertEqual(row['completed_samples'], 0)
            self.assertEqual(row['unfinished_samples'], 4)
        comparison = parser.comparison_metrics(100)
        self.assertIsNone(comparison['final_e2e_mean_ms'])
        self.assertEqual(comparison['sample_unfinished'], 4)
        self.assertIn('End-to-end latency: N/A ms', parser.result())

    def test_explicit_ids_use_launch_ids_not_lexicographic_log_positions(self):
        write_logs(self.root, [4, 4, 1, 4, 4, 4, 4, 4, 4, 4, 1, 4])
        metrics = self.parse(ids=(2, 10)).measurement_metrics(duration=100)
        self.assertEqual(metrics['pooled']['release_to_final']['mean_ms'], 3500)
        self.assertEqual({r['replica'] for r in metrics['replicas']}, {0, 1, 3, 4, 5, 6, 7, 8, 9, 11})
        for row in metrics['replicas']:
            self.assertEqual(Path(row['replica_log']).name, f"primary-{row['replica']}.log")

    def test_other_modes_keep_all_replicas(self):
        write_logs(self.root, [1, 4, 4, 4])
        self.parse(ids=(0,))
        for mode in ('none', 'targeted', 'network'):
            with self.subTest(mode=mode):
                metrics = self.parse(mode=mode, ids=() if mode == 'none' else (0,)).measurement_metrics(100)
                self.assertEqual(metrics['pooled']['release_to_final']['mean_ms'], 2750)
                self.assertEqual(len(metrics['replicas']), 4)
                self.assertEqual(metrics['excluded_primary_ids'], [])

    def test_local_a1_crash_runner_passes_survivors_to_real_statistics(self):
        from benchmark.local import LocalBench
        write_logs(self.root, [1, 4, 4, 4])
        for name in ('primary-0.log', 'worker-0-0.log', 'client-0-0.log'):
            (self.root / name).unlink()
        params = dict(nodes=4, workers=1, rate=4000, tx_size=512, duration=100, faults=0,
                      adversary=dict(mode='silent', byzantine_ids=[0]))
        node = dict(header_size=1000, max_header_delay=200, gc_depth=50,
                    sync_retry_delay=10000, sync_retry_nodes=3,
                    batch_size=500000, max_batch_delay=200)
        keys = [SimpleNamespace(name=b64encode(bytes([i]) * 32).decode()) for i in range(4)]
        with ExitStack() as stack:
            for name in ('subprocess.run', 'sleep', 'LocalProcesses',
                         'LocalCommittee.print', 'NodeParameters.print'):
                stack.enter_context(patch('benchmark.local.' + name))
            stack.enter_context(patch('benchmark.local.Key.from_file', side_effect=keys))
            stack.enter_context(patch('benchmark.local.PathMaker.logs_path', return_value=str(self.root)))
            stack.enter_context(patch('benchmark.local.PathMaker.results_path', return_value=str(self.root)))
            parser = LocalBench(params, node).run()
        self.assertEqual(parser.measurement_metrics(100)['pooled']['release_to_final']['mean_ms'], 3500)

    def test_remote_download_uses_the_same_silent_population(self):
        from benchmark.remote import Bench
        write_logs(self.root, [1, 4, 4, 4])
        bench = Bench.__new__(Bench)
        bench.connect = {}
        committee = SimpleNamespace(size=lambda: 4, workers_addresses=lambda _: [[(0, '127.0.0.1:3001')]] * 4,
                                    primary_addresses=lambda _: ['127.0.0.1:3000'] * 4)
        with patch('benchmark.remote.Connection'), patch('benchmark.remote.subprocess.run'), \
             patch('benchmark.remote.PathMaker.logs_path', return_value=str(self.root)):
            parser = bench._logs(committee, 0, adversary=dict(mode='silent', byzantine_ids=[0]))
        self.assertEqual(parser.measurement_metrics(100)['pooled']['release_to_final']['mean_ms'], 3500)

    def test_malformed_or_mismatched_saved_population_is_rejected(self):
        write_logs(self.root, [1, 4, 4, 4])
        for content in ('not-json', json.dumps(dict(schema_version=1, primary_ids=[99], excluded_primary_ids=[0]))):
            (self.root / 'measurement-population.json').write_text(content)
            with self.subTest(content=content), self.assertRaises(ParseError):
                LogParser.process(str(self.root), faults=0)

    def test_invalid_exclusions_fail_instead_of_silently_using_wrong_population(self):
        write_logs(self.root, [1, 4, 4, 4])
        for ids in ((9,), (-1,), (0, 0), (0, 1, 2, 3), (True,)):
            with self.subTest(ids=ids), self.assertRaises(ParseError):
                self.parse(ids=ids)


if __name__ == '__main__':
    unittest.main()
