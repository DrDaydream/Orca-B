import unittest

from benchmark.logs import LogParser, ParseError


class ComparisonMetricsTests(unittest.TestCase):
    def parser(self):
        parser = LogParser.__new__(LogParser)
        parser.start = [100.0]
        parser.size = [512]
        parser.rate = [50000]
        parser.proposals = {'a': 100.5, 'b': 101.0, 'late': 119.0}
        parser.commits = {'a': 101.0, 'b': 102.0, 'late': 119.5}
        parser.final_commits = {'a': 103.0, 'b': 114.0, 'late': 121.0}
        parser.sizes = {'a': 5120, 'b': 5120, 'late': 5120}
        parser.sent_samples = [{0: 100.0, 1: 101.0, 2: 119.0, 3: 119.5}]
        parser.received_samples = [{0: 'a', 1: 'b', 2: 'late'}]
        return parser

    def test_plateau_does_not_shorten_throughput_window(self):
        result = self.parser().comparison_metrics(20)
        self.assertEqual(result['window_seconds'], 20)
        self.assertEqual(result['final_transactions'], 20)
        self.assertEqual(result['final_tps'], 1)
        self.assertEqual(result['final_batches'], 2)

    def test_latency_uses_final_order_and_reports_unfinished_samples(self):
        result = self.parser().comparison_metrics(20)
        self.assertEqual(result['sample_sent'], 4)
        self.assertEqual(result['sample_completed'], 2)
        self.assertEqual(result['sample_unfinished'], 2)
        self.assertEqual(result['sample_completion_ratio'], 0.5)
        self.assertEqual(result['final_e2e_mean_ms'], 8000)
        self.assertEqual(result['final_e2e_p95_ms'], 13000)
        self.assertEqual(result['final_consensus_mean_ms'], 7750)

    def test_no_completions_does_not_report_zero_latency(self):
        parser = self.parser()
        parser.final_commits = {}
        result = parser.comparison_metrics(20)
        self.assertEqual(result['final_tps'], 0)
        self.assertEqual(result['sample_unfinished'], 4)
        self.assertIsNone(result['final_e2e_mean_ms'])
        self.assertIsNone(result['final_e2e_p95_ms'])

    def test_window_is_half_open_and_excludes_later_client_samples(self):
        parser = self.parser()
        parser.final_commits['b'] = 120.0
        parser.sent_samples[0][4] = 120.0
        result = parser.comparison_metrics(20)
        self.assertEqual(result['sample_sent'], 4)
        self.assertEqual(result['sample_completed'], 1)
        self.assertEqual(result['final_transactions'], 10)

    def test_invalid_duration_is_rejected(self):
        for duration in [0, -1, float('nan'), float('inf')]:
            with self.subTest(duration=duration), self.assertRaises(ValueError):
                self.parser().comparison_metrics(duration)

    def test_clock_inconsistency_is_not_a_latency_improvement(self):
        parser = self.parser()
        parser.final_commits['a'] = 100.25
        with self.assertRaises(ParseError):
            parser.comparison_metrics(20)

    def test_missing_metadata_does_not_silently_drop_committed_batches(self):
        for field in ['sizes', 'proposals']:
            parser = self.parser()
            del getattr(parser, field)['a']
            with self.subTest(field=field), self.assertRaises(ParseError):
                parser.comparison_metrics(20)

    def test_native_bullshark_logs_use_actual_ordered_commit(self):
        parser = LogParser.__new__(LogParser)
        client = '''2026-11-16T01:00:00.000Z INFO client: Transactions size: 512 B
2026-11-16T01:00:00.000Z INFO client: Transactions rate: 50000 tx/s
2026-11-16T01:00:00.000Z INFO client: Start sending transactions
2026-11-16T01:00:00.000Z INFO client: Sending sample transaction 0
'''
        primary = '''2026-11-16T01:00:00.000Z INFO node: Header size set to 20 B
2026-11-16T01:00:00.000Z INFO node: Max header delay set to 200 ms
2026-11-16T01:00:00.000Z INFO node: Garbage collection depth set to 50
2026-11-16T01:00:00.000Z INFO node: Sync retry delay set to 10000 ms
2026-11-16T01:00:00.000Z INFO node: Sync retry nodes set to 3
2026-11-16T01:00:00.000Z INFO node: Batch size set to 500000 B
2026-11-16T01:00:00.000Z INFO node: Max batch delay set to 200 ms
2026-11-16T01:00:00.000Z INFO node: Primary booted on /ip4/127.0.0.1/tcp/3000/http
2026-11-16T01:00:01.000Z INFO primary: Created B1(author) -> batch=
2026-11-16T01:00:03.000Z INFO consensus: Committed B1(author) -> batch=
'''
        worker = '''2026-11-16T01:00:00.000Z INFO worker: Worker booted on /ip4/127.0.0.1/tcp/3001/http
2026-11-16T01:00:00.500Z INFO worker: Batch batch= contains 5120 B
2026-11-16T01:00:00.500Z INFO worker: Batch batch= contains sample tx 0
'''
        size, rate, start, _, sent = parser._parse_clients(client)
        proposals, commits, final, *rest = parser._parse_primaries(primary)
        parser.start, parser.size, parser.rate = [start], [size], [rate]
        parser.proposals, parser.commits, parser.final_commits = proposals, commits, final
        # Narwhal 3257759 logs Result<Digest> in workers, but bare digests in primaries.
        for digest in ['batch=', 'Ok(batch=)']:
            with self.subTest(worker_digest=digest):
                sizes, received, worker_ip = parser._parse_workers(
                    worker.replace('Batch batch=', f'Batch {digest}')
                )
                parser.sizes = sizes
                parser.sent_samples, parser.received_samples = [sent], [received]
                result = parser.comparison_metrics(20)
                self.assertEqual(result['final_tps'], 0.5)
                self.assertEqual(result['final_consensus_mean_ms'], 2000)
                self.assertEqual(result['final_e2e_mean_ms'], 3000)
                self.assertEqual(result['sample_completed'], 1)
                self.assertEqual(rest[-1], worker_ip)

    def test_uppercase_runtime_errors_reject_incomplete_measurements(self):
        parser = LogParser.__new__(LogParser)
        for parse in [parser._parse_clients, parser._parse_primaries, parser._parse_workers]:
            with self.subTest(parser=parse.__name__), self.assertRaises(ParseError):
                parse('2026-11-16T01:00:00.000Z ERROR node: storage failed\n')


if __name__ == '__main__':
    unittest.main()
