"""Verify the native terminal summary with complete log records."""
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from benchmark.logs import LogParser
from test_clock_measurements import EPOCH_US, PLAN, client, line, primary, worker
from test_silent_statistics import write_logs


class SummaryOutputTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        pool = patch('benchmark.logs.Pool')
        self.addCleanup(pool.stop)
        pool.start().return_value.__enter__.return_value.map.side_effect = (
            lambda function, values: list(map(function, values)))

    def clock_logs(self, *, include_proposals=True):
        (self.root / 'measurement-window.json').write_text(json.dumps(PLAN))
        (self.root / 'client-0-0.log').write_text(client())
        (self.root / 'worker-0-0.log').write_text(worker())
        for ident in range(2):
            log = primary({'batchA=': (4 + ident) * 1_000_000,
                           'batchB=': (6 + ident) * 1_000_000})
            if include_proposals and ident == 0:
                for batch in ('batchA=', 'batchB='):
                    log += line(EPOCH_US + 500_000, f'Created B1(author) -> {batch}')
            (self.root / f'primary-{ident}.log').write_text(log)

    def summary(self):
        return LogParser.process(str(self.root), adversary={
            'mode': 'none', 'byzantine_ids': []}).result()

    def check_comparison_output(self):
        parser = LogParser.process(str(self.root))
        output = self.root / 'comparison.json'
        with redirect_stdout(io.StringIO()) as verbose:
            parser.print_comparison(6, str(output))
        self.assertTrue(verbose.getvalue().strip())
        saved = output.read_bytes()
        output.unlink()
        with redirect_stdout(io.StringIO()) as quiet:
            parser.print_comparison(6, str(output), verbose=False)
        self.assertEqual(quiet.getvalue(), '')
        self.assertEqual(output.read_bytes(), saved)

    def test_quiet_clocked_comparison_keeps_the_detailed_json(self):
        self.clock_logs()
        self.check_comparison_output()

    def test_quiet_legacy_comparison_keeps_the_detailed_json(self):
        write_logs(self.root, [1, 4])
        self.check_comparison_output()

    def test_clocked_summary_reports_rule_ready_and_final_commit_percentiles(self):
        self.clock_logs()
        report = self.summary()
        for expected in (' + CONFIG:', 'Adversary mode: A0 (none)', 'Byzantine authority IDs: []',
                         'Execution time: 6 s', 'Header size: 1,000 B',
                         ' + RESULTS:', 'Consensus latency: 4,500.000 ms',
                         'Consensus latency P50: 3,500.000 ms',
                         'Consensus latency P99: 5,500.000 ms',
                         'Rule 3 skip leader ratio:', 'End-to-end TPS: 0.500 tx/s',
                         'End-to-end latency: 3,500.000 ms',
                         'End-to-end latency P50: 3,000.000 ms',
                         'End-to-end latency P99: 4,000.000 ms'):
            self.assertIn(expected, report)
        self.assertNotIn('Final-window replica', report)
        self.assertNotIn('Release -> ingress', report)

    def test_replica_without_selected_samples_preserves_pooled_e2e_percentiles(self):
        self.clock_logs()
        (self.root / 'primary-1.log').write_text(primary({}))
        report = self.summary()
        self.assertIn('End-to-end latency: N/A ms', report)
        self.assertIn('End-to-end latency P50: 3,000.000 ms', report)
        self.assertIn('End-to-end latency P99: 3,000.000 ms', report)

    def test_missing_proposals_preserve_e2e_and_leave_consensus_unavailable(self):
        self.clock_logs(include_proposals=False)
        report = self.summary()
        self.assertIn('Consensus latency P99: N/A ms', report)
        self.assertIn('End-to-end latency P99: 4,000.000 ms', report)

    def test_a3_reports_noncontiguous_byzantine_authority_ids(self):
        write_logs(self.root, [1, 4, 2, 3])
        report = LogParser.process(str(self.root), adversary={
            'mode': 'targeted', 'byzantine_ids': [2, 0]}).result()
        self.assertIn('Adversary mode: A3 (targeted)', report)
        self.assertIn('Byzantine authority IDs: [0, 2]', report)

    def test_legacy_summary_uses_release_to_final_samples(self):
        write_logs(self.root, [1, 4])
        report = self.summary()
        self.assertIn('End-to-end latency P50: 500.000 ms', report)
        self.assertIn('End-to-end latency P99: 3,500.000 ms', report)

    def test_skip_is_a_leader_outcome_and_does_not_add_committed_blocks(self):
        write_logs(self.root, [1, 4])
        with (self.root / 'primary-0.log').open('a') as out:
            for idx, (rule, outcome, blocks) in enumerate((
                    (1, 'commit', 3), (2, 'commit', 1), (3, 'skip', 0))):
                out.write(f'Commit rule stats leader L{idx} rule {rule} '
                          f'outcome {outcome} blocks {blocks}\n')
        report = self.summary()
        for expected in ('Rule 1 leader ratio: 33.33%', 'Rule 2 leader ratio: 33.33%',
                         'Rule 3 commit leader ratio: 0.00%', 'Rule 3 skip leader ratio: 33.33%',
                         'Rule 1 block ratio: 75.00%', 'Rule 2 block ratio: 25.00%',
                         'Rule 3 block ratio: 0.00%'):
            self.assertIn(expected, report)

    def test_aba_sent_bytes_and_transaction_equivalents_use_the_run_tx_size(self):
        self.clock_logs()
        for ident in range(2):
            with (self.root / f'primary-{ident}.log').open('a') as out:
                for seconds, sent in ((3, 100), (5, 100 + 400 * (ident + 1))):
                    at = EPOCH_US + seconds * 1_000_000
                    out.write(line(at, f'Benchmark ABA bytes at_us {at} '
                                   f'elapsed_us {seconds * 1_000_000} sent {sent} received 9999'))
                out.write(line(EPOCH_US + 4_000_000,
                               f'ABA duration round 1 ms {100 + 200 * ident}'))
        report = self.summary()
        for expected in ('ABA average duration: 200 ms', 'ABA maximum duration: 300 ms',
                         'ABA minimum duration: 100 ms',
                         'ABA mean send rate per committee node: 300.000 B/s (3.000 tx/s equivalent)',
                         'ABA total send rate: 600.000 B/s (6.000 tx/s equivalent)'):
            self.assertIn(expected, report)

    def test_missing_counters_and_duration_are_unavailable(self):
        self.clock_logs()
        report = self.summary()
        self.assertIn('ABA average duration: N/A ms', report)
        self.assertIn('ABA total send rate: N/A B/s (N/A tx/s equivalent)', report)
