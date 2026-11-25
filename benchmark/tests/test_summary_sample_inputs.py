import unittest

from benchmark.logs import LogParser


class NativeSummaryInputsTests(unittest.TestCase):
    def setUp(self):
        self.parser = LogParser.__new__(LogParser)
        self.parser.proposals = {'a': 1.0, 'b': 2.0}
        self.parser.commits = {'a': 1.0, 'b': 4.0}
        self.parser.sizes = {'a': 100, 'b': 200}
        self.parser.size = (100,)
        self.parser.start = (0.0,)

    def test_consensus_samples_have_one_entry_per_committed_batch(self):
        self.assertEqual(self.parser._consensus_latency_samples(), [0.0, 2.0])
        self.assertEqual(self.parser._consensus_throughput(), (1.0, 100.0, 3.0))

    def test_partial_pairing_does_not_report_a_complete_distribution(self):
        del self.parser.proposals['a']
        self.assertEqual(self.parser._consensus_latency_samples(), [])
        self.assertIsNone(self.parser._consensus_throughput()[0])
        self.assertEqual(self.parser._end_to_end_throughput(), (0.75, 75.0, 4.0))

    def test_commit_before_proposal_invalidates_consensus_latency(self):
        self.parser.commits['a'] = 0.9
        self.assertEqual(self.parser._consensus_latency_samples(), [])

    def test_missing_batch_size_does_not_count_as_zero_bytes(self):
        del self.parser.sizes['b']
        self.assertEqual(self.parser._consensus_throughput(), (None, None, 3.0))
        self.assertEqual(self.parser._end_to_end_throughput(), (None, None, 4.0))

    def test_zero_duration_or_absent_start_cannot_supply_a_rate(self):
        self.parser.start = (4.0,)
        self.assertIsNone(self.parser._end_to_end_throughput()[0])
        self.parser.proposals.clear()
        self.assertIsNone(self.parser._consensus_throughput()[0])

    def test_no_commits_is_zero_throughput_without_latency(self):
        self.parser.commits.clear()
        self.assertEqual(self.parser._consensus_throughput()[:2], (0, 0))
        self.assertEqual(self.parser._consensus_latency_samples(), [])
