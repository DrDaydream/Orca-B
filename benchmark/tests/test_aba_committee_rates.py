import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from benchmark.logs import LogParser
from test_silent_statistics import EPOCH_US, line, write_logs


class AbaCommitteeRateTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        pool = patch('benchmark.logs.Pool')
        self.addCleanup(pool.stop)
        pool.start().return_value.__enter__.return_value.map.side_effect = (
            lambda function, values: list(map(function, values)))

    def write_counters(self, node, rate, *, full=True):
        with (self.root / f'primary-{node}.log').open('a') as out:
            for second in ((1, 3) if full else (1,)):
                stamp = EPOCH_US + second * 1_000_000
                out.write(line(second, f'Benchmark ABA bytes at_us {stamp} '
                               f'elapsed_us {second * 1_000_000} sent {second * rate} received 50000'))

    def test_running_silent_authority_counts_in_committee_traffic(self):
        write_logs(self.root, [4] * 4)
        for node in range(4):
            self.write_counters(node, (node + 1) * 512)
        parser = LogParser.process(str(self.root), adversary=dict(mode='silent', byzantine_ids=[3]))
        release = parser.measurement_metrics(duration=4)
        self.assertEqual(release['included_primary_ids'], [0, 1, 2])
        traffic = release.get('aba_committee')
        self.assertIsNotNone(traffic)
        self.assertEqual(traffic['total_sent_bytes_per_second'], 5120)
        self.assertEqual(traffic['mean_sent_bytes_per_second'], 1280)
        self.assertEqual(traffic['total_equivalent_tx_per_second'], 10)
        self.assertEqual(traffic['mean_equivalent_tx_per_second'], 2.5)

    def test_crashed_authority_is_zero_in_the_full_committee_average(self):
        write_logs(self.root, [4] * 3)
        for node in range(3):
            self.write_counters(node, (node + 1) * 512)
        parser = LogParser.process(str(self.root), committee_size=4, adversary=dict(
            mode='silent', fault_model='crash-stop', byzantine_ids=[3]))
        release = parser.measurement_metrics(duration=4)
        self.assertEqual(release['crashed_primary_ids'], [3])
        traffic = release.get('aba_committee')
        self.assertIsNotNone(traffic)
        self.assertEqual(traffic['total_sent_bytes_per_second'], 3072)
        self.assertEqual(traffic['mean_sent_bytes_per_second'], 768)

    def test_missing_second_snapshot_from_running_silent_authority_leaves_rates_unavailable(self):
        write_logs(self.root, [4] * 4)
        for node in range(4):
            self.write_counters(node, 512, full=(node != 3))
        parser = LogParser.process(str(self.root), adversary=dict(mode='silent', byzantine_ids=[3]))
        traffic = parser.measurement_metrics(duration=4).get('aba_committee')
        self.assertIsNotNone(traffic)
        for value in ('total_sent_bytes_per_second', 'mean_sent_bytes_per_second',
                      'total_equivalent_tx_per_second', 'mean_equivalent_tx_per_second'):
            self.assertIsNone(traffic[value])
