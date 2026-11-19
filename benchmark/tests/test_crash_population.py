"""A1 archives contain only surviving authorities, including all their sources."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from benchmark.logs import LogParser, ParseError
from test_silent_statistics import write_logs


class CrashPopulationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        write_logs(self.root, [4] * 10)
        for ident in (7, 8, 9):
            for name in (f'primary-{ident}.log', f'worker-{ident}-0.log', f'client-{ident}-0.log'):
                (self.root / name).unlink()
        self.options = dict(mode='silent', fault_model='crash-stop', byzantine_ids=[7, 8, 9])
        pool = patch('benchmark.logs.Pool')
        fake = pool.start()
        self.addCleanup(pool.stop)
        fake.return_value.__enter__.return_value.map.side_effect = lambda f, values: list(map(f, values))

    def parse(self):
        return LogParser.process(str(self.root), adversary=self.options, committee_size=10)

    def test_only_survivors_contribute_and_reparse_preserves_original_committee(self):
        self.parse()
        parser = LogParser.process(str(self.root))
        metrics = parser.measurement_metrics(duration=60)
        self.assertEqual(parser.committee_size, 10)
        self.assertEqual(parser.faults, 3)
        self.assertEqual(metrics['crashed_primary_ids'], [7, 8, 9])
        self.assertEqual(metrics['source_pairs'], 7)
        self.assertEqual(metrics['pooled']['release_to_final']['count'], 49)
        self.assertEqual(metrics['pooled']['release_to_final']['mean_ms'], 3500)
        self.assertEqual(sum(parser.rate), 7000)

    def test_missing_surviving_primary_is_an_error(self):
        (self.root / 'primary-6.log').unlink()
        with self.assertRaises(ParseError):
            self.parse()

    def test_missing_client_and_worker_pair_is_an_error(self):
        (self.root / 'worker-6-0.log').unlink()
        (self.root / 'client-6-0.log').unlink()
        with self.assertRaises(ParseError):
            self.parse()


if __name__ == '__main__':
    unittest.main()
