"""Report the configured adversary consistently on first parse and replay."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from benchmark.logs import LogParser, ParseError
from test_silent_statistics import write_logs


class SummaryModeTests(unittest.TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        write_logs(self.root, [4] * 12)
        self.manifest = self.root / 'measurement-population.json'
        pool = patch('benchmark.logs.Pool')
        fake = pool.start()
        self.addCleanup(pool.stop)
        fake.return_value.__enter__.return_value.map.side_effect = lambda f, xs: list(map(f, xs))

    def test_runtime_modes_and_ids_survive_fab_logs(self):
        for mode, ids, label, expected_ids in (
            ('none', [], 'A0 (none)', []),
            ('silent', [10, 2], 'A1 (silent)', [2, 10]),
            ('targeted', [10, 2], 'A3 (targeted)', [2, 10]),
            ('network', [10, 2], 'A5 (network)', [2, 10]),
            ('network', [], 'A5 (network)', []),
        ):
            with self.subTest(mode=mode, ids=ids):
                live = LogParser.process(str(self.root), faults=0, committee_size=12,
                                         adversary=dict(mode=mode, byzantine_ids=ids))
                replay = LogParser.process(str(self.root), faults='?')
                for parser in (live, replay):
                    self.assertEqual((parser.adversary_mode, parser.byzantine_ids), (label, expected_ids))
                    self.assertEqual((parser.committee_size, parser.workers, parser.faults), (12, 1, 0))

    def test_old_crash_manifest_identifies_nodes_without_logs(self):
        for i in (2, 10):
            for name in (f'primary-{i}.log', f'client-{i}-0.log', f'worker-{i}-0.log'):
                (self.root / name).unlink()
        self.manifest.write_text(json.dumps(dict(schema_version=2, fault_model='crash-stop',
            crashed_primary_ids=[10, 2], committee_size=12, excluded_primary_ids=[],
            primary_ids=[i for i in range(12) if i not in (2, 10)])))
        parser = LogParser.process(str(self.root), faults='?')
        self.assertEqual((parser.adversary_mode, parser.byzantine_ids), ('A1 (silent)', [2, 10]))
        self.assertEqual((parser.committee_size, parser.workers, parser.faults), (12, 1, 2))

    def test_no_evidence_is_unknown_and_legacy_silent_exclusions_are_preserved(self):
        parser = LogParser.process(str(self.root), faults='?')
        self.assertEqual((parser.adversary_mode, parser.byzantine_ids), ('unknown', None))
        self.manifest.write_text(json.dumps(dict(schema_version=1, primary_ids=list(range(12)),
                                                  excluded_primary_ids=[10, 2])))
        parser = LogParser.process(str(self.root), faults='?')
        self.assertEqual((parser.adversary_mode, parser.byzantine_ids), ('A1 (silent)', [2, 10]))

    def test_saved_byzantine_ids_must_match_real_primary_ids(self):
        self.manifest.write_text(json.dumps(dict(schema_version=1, primary_ids=list(range(12)),
            excluded_primary_ids=[], adversary=dict(mode='network', byzantine_ids=[99]))))
        with self.assertRaises(ParseError):
            LogParser.process(str(self.root), faults='?')

    def test_old_runtime_manifest_recovers_mode_without_guessing_member_ids(self):
        self.manifest.write_text(json.dumps(dict(schema_version=1, primary_ids=list(range(12)),
                                                 excluded_primary_ids=[])))
        for name, count, mode, ids in (('None', 0, 'A0 (none)', []),
                                       ('Targeted', 2, 'A3 (targeted)', None),
                                       ('Network', 0, 'A5 (network)', [])):
            with self.subTest(mode=mode):
                write_logs(self.root, [4] * 12)
                for path in self.root.glob('primary-*.log'):
                    with path.open('a') as output:
                        output.write(f'BENCH_ADVERSARY mode={name} action=configure sender=abc '
                                     f'fixed_byzantine={count} seed=1\n')
                parser = LogParser.process(str(self.root), faults='?')
                self.assertEqual((parser.adversary_mode, parser.byzantine_ids), (mode, ids))
                self.assertEqual((parser.committee_size, parser.workers, parser.faults), (12, 1, 0))

    def test_incomplete_old_runtime_population_does_not_imply_a_smaller_committee(self):
        for name in ('primary-2.log', 'client-2-0.log', 'worker-2-0.log'):
            (self.root / name).unlink()
        self.manifest.write_text(json.dumps(dict(schema_version=1,
            primary_ids=[i for i in range(12) if i != 2], excluded_primary_ids=[])))
        parser = LogParser.process(str(self.root), faults='?')
        self.assertEqual(parser.committee_size, '?')
