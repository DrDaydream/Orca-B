"""A1 removes complete authorities while preserving committee and rate sharing."""
import json
from pathlib import Path
import shlex
import tempfile
import unittest

from benchmark import adversary


def parameters(protocol='orca', **values):
    args = dict(nodes=10, workers=1, rate='10000', tx_size=512, duration=60,
                faults=0, runs=1, default_faults=0, protocol=protocol,
                adversary='A1', byzantine=3)
    args.update(values)
    return adversary.remote_parameters(**args)


class CrashAdversaryTests(unittest.TestCase):
    def test_a1_count_uses_last_authorities_like_legacy_bullshark_faults(self):
        for protocol in ('orca', 'bullshark'):
            options = parameters(protocol)['adversary']
            self.assertEqual(options['byzantine_ids'], [7, 8, 9])
            self.assertEqual(options['fault_model'], 'crash-stop')
            self.assertFalse(options['bullshark_alternating'])

    def test_explicit_ids_are_not_renumbered_and_runtime_injection_is_disabled(self):
        options = parameters(byzantine=0, byzantine_ids='1,4,8')['adversary']
        names = [f'key-{i}' for i in range(10)]
        config = adversary.resolve_adversary(options, names)
        self.assertEqual(adversary.active_launch_ids(10, config), [0, 2, 3, 5, 6, 7, 9])
        env = shlex.split(adversary.adversary_environment(config))
        wire = json.loads(next(token.split('=', 1)[1] for token in env
                               if token.startswith('BENCH_ADVERSARY_CONFIG=')))
        self.assertEqual(wire['mode'], 'none')
        self.assertFalse(wire['bullshark_alternating'])

    def test_a3_and_a5_keep_first_ids_and_all_processes(self):
        for mode in ('A3', 'A5'):
            options = parameters(adversary=mode)['adversary']
            self.assertEqual(options['byzantine_ids'], [0, 1, 2])
            self.assertEqual(adversary.active_launch_ids(10, options), list(range(10)))
            self.assertTrue(options['bullshark_alternating'])

    def test_crash_mode_rejects_bounded_round_windows_and_mixed_faults(self):
        for values in (dict(start_round=2), dict(attack_rounds=5), dict(faults=1)):
            for protocol in ('orca', 'bullshark'):
                with self.subTest(values=values, protocol=protocol), self.assertRaises(ValueError):
                    parameters(protocol, **values)

    def test_snapshot_records_active_population_and_reduced_offered_rate(self):
        params = parameters()
        names = [f'key-{i}' for i in range(10)]
        config = adversary.resolve_adversary(params['adversary'], names)
        with tempfile.TemporaryDirectory() as directory:
            path = adversary.write_run_snapshot(directory, params, {}, names, config,
                                                10, 10000, 1, protocol='orca-a')
            data = json.loads(Path(path).read_text())
        self.assertEqual(data['nodes'], 10)
        self.assertEqual(data['active_launch_ids'], list(range(7)))
        self.assertEqual(data['inactive_launch_ids'], [7, 8, 9])
        self.assertEqual(data['offered_rate_tps'], 7000)
        self.assertEqual(data['fault_model'], 'crash-stop')


if __name__ == '__main__':
    unittest.main()
