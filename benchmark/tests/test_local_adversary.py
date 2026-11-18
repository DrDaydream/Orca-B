"""Exercise local Fabric parsing and the real launch/configuration boundary."""
from base64 import b64encode
from contextlib import ExitStack
import json
from pathlib import Path
import shlex
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from invoke import Context
from invoke.exceptions import Exit
from invoke.parser import Parser, ParserContext
import fabfile
from benchmark.local import LocalBench
from benchmark.utils import BenchError

PROTOCOL = 'orca'
KEYS = [b64encode(bytes([n]) * 32).decode() for n in (9, 3, 6, 1)]


def parse_local(args):
    parser = Parser(contexts=[ParserContext(name='local', args=fabfile.local.get_arguments())])
    return parser.parse_argv(['local'] + args)[0].as_kwargs


def task_parameters(args):
    with patch.object(fabfile, 'LocalBench') as bench:
        fabfile.local(Context(), **parse_local(args))
        return bench.call_args.args


class LocalAdversaryTests(unittest.TestCase):
    def test_cli_accepts_workload_header_and_all_adversary_flags(self):
        params, node = task_parameters([
            '--nodes', '4', '--workers', '2', '--rate', '123', '--tx-size', '256',
            '--duration', '12', '--faults', '0', '--header-size', '20',
            '--adversary', 'A3', '--byzantine-ids', '1', '--seed', '42',
            '--start-round', '5', '--attack-rounds', '8', '--delay-ms', '99',
            '--max-pending-messages', '30', '--max-pending-bytes', '4096',
            '--bullshark-alternating', 'false',
        ])
        self.assertEqual([params[k] for k in ['nodes', 'workers', 'rate', 'tx_size', 'duration']], [4, 2, 123, 256, 12])
        self.assertEqual(node['header_size'], 20)
        self.assertEqual(params['adversary'], dict(mode='targeted', byzantine_ids=[1],
            seed=42, start_round=5, rounds=8, delay_ms=99, max_pending_messages=30,
            max_pending_bytes=4096, bullshark_alternating=False))

    def test_no_arguments_preserve_original_local_defaults(self):
        params, node = task_parameters([])
        self.assertEqual(params, dict(faults=0, nodes=4, workers=1, rate=50000, tx_size=512, duration=20))
        self.assertEqual(node['header_size'], 1000)

    def test_invalid_cli_stops_before_local_process_manager(self):
        cases = [dict(adversary='targeted', byzantine=2), dict(adversary='network', start_round=0),
                 dict(adversary='silent', byzantine_ids='1,1'), dict(rate='100,200'),
                 dict(header_size=0), dict(nodes=1), dict(delay_ms=10)]
        if PROTOCOL == 'orca':
            cases.append(dict(adversary='network', faults=1))
        else:
            cases.append(dict(adversary='silent', byzantine_ids='3', faults=1))
        for values in cases:
            with self.subTest(values=values), patch.object(fabfile, 'LocalBench') as bench:
                with self.assertRaises(Exit):
                    fabfile.local(Context(), **values)
                bench.assert_not_called()

    def test_local_launch_resolves_fresh_keys_and_records_actual_config(self):
        for mode in ['none', 'silent', 'targeted', 'network']:
            flags = ['--adversary', mode, '--rate', '100', '--duration', '1']
            if mode in ['silent', 'targeted']:
                flags += ['--byzantine-ids', '1']
            params, node = task_parameters(flags)
            with self.subTest(mode=mode), TemporaryDirectory() as directory, ExitStack() as stack:
                stack.enter_context(patch('benchmark.local.subprocess.run'))
                stack.enter_context(patch('benchmark.local.sleep'))
                stack.enter_context(patch('benchmark.local.Key.from_file', side_effect=[SimpleNamespace(name=n) for n in KEYS]))
                stack.enter_context(patch('benchmark.local.LocalCommittee.print'))
                stack.enter_context(patch('benchmark.local.NodeParameters.print'))
                stack.enter_context(patch('benchmark.local.LogParser.process'))
                stack.enter_context(patch('benchmark.local.PathMaker.results_path', return_value=directory))
                if PROTOCOL == 'orca':
                    stack.enter_context(patch('benchmark.local.build_client_schedules', side_effect=AssertionError('Old input suppression reached')))
                bench = LocalBench(params, node)
                bench.processes = MagicMock()
                bench._kill_nodes = lambda: None
                commands = []
                bench._background_run = lambda command, log: commands.append(command)
                bench.run()
                snapshots = list(Path(directory).glob('*.parameters.json'))
                self.assertEqual(len(snapshots), 1)
                snapshot = json.loads(snapshots[0].read_text())
                config = snapshot['adversary']
                self.assertEqual(snapshot['launch_authorities'], KEYS)
                self.assertEqual(config['mode'], mode)
                self.assertEqual(config['byzantine_authorities'], [KEYS[1]] if mode in ['silent', 'targeted'] else [])
                primaries = [c for c in commands if c.endswith(' primary')]
                self.assertEqual(len(primaries), 4)
                for command in primaries:
                    env = dict(token.split('=', 1) for token in shlex.split(command)
                               if '=' in token and token.split('=', 1)[0].isidentifier())
                    self.assertEqual(json.loads(env['BENCH_ADVERSARY_CONFIG']), config)
                    if PROTOCOL == 'orca':
                        self.assertEqual(env['ORCA_FAULTS'], '0')
                clients = [c for c in commands if './benchmark_client ' in c]
                self.assertEqual(len(clients), 4)
                self.assertTrue(all('--silence-schedule' not in c for c in clients))

    def test_direct_local_api_rejects_bad_identity_before_cleanup(self):
        params, node = task_parameters([])
        params['adversary'] = {'mode': 'targeted', 'byzantine_ids': [9]}
        with patch('benchmark.local.subprocess.run') as run:
            with self.assertRaises(BenchError):
                LocalBench(params, node)
            run.assert_not_called()


if __name__ == '__main__':
    unittest.main()
