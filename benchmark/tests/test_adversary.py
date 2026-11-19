"""Offline parameter and launch checks; no AWS calls or native build."""
import json
import os
from base64 import b64encode
from collections import OrderedDict
from pathlib import Path
import shlex
import shutil
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from benchmark.adversary import (
    remote_parameters, resolve_adversary, write_run_snapshot,
)
from benchmark.commands import CommandMaker
from benchmark.config import BenchParameters, Committee, NodeParameters
from benchmark.remote import Bench

PROTOCOL = 'orca'
DEFAULTS = {'faults': 0, 'nodes': [20], 'rate': [50000], 'duration': 80, 'runs': 1}
KEYS = [b64encode(bytes([i]) * 32).decode() for i in (9, 3, 6, 1)]


def parameters(**overrides):
    args = dict(nodes=4, workers=1, rate='100,200', tx_size=512,
                duration=10, faults=None, runs=1, default_faults=0,
                protocol=PROTOCOL)
    args.update(overrides)
    return remote_parameters(**args)


def environment(command):
    tokens = shlex.split(command)
    return dict(token.split('=', 1) for token in tokens
                if '=' in token and token.split('=', 1)[0].isidentifier())


class AdversaryParameterTests(unittest.TestCase):
    def test_missing_option_preserves_legacy_but_none_is_explicit(self):
        legacy = parameters(default_faults=1)
        self.assertEqual(legacy['faults'], 1)
        self.assertNotIn('adversary', legacy)
        baseline = parameters(adversary='A0', default_faults=1)
        self.assertEqual(baseline['faults'], 0)
        self.assertEqual(resolve_adversary(baseline['adversary'], KEYS)['mode'], 'none')

    def test_aliases_emit_only_supported_modes(self):
        for alias, mode in [('A0', 'none'), ('A1', 'silent'),
                            ('A3', 'targeted'), ('A5', 'network')]:
            with self.subTest(alias=alias):
                params = parameters(adversary=alias, byzantine=0 if mode == 'none' else 1)
                self.assertEqual(resolve_adversary(params['adversary'], KEYS)['mode'], mode)
        for alias in ('A2', 'A4', 'anything'):
            with self.subTest(alias=alias), self.assertRaises(ValueError):
                parameters(adversary=alias)

    def test_launch_ids_resolve_without_sorting_public_keys(self):
        params = parameters(adversary='targeted', byzantine_ids='1')
        config = resolve_adversary(params['adversary'], KEYS)
        self.assertEqual(config['byzantine_authorities'], [KEYS[1]])
        self.assertEqual(config['seed'], 1)
        self.assertEqual(config['start_round'], 1)
        self.assertEqual(config['rounds'], 0)
        self.assertEqual(config['max_pending_bytes'], 67108864)

    def test_a1_count_selects_last_launch_ids_and_resolves_full_keys(self):
        params = parameters(adversary='silent', byzantine=1)
        self.assertEqual(resolve_adversary(params['adversary'], KEYS)
                         ['byzantine_authorities'], [KEYS[-1]])

    def test_invalid_identity_sets_fail_before_launch(self):
        cases = [dict(byzantine=2), dict(byzantine_ids='1,1'),
                 dict(byzantine_ids='4'), dict(byzantine_ids='-1'),
                 dict(byzantine=1, byzantine_ids='0')]
        for args in cases:
            with self.subTest(args=args), self.assertRaises(ValueError):
                parameters(adversary='silent', **args)
        with self.assertRaises(ValueError):
            parameters(byzantine=1)
        with self.assertRaises(ValueError):
            parameters(adversary='none', byzantine=1)

    def test_bounds_reject_overflow_and_invalid_windows(self):
        for args in [
            dict(seed=-1), dict(seed=2**64), dict(start_round=0),
            dict(start_round=2**64 - 1, attack_rounds=1),
            dict(attack_rounds=-1), dict(delay_ms=-1),
            dict(delay_ms=0), dict(delay_ms=86400001),
            dict(max_pending_messages=0), dict(max_pending_messages=2**64),
            dict(max_pending_bytes=0), dict(max_pending_bytes=2**64),
        ]:
            with self.subTest(args=args), self.assertRaises(ValueError):
                parameters(adversary='network', **args)

    def test_no_fixed_byzantine_set_for_network_or_bullshark_alternation(self):
        config = resolve_adversary(parameters(adversary='network')['adversary'], KEYS)
        self.assertEqual(config['byzantine_authorities'], [])
        if PROTOCOL == 'bullshark':
            options = parameters(adversary='targeted')['adversary']
            self.assertTrue(resolve_adversary(options, KEYS)['bullshark_alternating'])
        with self.assertRaises(ValueError):
            parameters(adversary='silent')

    def test_false_alternation_requires_fixed_set_for_silent_and_targeted(self):
        for mode in ('silent', 'targeted'):
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                parameters(adversary=mode, bullshark_alternating='false')
        params = parameters(adversary='targeted', byzantine=1,
                            bullshark_alternating='false')
        self.assertFalse(resolve_adversary(params['adversary'], KEYS)
                         ['bullshark_alternating'])
        with self.assertRaises(ValueError):
            parameters(adversary='network', bullshark_alternating='maybe')

    def test_faults_keep_protocol_meaning(self):
        if PROTOCOL == 'bullshark':
            params = parameters(adversary='targeted', byzantine_ids='0', faults=1)
            self.assertEqual(params['faults'], 1)
            with self.assertRaises(ValueError):
                parameters(adversary='silent', byzantine_ids='3', faults=1)
        else:
            with self.assertRaises(ValueError):
                parameters(adversary='silent', byzantine=1, faults=1)

    def test_primary_environment_round_trips_exact_json_and_disables_legacy(self):
        config = resolve_adversary(parameters(adversary='A0')['adversary'], KEYS)
        with patch.dict(os.environ, {'ORCA_RULE3_BEHAVIOR': 'invalid',
                                     'ORCA_ADVERSARY_SEED': 'invalid'}):
            kwargs = dict(adversary_config=config)
            if PROTOCOL == 'orca':
                kwargs['faults'] = 3
            command = CommandMaker.run_primary('key', 'committee', 'store', 'params', **kwargs)
        self.assertEqual(shlex.split(command)[0], 'env')
        env = environment(command)
        self.assertEqual(json.loads(env['BENCH_ADVERSARY_CONFIG']), config)
        if PROTOCOL == 'orca':
            self.assertEqual(env['ORCA_FAULTS'], '0')
        else:
            self.assertNotIn('ORCA_FAULTS', env)

    def test_legacy_primary_has_no_new_environment(self):
        kwargs = dict(faults=1) if PROTOCOL == 'orca' else {}
        with patch.dict(os.environ, {'ORCA_RULE3_BEHAVIOR': 'mixed',
                                     'ORCA_ADVERSARY_SEED': '7'}):
            command = CommandMaker.run_primary('key', 'committee', 'store', 'params', **kwargs)
        self.assertNotIn('BENCH_ADVERSARY_CONFIG', environment(command))
        if PROTOCOL == 'orca':
            self.assertEqual(environment(command)['ORCA_FAULTS'], '1')
            self.assertEqual(environment(command)['ORCA_ADVERSARY_SEED'], '7')

    def test_explicit_mode_preserves_opt_in_latency_tracing(self):
        config = resolve_adversary(parameters(adversary='none')['adversary'], KEYS)
        with patch.dict(os.environ, {'ORCA_LATENCY_TRACE': '1'}):
            command = CommandMaker.run_primary('key', 'committee', 'store', 'params',
                                              adversary_config=config)
        self.assertEqual(environment(command)['RUST_LOG'], 'info,orca_latency=debug')

    def test_snapshot_keeps_run_input_and_actual_identity_mapping(self):
        params = parameters(adversary='targeted', byzantine_ids='1', seed=42)
        config = resolve_adversary(params['adversary'], KEYS)
        with TemporaryDirectory() as directory:
            output = write_run_snapshot(directory, params, {'header_size': 20},
                                        KEYS, config, 4, 200, 2, protocol=PROTOCOL)
            snapshot = json.loads(Path(output).read_text())
        self.assertEqual(snapshot['adversary'], config)
        self.assertEqual(snapshot['launch_authorities'], KEYS)
        self.assertEqual(snapshot['nodes'], 4)
        self.assertEqual(snapshot['rate'], 200)
        self.assertEqual(snapshot['run'], 2)
        self.assertEqual(snapshot['benchmark_parameters']['duration'], 10)
        self.assertEqual(snapshot['node_parameters']['header_size'], 20)


class FabricArgumentTests(unittest.TestCase):
    def parse(self, args):
        import fabfile
        from invoke.parser import Parser, ParserContext
        parser = Parser(contexts=[
            ParserContext(name='remote', args=fabfile.remote.get_arguments())
        ])
        return parser.parse_argv(['remote'] + args)[0].as_kwargs

    def invoke(self, args):
        import fabfile
        from invoke import Context
        with patch('benchmark.remote.Bench') as remote_bench:
            # Orca imports Bench into fabfile; Bullshark imports it in the task.
            with patch.object(fabfile, 'Bench', remote_bench, create=True):
                fabfile.remote(Context(), **self.parse(args))
            return remote_bench.return_value.run.call_args.args[0]

    def test_real_fabric_parser_accepts_new_flags_and_false(self):
        params = self.invoke([
            '--nodes', '4', '--workers', '2', '--rate', '123',
            '--tx-size', '256', '--duration', '12', '--faults', '0', '--runs', '3',
            '--adversary', 'A3', '--byzantine-ids', '1',
            '--seed', '42', '--start-round', '5', '--attack-rounds', '8',
            '--delay-ms', '99', '--max-pending-messages', '30',
            '--max-pending-bytes', '4096', '--bullshark-alternating', 'false',
        ])
        self.assertEqual((params['nodes'], params['workers'], params['rate']), ([4], 2, [123]))
        self.assertEqual((params['tx_size'], params['duration'], params['runs']), (256, 12, 3))
        config = resolve_adversary(params['adversary'], KEYS)
        self.assertEqual((config['seed'], config['start_round'], config['rounds']), (42, 5, 8))
        self.assertEqual((config['delay_ms'], config['max_pending_messages'],
                          config['max_pending_bytes']), (99, 30, 4096))
        self.assertFalse(config['bullshark_alternating'])

    def test_fabric_default_benchmark_preserves_previous_run(self):
        params = self.invoke([])
        self.assertNotIn('adversary', params)
        for key, value in DEFAULTS.items():
            self.assertEqual(params[key], value, key)

    def test_bad_fabric_parameters_rejected_before_aws_construction(self):
        import fabfile
        from invoke import Context
        from invoke.exceptions import Exit
        with patch('benchmark.remote.Bench', side_effect=AssertionError('AWS reached')) as remote_bench:
            with patch.object(fabfile, 'Bench', remote_bench, create=True):
                with self.assertRaises(Exit):
                    fabfile.remote(Context(), **self.parse([
                        '--nodes', '4', '--adversary', 'targeted', '--byzantine', '2',
                    ]))



class LaunchIntegrationTests(unittest.TestCase):
    def committee(self):
        return Committee(OrderedDict(
            (name, [f'192.0.2.{i + 1}'] * 2) for i, name in enumerate(KEYS)
        ), 3000)

    def test_explicit_mode_launches_config_and_never_suppresses_client_input(self):
        params = parameters(adversary='targeted', byzantine_ids='1')
        config = resolve_adversary(params['adversary'], KEYS)
        bench = Bench.__new__(Bench)
        bench.connect = {}
        commands = []
        bench.kill = lambda **kwargs: None
        bench._background_run = lambda host, command, log: commands.append(command)
        bench._wait_for_clients = lambda clients, duration: None
        node = NodeParameters(dict(header_size=20, max_header_delay=200,
                                   gc_depth=50, sync_retry_delay=10000,
                                   sync_retry_nodes=3, batch_size=500000,
                                   max_batch_delay=200, max_concurrent_requests=500000))
        args = [100, self.committee(), BenchParameters(params)]
        if PROTOCOL == 'orca':
            args.append(node)
        with patch('benchmark.remote.sleep'):
            if PROTOCOL == 'orca':
                with patch('benchmark.remote.build_client_schedules',
                           side_effect=AssertionError('Legacy input suppression reached')):
                    bench._run_single(*args, adversary_config=config)
            else:
                bench._run_single(*args, adversary_config=config)
        primaries = [command for command in commands if command.endswith(' primary')]
        clients = [command for command in commands if './benchmark_client ' in command]
        self.assertEqual(len(primaries), 4)
        self.assertEqual(len(clients), 4)
        for command in primaries:
            self.assertEqual(json.loads(environment(command)['BENCH_ADVERSARY_CONFIG']), config)
        for command in clients:
            self.assertNotIn('--silence-schedule', shlex.split(command))

    def test_remote_run_resolves_keys_and_records_the_config_it_launches(self):
        from types import SimpleNamespace
        params = parameters(adversary='targeted', byzantine_ids='1', rate=100)
        node = dict(header_size=20, max_header_delay=200, gc_depth=50,
                    sync_retry_delay=10000, sync_retry_nodes=3, batch_size=500000,
                    max_batch_delay=200, max_concurrent_requests=500000)
        committee = self.committee()
        bench = Bench.__new__(Bench)
        bench.connect = {}
        bench._select_hosts = lambda params: committee.ips()
        bench._controller_revision = lambda: 'a' * 40
        bench.install = lambda **kwargs: None
        bench._update = lambda hosts, collocate: None
        bench._check_binaries = lambda hosts: None
        bench._config = lambda *args: committee
        bench.kill = lambda **kwargs: None
        bench._wait_for_clients = lambda clients, duration: None
        commands = []
        bench._background_run = lambda host, command, log: commands.append(command)
        bench._logs = lambda *args, **kwargs: SimpleNamespace(
            result=lambda: '', print=lambda *args: None,
            print_comparison=lambda *args: None,
        )
        with TemporaryDirectory() as directory:
            with patch('benchmark.remote.PathMaker.results_path', return_value=directory), \
                    patch('benchmark.remote.sleep'), patch('benchmark.remote.Print'):
                bench.run(params, node)
            files = list(Path(directory).glob('*.parameters.json'))
            self.assertEqual(len(files), 1)
            snapshot = json.loads(files[0].read_text(encoding='utf-8'))
        self.assertEqual(snapshot['adversary']['byzantine_authorities'], [KEYS[1]])
        self.assertEqual(snapshot['launch_authorities'], KEYS)
        primaries = [command for command in commands if command.endswith(' primary')]
        self.assertEqual(len(primaries), 4)
        for command in primaries:
            self.assertEqual(json.loads(environment(command)['BENCH_ADVERSARY_CONFIG']),
                             snapshot['adversary'])


    def test_tmux_shell_argument_preserves_environment_json(self):
        config = resolve_adversary(parameters(adversary='network')['adversary'], KEYS)
        primary = CommandMaker.run_primary('key', 'committee', 'store', 'params',
                                            adversary_config=config)
        bench = Bench.__new__(Bench)
        bench.connect = {}
        from types import SimpleNamespace
        class Connection:
            def __init__(self, *args, **kwargs):
                pass
            def run(self, command, **kwargs):
                self_test.commands.append(command)
                return SimpleNamespace(stderr='')
        self.commands = []
        self_test = self
        with patch('benchmark.remote.Connection', Connection):
            bench._background_run('192.0.2.1', primary, 'logs/primary-0.log')
        command = shlex.split(self.commands[0])[-1]
        if PROTOCOL == 'bullshark':
            self.assertEqual(shlex.split(command)[:2], ['bash', '-lc'])
            command = shlex.split(command)[2]
            self.assertEqual(shlex.split(command)[:2], ['exec', 'env'])
        self.assertEqual(json.loads(environment(command)['BENCH_ADVERSARY_CONFIG']), config)

        bash = shutil.which('bash')
        if os.name == 'nt' and shutil.which('git'):
            git_bash = Path(shutil.which('git')).resolve().parents[1] / 'bin/bash.exe'
            bash = str(git_bash) if git_bash.is_file() else None
        if not bash:
            self.skipTest('Bash is required for the shell integration check')
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'logs').mkdir()
            stub = root / 'node'
            stub.write_text('#!/usr/bin/env bash\nprintf \'%s\\n\' "$BENCH_ADVERSARY_CONFIG"\n',
                            encoding='utf-8')
            stub.chmod(0o755)
            # tmux passes one shell command to its child shell. Keep both shell
            # parsing stages real while replacing only the terminal server.
            harness = 'tmux() { for arg in "$@"; do :; done; bash -c "$arg"; }; '
            result = subprocess.run([bash, '-c', harness + self.commands[0]],
                                    cwd=root, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            actual = (root / 'logs/primary-0.log').read_text(encoding='utf-8').strip()
            self.assertEqual(json.loads(actual), config)

if __name__ == '__main__':
    unittest.main()
