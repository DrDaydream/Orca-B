import base64
import json
import shlex
import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import fabfile
from benchmark.adversary import resolve_adversary
from benchmark.commands import CommandMaker
from benchmark.config import LocalCommittee
from benchmark.logs import LogParser, ParseError
from benchmark.remote import Bench


class RemoteFaultTests(unittest.TestCase):
    def parameters(self, **options):
        with patch.object(fabfile, 'Bench') as factory:
            fabfile.remote.body(None, **options)
            args = factory.return_value.run.call_args.args
        return args[0]

    def test_main_defaults_and_faults_only(self):
        defaults = dict(faults=3, nodes=[10], workers=1, collocate=True,
                        rate=[10000, 110000], tx_size=512, duration=300, runs=2)
        self.assertEqual(self.parameters(), defaults)
        for flag in ('fault', 'faults'):
            for count in (0, 2):
                self.assertEqual(self.parameters(**{flag: str(count)}),
                                 dict(defaults, faults=count))

    def test_explicit_modes_are_independent_of_legacy_faults(self):
        for label, mode, count in [('A0', 'none', 0), ('A1', 'silent', 1),
                                   ('A3', 'targeted', 1), ('A5', 'network', 0)]:
            p = self.parameters(adversary=label, byzantine=count)
            self.assertEqual(p['faults'], 0)
            self.assertEqual(p['adversary']['mode'], mode)
        with self.assertRaises(fabfile.Exit):
            self.parameters(adversary='A1', faults='1', byzantine=1)

    def test_survivors_keep_committee_ids_and_full_committee_rate(self):
        names = [base64.b64encode(bytes([i]) * 32).decode() for i in range(4)]
        committee = LocalCommittee(names, 3000, 2)
        p = self.parameters(nodes=4, workers=2, adversary='A1', byzantine_ids='1')
        config = resolve_adversary(p['adversary'], names)
        params = SimpleNamespace(**p)
        bench = Bench.__new__(Bench)
        with patch.object(bench, '_background_run') as launch, patch.object(bench, 'kill'), \
                patch('benchmark.remote.sleep'), patch('benchmark.remote.progress_bar', return_value=[]):
            bench._run_single(8000, committee, params,
                              SimpleNamespace(json={'max_header_delay': 200}),
                              adversary_config=config)
        commands = {log.split('/')[-1]: shlex.split(cmd) for _, cmd, log in
                    (call.args for call in launch.call_args_list)}
        for ident in range(4):
            self.assertEqual(f'primary-{ident}.log' in commands, ident != 1)
            for wid in range(2):
                self.assertEqual(f'worker-{ident}-{wid}.log' in commands, ident != 1)
                self.assertEqual(f'client-{ident}-{wid}.log' in commands, ident != 1)
        cmd = commands['client-2-1.log']
        self.assertEqual(cmd[cmd.index('--rate') + 1], '1000')
        self.assertEqual(len(cmd[cmd.index('--nodes') + 1:]), 6)
        self.assertNotIn('--silence-schedule', cmd)
        cmd = commands['primary-2.log']
        self.assertIn('ORCA_FAULTS=0', cmd)
        env = next(token for token in cmd if token.startswith('BENCH_ADVERSARY_CONFIG='))
        self.assertEqual(json.loads(env.split('=', 1)[1])['mode'], 'none')

    def test_legacy_starts_all_authorities_and_keeps_client_schedules(self):
        names = [base64.b64encode(bytes([i]) * 32).decode() for i in range(4)]
        committee = LocalCommittee(names, 3000, 1)
        params = SimpleNamespace(**self.parameters(nodes=4, faults='1'))
        bench = Bench.__new__(Bench)
        with patch.object(bench, '_background_run') as launch, patch.object(bench, 'kill'), \
                patch('benchmark.remote.sleep'), patch('benchmark.remote.progress_bar', return_value=[]):
            bench._run_single(8000, committee, params, SimpleNamespace(json={'max_header_delay': 200}))
        commands = {log.split('/')[-1]: shlex.split(cmd) for _, cmd, log in
                    (call.args for call in launch.call_args_list)}
        self.assertEqual(len(commands), 12)
        for i in range(4):
            self.assertIn('ORCA_FAULTS=1', commands[f'primary-{i}.log'])
            self.assertFalse(any(v.startswith('BENCH_ADVERSARY_CONFIG=') for v in commands[f'primary-{i}.log']))
            self.assertIn('--silence-schedule', commands[f'client-{i}-0.log'])

    def test_saved_crash_population_reparses_with_original_committee(self):
        class PopulationParser(LogParser):
            def __init__(self, clients, primaries, workers, faults=0):
                self.faults = faults
                self.committee_size = len(primaries)

        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)
            for i in (0, 2, 3):
                (path / f'primary-{i}.log').touch()
                for wid in range(2):
                    (path / f'client-{i}-{wid}.log').touch()
                    (path / f'worker-{i}-{wid}.log').touch()
            options = self.parameters(nodes=4, adversary='A1', byzantine_ids='1')['adversary']
            live = PopulationParser.process(folder, adversary=options, committee_size=4)
            archived = PopulationParser.process(folder, faults='?')
            for result in (live, archived):
                self.assertEqual((result.committee_size, result.faults, result.workers), (4, 1, 2))
            (path / 'client-2-1.log').unlink()
            with self.assertRaises(ParseError):
                PopulationParser.process(folder)

    def test_remote_tmux_retains_quoted_json(self):
        name = base64.b64encode(bytes(range(32))).decode()
        config = self.parameters(nodes=4, adversary='A3', byzantine_ids='0')['adversary']
        config = resolve_adversary(config, [name, 'b', 'c', 'd'])
        cmd = CommandMaker.run_primary('key', 'committee', 'db', 'params', adversary_config=config)
        bench = Bench.__new__(Bench)
        bench.connect = {}
        with patch('benchmark.remote.Connection') as ssh:
            ssh.return_value.run.return_value.stderr = ''
            bench._background_run('test-host', cmd, 'logs/primary-0.log')
        script = shlex.split(ssh.return_value.run.call_args.args[0])[-1]
        env = next(x for x in shlex.split(script) if x.startswith('BENCH_ADVERSARY_CONFIG='))
        self.assertEqual(json.loads(env.split('=', 1)[1]), config)

    def test_crash_count_tracks_each_committee_in_an_edited_remote_sweep(self):
        from benchmark.adversary import remote_parameters
        names = [f'key-{i}' for i in range(10)]
        params = remote_parameters(nodes=[4, 10], rate=[10000, 110000], workers=1, duration=30,
                                   tx_size=512, runs=1, faults=None, default_faults=3,
                                   adversary='A1', byzantine=1)
        for n in params['nodes']:
            self.assertEqual(resolve_adversary(params['adversary'], names[:n])['byzantine_ids'], [n-1])
