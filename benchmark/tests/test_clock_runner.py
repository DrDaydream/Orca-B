"""Validate workload options and the real SSH command/plan boundary without AWS."""
from collections import OrderedDict
from contextlib import ExitStack
import io
import json
from pathlib import Path
import shlex
import shutil
import subprocess
import os
import sys
import time
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

import fabfile
from benchmark.adversary import remote_parameters, resolve_adversary
from benchmark.commands import CommandMaker
from benchmark.config import BenchParameters, Committee, NodeParameters
from benchmark.remote import Bench, ExecutionError
from benchmark import timing
from benchmark.local_processes import LocalProcesses


def parameters(**values):
    inputs = dict(nodes=4, workers=1, rate=100, tx_size=512, duration=10,
                  faults=0, runs=1, default_faults=0, protocol='orca')
    inputs.update(values)
    return remote_parameters(**inputs)


class ClockRunnerTests(unittest.TestCase):
    def test_common_clock_is_default_without_changing_workload(self):
        params = BenchParameters(parameters())
        self.assertTrue(params.common_clock)
        self.assertEqual((params.duration, params.warmup, params.drain, params.rate),
                         (10, 0, 0, [100]))

    def test_interruption_during_clock_wait_stops_owned_processes_and_survives_cleanup_error(self):
        for cleanup_fails in (False, True):
            with self.subTest(cleanup_fails=cleanup_fails):
                bench = Bench.__new__(Bench)
                bench.connect = {}
                bench._background_run = MagicMock()
                bench._wait_for_clients = MagicMock(side_effect=KeyboardInterrupt)
                bench.kill = MagicMock(side_effect=[None, RuntimeError('cleanup failed')]
                                      if cleanup_fails else None)
                config = parameters(adversary='A1', byzantine=1)
                committee = Committee(OrderedDict((name, ['192.0.2.' + str(i + 1)] * 2)
                    for i, name in enumerate(['a', 'b', 'c', 'd'])), 3000)
                adversary = resolve_adversary(config['adversary'], ['a', 'b', 'c', 'd'])
                node = NodeParameters(dict(header_size=20, max_header_delay=200, gc_depth=50,
                    sync_retry_delay=10000, sync_retry_nodes=3, batch_size=500000, max_batch_delay=200))
                with patch('benchmark.remote.Print'), self.assertRaises(KeyboardInterrupt):
                    bench._run_single(100, committee, BenchParameters(config), node,
                                      adversary_config=adversary)
                self.assertEqual(bench.kill.call_count, 2)
                self.assertFalse(bench.kill.call_args.kwargs['delete_logs'])

    def test_uneven_regions_still_supply_the_full_committee(self):
        bench = Bench.__new__(Bench)
        regions = {str(region): [f'region-{region}-host-{i}' for i in range(count)]
                   for region, count in enumerate((8, 7, 7, 7, 6))}
        bench.manager = SimpleNamespace(hosts=lambda: regions)
        selected = bench._select_hosts(BenchParameters(parameters(nodes=35)))
        self.assertEqual(len(selected), 35)
        self.assertEqual(selected[:5], [hosts[0] for hosts in regions.values()])
        self.assertEqual(selected[-1], regions['0'][-1])

    def test_failed_remote_run_collects_current_files_before_removing_owned_storage(self):
        bench = Bench.__new__(Bench)
        bench.connect = {}
        bench._remote_config = '.orca-benchmark/' + 'a' * 32
        sequence = []
        bench.kill = MagicMock(side_effect=lambda **kwargs: sequence.append(
            'prepare' if kwargs['delete_logs'] else 'stop'))
        bench._background_run = MagicMock()
        bench._wait_for_clients = MagicMock(side_effect=ExecutionError('arming failed'))
        config = parameters(adversary='A1', byzantine=1)
        committee = Committee(OrderedDict((name, ['192.0.2.' + str(i + 1)] * 2)
            for i, name in enumerate(['a', 'b', 'c', 'd'])), 3000)
        adversary = resolve_adversary(config['adversary'], ['a', 'b', 'c', 'd'])
        node = NodeParameters(dict(header_size=20, max_header_delay=200, gc_depth=50,
            sync_retry_delay=10000, sync_retry_nodes=3, batch_size=500000, max_batch_delay=200))

        def get(remote, local):
            self.assertTrue(remote.startswith(bench._run_dir + '/logs/'))
            sequence.append('download')
            if remote.endswith('worker-2-0.log'):
                raise FileNotFoundError(remote)
            Path(local).write_text('this attempt')
        def remove(command, **kwargs):
            self.assertIn(bench._run_dir, command)
            self.assertIn('rm -rf --', command)
            sequence.append('remove')

        with TemporaryDirectory() as directory:
            target = Path(directory) / 'attempt'
            with patch.dict(os.environ, {'ORCA_BENCHMARK_LOGS_DIRECTORY': str(target)}), \
                    patch('benchmark.remote.Connection') as connection, \
                    patch('benchmark.remote.Group') as group, \
                    patch('benchmark.remote.Print'), self.assertRaisesRegex(ExecutionError, 'arming failed'):
                connection.return_value.__enter__.return_value.get.side_effect = get
                group.return_value.__enter__.return_value.run.side_effect = remove
                bench._run_single(100, committee, BenchParameters(config), node,
                                  adversary_config=adversary)
            self.assertEqual((target / 'client-0-0.log').read_text(), 'this attempt')
            self.assertFalse((target / 'worker-2-0.log').exists())
            self.assertIn('worker-2-0.log', (target / 'collection-issues.json').read_text())
        self.assertEqual(sequence, ['prepare', 'stop'] + ['download'] * 9 + ['remove'])

    def test_invalid_phase_lengths_rejected_before_runner_construction(self):
        for key in ('warmup', 'drain'):
            for value in (-1, '1.2', True):
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    parameters(**{key: value})

    def test_fabric_exposes_warmup_drain_and_remote_header_override(self):
        with patch.object(fabfile, 'Bench') as bench:
            fabfile.remote.body(None, nodes=4, rate='123', duration=10, faults=0,
                                warmup=3, drain=2, header_size=20)
        params, nodes, _ = bench.return_value.run.call_args.args
        self.assertEqual((params['warmup'], params['drain'], nodes['header_size']), (3, 2, 20))

    def test_client_clock_file_is_one_shell_argument(self):
        command = CommandMaker.run_client('127.0.0.1:3000', 512, 100, [],
                                          start_plan='logs/start plan.json')
        args = shlex.split(command)
        self.assertEqual(args[args.index('--start-plan') + 1], 'logs/start plan.json')

    def test_unsynchronized_controller_prevents_remote_plan_publication(self):
        bench = Bench.__new__(Bench)
        bench.connect = {}
        with patch('benchmark.remote.subprocess.run',
                   side_effect=subprocess.CalledProcessError(1, 'chronyc')), \
                patch('benchmark.remote.Group') as group:
            with self.assertRaisesRegex(ExecutionError, 'clock|chrony'):
                bench._check_clock_sync(['192.0.2.1'])
        group.assert_not_called()

    def test_clock_sync_checks_are_bounded_on_controller_and_hosts(self):
        bench = Bench.__new__(Bench)
        bench.connect = {}
        with patch('benchmark.remote.subprocess.run') as run, patch('benchmark.remote.Group') as group:
            bench._check_clock_sync(['192.0.2.1'])
        self.assertEqual(run.call_args.args[0], ['chronyc', 'waitsync', '10', '0.1', '0', '1'])
        self.assertLessEqual(run.call_args.kwargs['timeout'], 15)
        self.assertIn('chronyc waitsync', group.return_value.__enter__.return_value.run.call_args.args[0])
        self.assertLessEqual(group.return_value.__enter__.return_value.run.call_args.kwargs['timeout'], 15)

    def test_plan_upload_is_scoped_atomic_and_saved_exactly(self):
        bench = Bench.__new__(Bench)
        bench.connect = {}
        bench._run_id = 'own-run'
        bench._run_dir = '.orca-benchmark/config/own-run'
        bench._clock_clients = [('192.0.2.1', 'logs/client-0-0.log'),
                                ('192.0.2.1', 'logs/client-0-1.log')]
        bench._snapshot = None
        plan = dict(schema=2, run_id='own-run', load_us=31_000_000, start_us=33_000_000,
                    end_us=43_000_000, drain_end_us=45_000_000)
        commands, uploads = [], []
        class Connection:
            def __init__(self, host, **kwargs):
                self.host = host
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def open(self):
                pass
            def sftp(self):
                return SimpleNamespace(get_channel=lambda: SimpleNamespace(settimeout=lambda timeout: None))
            def put(self, value, remote):
                uploads.append((self.host, value.read(), remote))
            def run(self, command, **kwargs):
                commands.append(command)
                return SimpleNamespace(stdout='', stderr='', exited=0)
        with TemporaryDirectory() as directory, ExitStack() as stack:
            logs = Path(directory) / 'logs'
            results = Path(directory) / 'results'
            stack.enter_context(patch('benchmark.remote.Connection', Connection))
            stack.enter_context(patch('benchmark.remote.PathMaker.logs_path', return_value=str(logs)))
            stack.enter_context(patch('benchmark.remote.PathMaker.results_path', return_value=str(results)))
            bench._publish_clock(plan)
            self.assertEqual(json.loads((logs / 'measurement-window.json').read_text()), plan)
            self.assertEqual(json.loads((results / 'own-run.measurement-window.json').read_text()), plan)
        self.assertEqual(len(uploads), 1)
        self.assertEqual(json.loads(uploads[0][1]), plan)
        self.assertTrue(uploads[0][2].startswith('.orca-benchmark/config/own-run/logs/'))
        self.assertTrue(uploads[0][2].endswith('.tmp'))
        self.assertEqual(len(commands), 1)
        self.assertIn('test ! -e logs/benchmark-start.json', commands[0])
        self.assertIn('mv --', commands[0])
        self.assertNotIn('kill-server', commands[0])

    def test_background_and_cleanup_only_target_this_runs_tmux_sessions(self):
        bench = Bench.__new__(Bench)
        bench.connect = {}
        bench._run_id = 'own-run'
        bench._run_dir = '.orca-benchmark/config/own-run'
        with patch('benchmark.remote.Connection') as connection, patch('benchmark.remote.Group') as group:
            connection.return_value.run.return_value.stderr = ''
            bench._background_run('192.0.2.1', './benchmark_client x', 'logs/client-0-0.log')
            command = connection.return_value.run.call_args.args[0]
            bench.kill(hosts=['192.0.2.1'])
            cleanup = group.return_value.run.call_args.args[0]
        self.assertIn('orca-own-run-client-0-0', command)
        self.assertIn('cd .orca-benchmark/config/own-run', command)
        self.assertIn('orca-own-run-', cleanup)
        self.assertNotIn('kill-server', cleanup)

    def test_new_run_keeps_another_run_untouched_and_refuses_stale_directory(self):
        bash = shutil.which('bash')
        if os.name == 'nt' and shutil.which('git'):
            candidate = Path(shutil.which('git')).resolve().parents[1] / 'bin/bash.exe'
            bash = str(candidate) if candidate.is_file() else None
        if not bash:
            self.skipTest('Bash is required for this Ubuntu shell boundary check')
        bench = Bench.__new__(Bench)
        bench.connect = {}
        bench._run_id = 'own-run'
        bench._remote_config = '.orca-benchmark/config'
        bench._run_dir = '.orca-benchmark/config/own-run'
        with patch('benchmark.remote.Group') as group:
            bench.kill(hosts=['192.0.2.1'], delete_logs=True)
            command = group.return_value.run.call_args.args[0]
        with TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / bench._remote_config
            config.mkdir(parents=True)
            (config / '.committee.json').write_text('{"fixture":1}')
            (root / 'node').write_text('fixture node')
            (root / 'benchmark_client').write_text('fixture client')
            live = root / 'logs'
            live.mkdir()
            old_plan = live / 'benchmark-start.json'
            old_plan.write_text('{"run_id":"unrelated-live-run"}')
            result = subprocess.run([bash, '-c', command], cwd=root, text=True, capture_output=True,
                                    encoding='utf-8', errors='replace')
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual((root / bench._run_dir / '.committee.json').read_text(), '{"fixture":1}')
            self.assertFalse((root / bench._run_dir / timing.START_PLAN).exists())
            stale = subprocess.run([bash, '-c', command], cwd=root, text=True, capture_output=True,
                                   encoding='utf-8', errors='replace')
            self.assertNotEqual(stale.returncode, 0)
            self.assertEqual(json.loads(old_plan.read_text())['run_id'], 'unrelated-live-run')

    def test_local_processes_publish_one_plan_and_survive_through_drain(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            plan_file = root / 'start.json'
            primary_log = root / 'primary-0.log'
            processes = LocalProcesses()
            def launch(code, logfile):
                command = ' '.join(map(shlex.quote, [sys.executable, '-u', '-c', code]))
                processes.start(command, str(logfile))
            primary = 'import sys,time; print("Primary fixture booted on 127.0.0.1", file=sys.stderr); time.sleep(30)'
            client = '''import sys,time,json
from pathlib import Path
path = Path(PLAN_FILE)
print('Benchmark clock ready', file=sys.stderr)
while not path.exists(): time.sleep(0.005)
plan = json.loads(path.read_text())
print('Benchmark clock armed ' + json.dumps(plan), file=sys.stderr)
time.sleep(max(0, (plan['load_us'] - time.time_ns() // 1000) / 1e6))
actual = time.time_ns() // 1000
print(f"Benchmark input started at_us {actual} lateness_us {actual - plan['load_us']}", file=sys.stderr)
time.sleep(max(0, (plan['end_us'] - time.time_ns() // 1000) / 1e6))
print(f"Benchmark input stopped at_us {time.time_ns() // 1000} released_samples 0", file=sys.stderr)
time.sleep(30)
'''.replace('PLAN_FILE', repr(str(plan_file)))
            try:
                launch(primary, primary_log)
                for node in (0, 2):
                    launch(client, root / f'client-{node}-0.log')
                processes.clock = dict(primaries=[primary_log], warmup=0, drain=1,
                    run_id='fixture-run', publish=lambda plan: timing.write_plan(plan_file, plan))
                with patch.object(timing, 'LEAD_US', 500_000), patch.object(timing, 'ACK_LEAD_US', 100_000):
                    result = processes.wait(1)
                self.assertEqual(json.loads(plan_file.read_text()), result)
                self.assertGreaterEqual(time.time_ns() // 1000, result['drain_end_us'])
                self.assertTrue(all(process.poll() is None for process in processes.processes))
                for path in processes.client_logs:
                    armed = path.read_text().split('Benchmark clock armed ', 1)[1].splitlines()[0]
                    self.assertEqual(json.loads(armed), result)
            finally:
                processes.stop()


if __name__ == '__main__':
    unittest.main()
