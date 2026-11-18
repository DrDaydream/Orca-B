"""Build/install separation, using real Bash scripts without Rust or AWS."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

try:
    import boto3  # noqa: F401
except ImportError:
    raise unittest.SkipTest('AWS tests require benchmark/requirements.txt')

from benchmark.config import BenchParameters
from benchmark.remote import Bench
from benchmark.utils import BenchError


def bash_executable():
    bash = shutil.which('bash')
    if os.name == 'nt' and shutil.which('git'):
        candidate = Path(shutil.which('git')).resolve().parents[1] / 'bin/bash.exe'
        if candidate.is_file():
            bash = str(candidate)
    if not bash:
        raise unittest.SkipTest('Bash is required')
    return bash


class BuildTests(unittest.TestCase):
    def test_local_build_invalidates_marker_if_sources_change_during_compilation(self):
        bench = object.__new__(Bench)
        bench.revision = 'a' * 40
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory, '.orca-build-commit')
            failure = BenchError('sources changed', RuntimeError('dirty build sources'))
            with patch('benchmark.remote.PathMaker.binary_path', return_value=directory), \
                 patch.object(Bench, '_controller_revision', side_effect=['a' * 40, failure]), \
                 patch('benchmark.remote.subprocess.run', side_effect=lambda *a, **kw: marker.write_text('a' * 40)):
                with self.assertRaises(BenchError):
                    bench._build_local()
            self.assertFalse(marker.exists(), 'Changed source cannot retain a valid commit marker')

    def test_build_records_commit_only_after_both_binaries_exist_and_invalidates_failed_build(self):
        bench = object.__new__(Bench)
        bench.revision = 'a' * 40
        self.assertTrue(callable(getattr(bench, '_build_command', None)))
        with tempfile.TemporaryDirectory() as directory:
            node = Path(directory, 'node')
            node.mkdir()
            marker = Path(directory, 'target/release/.orca-build-commit')
            compiler = '''cargo() {
                echo 'Compiling fixture'
                mkdir -p ../target/release
                printf '#!/bin/sh\\nexit 0\\n' > ../target/release/node
                printf '#!/bin/sh\\nexit 0\\n' > ../target/release/benchmark_client
                chmod +x ../target/release/node ../target/release/benchmark_client
            }
'''
            good = subprocess.run([bash_executable(), '-c', compiler + bench._build_command()],
                                  cwd=node, capture_output=True, text=True)
            self.assertEqual(good.returncode, 0, good.stderr)
            self.assertIn('Compiling fixture', good.stdout)
            self.assertEqual(marker.read_text().strip(), 'a' * 40)
            failed = subprocess.run([bash_executable(), '-c',
                                     'cargo() { echo failed >&2; return 23; };\n' + bench._build_command()],
                                    cwd=node, capture_output=True, text=True)
            self.assertEqual(failed.returncode, 23)
            self.assertFalse(marker.exists(), 'A failed rebuild must not retain a success marker')

    def test_remote_build_streams_output_and_pins_controller_commit(self):
        bench = object.__new__(Bench)
        bench.revision = 'a' * 40
        bench.connect = {}
        bench.settings = SimpleNamespace(repo_name='bullshark', repo_url='origin', branch='main')
        with patch('benchmark.remote.Group') as group, \
             patch('benchmark.remote.Path.mkdir'), patch('benchmark.remote.Path.write_text'):
            bench._update(['203.0.113.1'], collocate=True)
        call = group.return_value.__enter__.return_value.run.call_args
        self.assertFalse(call.kwargs['hide'])

    def test_runtime_rejects_missing_and_outdated_controller_builds(self):
        bench = object.__new__(Bench)
        bench.revision = 'a' * 40
        bench.connect = {}
        bench.settings = SimpleNamespace(repo_name='bullshark')
        self.assertTrue(callable(getattr(bench, '_check_binaries', None)))
        with tempfile.TemporaryDirectory() as directory, \
             patch('benchmark.remote.PathMaker.binary_path', return_value=directory), \
             patch('benchmark.remote.Group') as group:
            with self.assertRaises(BenchError) as caught:
                bench._check_binaries(['203.0.113.1'])
            self.assertIn('fab install', str(caught.exception))
            group.assert_not_called()
            for binary in ('node', 'benchmark_client'):
                path = Path(directory, binary)
                path.write_text('#!/bin/sh\nexit 0\n')
                path.chmod(0o755)
            marker = Path(directory, '.orca-build-commit')
            marker.write_text('b' * 40)
            with self.assertRaises(BenchError):
                bench._check_binaries(['203.0.113.1'])
            marker.write_text('a' * 40)
            bench._check_binaries(['203.0.113.1'])
            group.assert_called_once()

    def test_remote_binary_checks_detect_wrong_commit_and_missing_executable(self):
        bench = object.__new__(Bench)
        bench.revision = 'a' * 40
        bench.settings = SimpleNamespace(repo_name='repo with space')
        self.assertTrue(callable(getattr(bench, '_binary_check_command', None)))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            release = root / 'repo with space/target/release'
            release.mkdir(parents=True)
            for binary in ('node', 'benchmark_client'):
                path = release / binary
                path.write_text('#!/bin/sh\nexit 0\n')
                path.chmod(0o755)
                os.link(path, root / binary)
            marker = release / '.orca-build-commit'
            def check():
                return subprocess.run([bash_executable(), '-c', bench._binary_check_command()],
                                      cwd=root, capture_output=True, text=True)
            self.assertNotEqual(check().returncode, 0)
            marker.write_text('b' * 40)
            self.assertNotEqual(check().returncode, 0)
            marker.write_text('a' * 40)
            result = check()
            self.assertEqual(result.returncode, 0, result.stderr)
            (root / 'node').unlink()
            self.assertNotEqual(check().returncode, 0)

    def test_configuration_generates_keys_and_uploads_without_cargo(self):
        bench = object.__new__(Bench)
        bench.revision = 'a' * 40
        bench.settings = SimpleNamespace(base_port=3000)
        bench.connect = {}
        params = BenchParameters(dict(nodes=[2], workers=1, rate=[1000],
                                      faults=0, tx_size=512, duration=1, runs=1))
        commands = []
        def run(command, **kwargs):
            commands.append(command)
            if isinstance(command, list) and command[:2] == ['./node', 'generate_keys']:
                filename = command[-1]
                Path(filename).write_text(json.dumps({'name': filename, 'secret': 'test-only'}))
            return SimpleNamespace(returncode=0)
        with tempfile.TemporaryDirectory() as directory:
            previous = Path.cwd()
            try:
                os.chdir(directory)
                with patch.object(Bench, '_controller_revision', return_value='a' * 40), \
                     patch('benchmark.remote.subprocess.run', side_effect=run), \
                     patch('benchmark.remote.Connection') as connection:
                    node_params = SimpleNamespace(print=lambda path: Path(path).write_text('{}'))
                    committee = bench._config(['203.0.113.1', '203.0.113.2'], node_params, params)
                self.assertEqual(committee.size(), 2)
                self.assertEqual(connection.return_value.put.call_count, 6)
                self.assertTrue(Path('.committee.json').is_file())
                self.assertFalse(any('cargo build' in (' '.join(command) if isinstance(command, list) else command) for command in commands), commands)
            finally:
                os.chdir(previous)


    def test_install_builds_controller_and_nodes_before_reporting_success(self):
        bench = object.__new__(Bench)
        bench.connect = {}
        bench.settings = SimpleNamespace(repo_name='orca', repo_url='origin', branch='adversary-experiments')
        revision = 'a' * 40
        with patch.object(Bench, '_controller_revision', return_value=revision), \
             patch('benchmark.remote.subprocess.run') as run, \
             patch('benchmark.remote.Group') as group:
            bench.install(hosts=['192.0.2.1'])
        local = [str(call.args[0]) for call in run.call_args_list]
        remote = [str(call.args[0]) for call in group.return_value.run.call_args_list]
        remote += [str(call.args[0]) for call in group.return_value.__enter__.return_value.run.call_args_list]
        self.assertTrue(any('cargo build' in command for command in local), local)
        self.assertTrue(any('cargo build' in command and revision in command for command in remote), remote)

    def test_remote_reuses_install_across_rates_and_never_prepares_or_builds(self):
        bench = object.__new__(Bench)
        bench.connect = {}
        bench.settings = SimpleNamespace(repo_name='orca', base_port=3000)
        parameters = dict(nodes=2, workers=1, faults=0, rate=[1000, 2000], tx_size=512, duration=1, runs=1)
        node_parameters = dict(header_size=1000, max_header_delay=200, gc_depth=50,
                               sync_retry_delay=10000, sync_retry_nodes=3, batch_size=500000, max_batch_delay=200)
        commands, experiments = [], []
        def run(command, **kwargs):
            commands.append(' '.join(command) if isinstance(command, list) else command)
            if isinstance(command, list) and command[:2] == ['./node', 'generate_keys']:
                Path(command[-1]).write_text(json.dumps(dict(name=command[-1], secret='fixture')))
            return SimpleNamespace(returncode=0)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            release = root / 'release'
            release.mkdir()
            for name in ('node', 'benchmark_client'):
                (release / name).write_text('#!/bin/sh\nexit 0\n')
                (release / name).chmod(0o755)
            (release / '.orca-build-commit').write_text('a' * 40)
            previous = Path.cwd()
            try:
                os.chdir(root)
                with patch.object(Bench, '_select_hosts', return_value=['192.0.2.1', '192.0.2.2']), \
                     patch.object(Bench, '_controller_revision', return_value='a' * 40), \
                     patch.object(Bench, 'install', side_effect=AssertionError('remote must not install')), \
                     patch.object(Bench, '_update', side_effect=AssertionError('remote must not update/build')), \
                     patch.object(Bench, '_run_single', side_effect=lambda rate, *a, **kw: experiments.append(rate)), \
                     patch.object(Bench, '_logs') as logs, \
                     patch('benchmark.remote.PathMaker.binary_path', return_value=str(release)), \
                     patch('benchmark.remote.subprocess.run', side_effect=run), \
                     patch('benchmark.remote.Connection'), patch('benchmark.remote.Group') as group:
                    bench.run(parameters, node_parameters)
                self.assertEqual(experiments, [1000, 2000])
                self.assertEqual(logs.return_value.print.call_count, 2)
                self.assertTrue((root / '.committee.json').is_file())
                commands += [str(call.args[0]) for call in group.return_value.__enter__.return_value.run.call_args_list]
                for command in commands:
                    self.assertFalse(any(word in command for word in ('cargo build', 'apt-get', 'rustup', 'git fetch', 'git checkout')), command)
            finally:
                os.chdir(previous)


if __name__ == '__main__':
    unittest.main()
