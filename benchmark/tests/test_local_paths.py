"""Exercise local benchmark paths without launching cargo, tmux or real nodes."""
import os
import subprocess
import sys
import tempfile
import unittest
from base64 import b64encode
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark import local
from benchmark.config import Key
from benchmark.utils import BenchError


class LocalPathsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name).resolve() / 'Orca B'
        self.bench = self.repo / 'benchmark'
        self.package = self.bench / 'benchmark'
        self.package.mkdir(parents=True)
        self.node = self.repo / 'node'
        self.node.mkdir()
        (self.node / 'Cargo.toml').write_text('[package]\nname = "node"\n')
        # The path ../node from the nested package points to a binary, reproducing
        # the user's Errno 20. It must never be used as the cargo cwd.
        (self.bench / 'node').write_bytes(b'previous node executable')
        self.previous_dir = Path.cwd()
        self.addCleanup(os.chdir, self.previous_dir)
        self.file_patch = patch.object(local, '__file__', str(self.package / 'local.py'))
        self.file_patch.start()
        self.addCleanup(self.file_patch.stop)

    def runner(self):
        return local.LocalBench(
            {'faults': 0, 'nodes': 4, 'workers': 1, 'rate': 100,
             'tx_size': 512, 'duration': 1},
            {'header_size': 512, 'max_header_delay': 200, 'gc_depth': 50,
             'sync_retry_delay': 10_000, 'sync_retry_nodes': 3,
             'batch_size': 500_000, 'max_batch_delay': 200},
        )

    def test_all_local_operations_use_benchmark_directory(self):
        for initial_dir in [self.repo, self.bench, self.package]:
            with self.subTest(initial_dir=initial_dir):
                os.chdir(initial_dir)
                calls = []

                def run(command, **kwargs):
                    effective_cwd = Path(kwargs.get('cwd', '.')).resolve()
                    calls.append((command, Path.cwd(), effective_cwd))
                    if command[0] == 'cargo':
                        self.assertEqual(effective_cwd, self.node)
                        self.assertTrue(effective_cwd.is_dir())
                    return subprocess.CompletedProcess(command, 0)

                keys = [Key(b64encode(bytes([i]) * 32).decode(), 'unused') for i in range(4)]
                with patch.object(local.subprocess, 'run', side_effect=run), \
                        patch.object(local, 'sleep'), \
                        patch.object(local.Key, 'from_file', side_effect=keys), \
                        patch.object(local.LogParser, 'process', return_value='parsed'):
                    self.assertEqual(self.runner().run(), 'parsed')
                self.assertEqual(Path.cwd(), initial_dir)
                self.assertTrue(calls)
                self.assertTrue(all(cwd == self.bench for _, cwd, _ in calls))
                self.assertTrue((self.bench / '.committee.json').is_file())
                self.assertTrue((self.bench / '.parameters.json').is_file())
                self.assertFalse((self.package / '.committee.json').exists())
                self.assertFalse((self.repo / '.committee.json').exists())

    def test_missing_source_stops_before_cleanup_or_tmux(self):
        (self.node / 'Cargo.toml').unlink()
        self.node.rmdir()
        self.node.write_bytes(b'not a source directory')
        os.chdir(self.package)
        with patch.object(local.subprocess, 'run') as run:
            with self.assertRaises(BenchError) as raised:
                self.runner().run()
            run.assert_not_called()
        self.assertIsInstance(raised.exception.cause, NotADirectoryError)
        self.assertEqual(Path.cwd(), self.package)

    def test_compile_os_error_is_reported_and_cwd_restored(self):
        os.chdir(self.package)

        def run(command, **kwargs):
            if command[0] == 'cargo':
                raise FileNotFoundError('cargo not installed')
            return subprocess.CompletedProcess(command, 0)

        with patch.object(local.subprocess, 'run', side_effect=run), patch.object(local, 'sleep'):
            with self.assertRaises(BenchError) as raised:
                self.runner().run()
        self.assertIsInstance(raised.exception.cause, FileNotFoundError)
        self.assertEqual(Path.cwd(), self.package)


if __name__ == '__main__':
    unittest.main()
