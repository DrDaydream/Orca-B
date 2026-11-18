from pathlib import Path
import shlex
import sys
import tempfile
import time
import unittest

from benchmark.local_processes import LocalProcesses


class LocalProcessesTests(unittest.TestCase):
    def test_env_prefix_delivers_json_to_actual_child(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'primary-env.log'
            processes = LocalProcesses()
            config = '{"mode":"network","byzantine_authorities":[]}'
            script = 'import os,sys; print(os.environ["BENCH_ADVERSARY_CONFIG"], file=sys.stderr)'
            command = 'env ' + shlex.quote('BENCH_ADVERSARY_CONFIG=' + config) + ' ' + ' '.join(
                shlex.quote(x) for x in [sys.executable, '-c', script])
            try:
                processes.start(command, str(path))
                self.assertEqual(processes.processes[0].wait(timeout=10), 0)
                self.assertEqual(path.read_text().strip(), config)
            finally:
                processes.stop()

    def launch(self, processes, path, code):
        command = ' '.join(shlex.quote(x) for x in [sys.executable, '-u', '-c', code])
        processes.start(command, str(path))

    def test_stop_reaps_children_before_logs_are_read(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'primary-0.log'
            processes = LocalProcesses()
            self.launch(processes, path, 'import sys,time; print("started", file=sys.stderr); time.sleep(60)')
            child = processes.processes[0]
            try:
                deadline = time.monotonic() + 5
                while not path.read_text() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertIn('started', path.read_text())
            finally:
                processes.stop()
            self.assertIsNotNone(child.poll())
            snapshot = path.read_bytes()
            time.sleep(0.05)
            self.assertEqual(snapshot, path.read_bytes())
            processes.stop()  # Cleanup is idempotent.

    def test_duration_starts_at_client_start_after_startup_delay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'client-0-0.log'
            processes = LocalProcesses()
            self.launch(processes, path,
                        'import datetime,sys,time; time.sleep(0.2); '
                        'print(datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00","Z"), '
                        '"INFO client: Start sending transactions", file=sys.stderr); time.sleep(60)')
            try:
                processes.wait(0.15)
                stamp = path.read_text().split()[0]
                from datetime import datetime
                elapsed = time.time() - datetime.fromisoformat(stamp.replace('Z', '+00:00')).timestamp()
                self.assertGreaterEqual(elapsed, 0.15)
            finally:
                processes.stop()

    def test_early_process_failure_rejects_run(self):
        with tempfile.TemporaryDirectory() as directory:
            processes = LocalProcesses()
            self.launch(processes, Path(directory) / 'client-0-0.log', 'raise SystemExit(3)')
            try:
                with self.assertRaisesRegex(RuntimeError, 'exited'):
                    processes.wait(1)
            finally:
                processes.stop()


if __name__ == '__main__':
    unittest.main()
