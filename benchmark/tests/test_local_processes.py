from pathlib import Path
import os
import shlex
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from benchmark.local_processes import LocalProcesses


class LocalProcessesTests(unittest.TestCase):
    def configured_child_threads(self, processes, process_count):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'worker-0-0.log'
            processes.configure_tokio(process_count)
            self.launch(processes, path,
                        'import os,sys; print(os.environ.get("TOKIO_WORKER_THREADS"), file=sys.stderr)')
            try:
                self.assertEqual(processes.processes[0].wait(timeout=10), 0)
                return path.read_text().strip()
            finally:
                processes.stop()

    def test_local_threads_share_affinity_across_colocated_processes(self):
        for cpus, processes, expected in [(16, 60, '2'), (1, 60, '1'), (24, 6, '4')]:
            with self.subTest(cpus=cpus, processes=processes), patch.dict(os.environ), \
                    patch.object(os, 'sched_getaffinity', create=True, return_value=set(range(cpus))):
                os.environ.pop('TOKIO_WORKER_THREADS', None)
                self.assertEqual(self.configured_child_threads(LocalProcesses(), processes), expected)
                self.assertNotIn('TOKIO_WORKER_THREADS', os.environ)

    def test_local_threads_fall_back_when_affinity_is_unavailable(self):
        with patch.dict(os.environ), \
                patch.object(os, 'sched_getaffinity', create=True, side_effect=OSError), \
                patch.object(os, 'cpu_count', return_value=12):
            os.environ.pop('TOKIO_WORKER_THREADS', None)
            self.assertEqual(self.configured_child_threads(LocalProcesses(), 4), '3')

    def test_explicit_local_thread_count_reaches_child_unchanged(self):
        with patch.dict(os.environ, TOKIO_WORKER_THREADS='7'):
            self.assertEqual(self.configured_child_threads(LocalProcesses(), 60), '7')

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
