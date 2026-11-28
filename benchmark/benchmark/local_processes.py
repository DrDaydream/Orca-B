"""Own local test processes and reap them before taking a log snapshot."""
from datetime import datetime
import os
from pathlib import Path
import re
import shlex
import subprocess
import time
from benchmark import timing
from benchmark.utils import Print


class LocalProcesses:
    def __init__(self):
        self.processes = []
        self.client_logs = []
        self.logs = []
        self.clock = None
        self.tokio_worker_threads = None

    def configure_tokio(self, process_count):
        """Share the local machine between its primaries, workers and clients."""
        try:
            cpus = len(os.sched_getaffinity(0))
        except (AttributeError, OSError):
            cpus = os.cpu_count() or 1
        override = os.environ.get('TOKIO_WORKER_THREADS')
        if override is None:
            # Allow another task to run while one runtime thread is busy in RocksDB.
            threads = max(min(2, cpus), cpus // process_count)
            self.tokio_worker_threads = str(threads)
            source = 'automatic'
        else:
            self.tokio_worker_threads = override
            source = 'set by environment'
        Print.info(f'Local Tokio runtime: TOKIO_WORKER_THREADS={self.tokio_worker_threads} '
                   f'per process ({source}, {cpus} CPUs, {process_count} Rust processes)')

    def start(self, command, log_file):
        args, env = shlex.split(command), os.environ.copy()
        if self.tokio_worker_threads is not None:
            env['TOKIO_WORKER_THREADS'] = self.tokio_worker_threads
        if args and args[0] == 'env':
            args.pop(0)
        while args and re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*=.*', args[0]):
            key, value = args.pop(0).split('=', 1)
            env[key] = value
        with open(log_file, 'wb') as log:
            self.processes.append(subprocess.Popen(args, env=env, stderr=log))
        self.logs.append(Path(log_file))
        if Path(log_file).name.startswith('client-'):
            self.client_logs.append(Path(log_file))

    def _check_alive(self):
        for process, logfile in zip(self.processes, self.logs):
            if process.poll() is not None:
                detail = logfile.read_text(encoding='utf-8', errors='replace')[-1000:]
                raise RuntimeError(f'Benchmark process {process.pid} ({logfile}) exited '
                                   f'with {process.returncode}: {detail}')

    def wait(self, duration):
        if self.clock is not None:
            def read_logs(timeout):
                self._check_alive()
                return {path: path.read_text(encoding='utf-8', errors='replace') for path in self.logs}
            return timing.coordinate(self.client_logs, read_logs=read_logs,
                                     duration=duration, **self.clock)
        # Start from the same event as comparison_metrics, not from process launch.
        if not self.client_logs:
            raise RuntimeError('No benchmark clients were launched')
        timeout = time.monotonic() + 120
        starts = {}
        while len(starts) < len(self.client_logs):
            self._check_alive()
            for path in self.client_logs:
                if path not in starts:
                    match = re.search(r'(\d{4}-\S+Z)[^\n]* Start ', path.read_text())
                    if match:
                        starts[path] = datetime.fromisoformat(match[1].replace('Z', '+00:00')).timestamp()
            if time.monotonic() >= timeout:
                raise RuntimeError('Clients did not start within 120 seconds')
            if len(starts) < len(self.client_logs):
                time.sleep(0.05)
        deadline = min(starts.values()) + duration
        if max(starts.values()) >= deadline:
            raise RuntimeError('Client startup skew exceeds the measurement window')
        while time.time() < deadline:
            self._check_alive()
            time.sleep(min(0.1, max(0, deadline - time.time())))
        self._check_alive()

    def stop(self):
        for process in self.processes:
            if process.poll() is None:
                process.terminate()
        for process in self.processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        self.processes.clear()
        self.client_logs.clear()
        self.logs.clear()
        self.clock = None
        self.tokio_worker_threads = None
