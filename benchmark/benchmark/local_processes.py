"""Own local test processes and reap them before taking a log snapshot."""
from datetime import datetime
import os
from pathlib import Path
import re
import shlex
import subprocess
import time


class LocalProcesses:
    def __init__(self):
        self.processes = []
        self.client_logs = []

    def start(self, command, log_file):
        args, env = shlex.split(command), os.environ.copy()
        while args and re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*=.*', args[0]):
            key, value = args.pop(0).split('=', 1)
            env[key] = value
        with open(log_file, 'wb') as log:
            self.processes.append(subprocess.Popen(args, env=env, stderr=log))
        if Path(log_file).name.startswith('client-'):
            self.client_logs.append(Path(log_file))

    def _check_alive(self):
        for process in self.processes:
            if process.poll() is not None:
                raise RuntimeError(f'Benchmark process {process.pid} exited with {process.returncode}')

    def wait(self, duration):
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
        # Polling is outside the node processes; default Tokio settings are untouched.
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
