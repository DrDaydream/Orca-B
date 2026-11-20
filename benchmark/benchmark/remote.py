# Copyright(C) Facebook, Inc. and its affiliates.
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from itertools import zip_longest
import json
from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from invoke.exceptions import Failure
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os import environ, pathsep, access, X_OK
from pathlib import Path
from os.path import basename, splitext, expanduser, join
from shlex import quote
from time import sleep
from math import ceil
from copy import deepcopy
from datetime import datetime
from re import fullmatch, search
import subprocess
from time import monotonic, time
from uuid import uuid4

from benchmark.config import Committee, Key, NodeParameters, BenchParameters, ConfigError
from benchmark.utils import BenchError, Print, PathMaker, progress_bar
from benchmark.commands import CommandMaker
from benchmark.adversary import active_launch_ids, resolve_adversary, validate_adversary, write_run_snapshot
from benchmark.adversary_schedule import build_client_schedules, client_silence_slot_ms
from benchmark.logs import LogParser, ParseError
from benchmark.instance import InstanceManager
from benchmark import timing


class FabricError(Exception):
    ''' Wrapper for Fabric exception with a meaningfull error message. '''

    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[-1]
        super().__init__(message)


class ExecutionError(Exception):
    pass


class Bench:
    BUILD_MARKER = '.orca-build-commit'

    def __init__(self, ctx):
        self.manager = InstanceManager.make()
        self.settings = self.manager.settings
        try:
            ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                self.manager.settings.key_path
            )
            self.connect = ctx.connect_kwargs
        except (IOError, PasswordRequiredException, SSHException) as e:
            raise BenchError('Failed to load SSH key', e)

    def _check_stderr(self, output):
        if isinstance(output, dict):
            for x in output.values():
                if x.stderr:
                    raise ExecutionError(x.stderr)
        else:
            if output.stderr:
                raise ExecutionError(output.stderr)

    def install(self, hosts=None):
        hosts = self.manager.hosts(flat=True) if hosts is None else hosts
        if not hosts:
            raise BenchError('No testbed instances available', RuntimeError('Run fab info'))

        packages = (
            'build-essential cmake clang-14 libclang-14-dev pkg-config '
            'libssl-dev curl ca-certificates git tmux chrony'
        )
        setup = ' && '.join([
            'set -eo pipefail',
            'if command -v cloud-init >/dev/null 2>&1; then '
            'orca_cloud_status=0; sudo cloud-init status --wait || orca_cloud_status=$?; '
            'if [ "$orca_cloud_status" -ne 0 ] && [ "$orca_cloud_status" -ne 2 ]; '
            'then exit "$orca_cloud_status"; fi; fi',
            f'orca_missing=0; for orca_package in {packages}; do '
            'if [ "$(dpkg-query -W -f=\'${Status}\' "$orca_package" 2>/dev/null)" '
            '!= "install ok installed" ]; then orca_missing=1; fi; done; '
            'if [ "$orca_missing" -eq 1 ]; then sudo apt-get update && '
            'sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y '
            f'{packages}; fi',
            'export PATH="$HOME/.cargo/bin:$PATH"',
            'if ! command -v rustup >/dev/null 2>&1; then '
            'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | '
            'sh -s -- -y --profile minimal --default-toolchain 1.85.0; fi',
            'if ! rustup run 1.85.0 rustc --version >/dev/null 2>&1; then '
            'rustup toolchain install 1.85.0 --profile minimal; fi',
            'sudo systemctl enable --now chrony',
            'chronyc waitsync 60 0.1',
        ])
        repo = quote(self.settings.repo_name)
        url = quote(self.settings.repo_url)
        branch = quote(self.settings.branch)
        cmd = [
            setup,
            f'(test -d {repo}/.git || git clone --branch {branch} {url} {repo})',
            f'(cd {repo} && rustup override set 1.85.0)',
        ]
        try:
            self.revision = self._controller_revision()
            Print.info('Checking build dependencies on the controller...')
            subprocess.run(['bash', '-c', setup], check=True)
            environ['PATH'] = expanduser('~/.cargo/bin') + pathsep + environ.get('PATH', '')
            environ['RUSTUP_TOOLCHAIN'] = '1.85.0'
            for variable in (
                'RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS',
                'CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS',
                'TOKIO_WORKER_THREADS',
            ):
                environ.pop(variable, None)
            Print.info(f'Checking dependencies on {len(hosts)} testbed machines...')
            g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=False)
            self._update(hosts, collocate=True)
            self._build_local()
            Print.heading(f'Initialized testbed of {len(hosts)} nodes')
        except (GroupException, ExecutionError, subprocess.SubprocessError, OSError, SSHException) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to install repo on testbed', e)

    def kill(self, hosts=[], delete_logs=False):
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        hosts = hosts if hosts else self.manager.hosts(flat=True)
        if getattr(self, '_run_dir', None):
            # A run owns only its UUID directory and prefixed tmux sessions.
            # In particular, a second controller must not kill an existing run.
            prefix = f'orca-{self._run_id}-'
            if delete_logs:
                directory = quote(self._run_dir)
                config = quote(getattr(self, '_remote_config', None) or '.')
                cmd = (f'orca_origin="$PWD"; orca_config="$PWD"/{config}; '
                       f'mkdir -p {quote(str(Path(self._run_dir).parent).replace(chr(92), "/"))} && '
                       f'mkdir {directory} && mkdir {directory}/logs && '
                       f'cd {directory} && ln -s "$orca_origin/node" node && '
                       'ln -s "$orca_origin/benchmark_client" benchmark_client && '
                       'cp "$orca_config"/.*.json .')
            else:
                directory = quote(self._run_dir)
                cmd = ("(tmux list-sessions -F '#{session_name}' 2>/dev/null || true) | "
                       'while IFS= read -r orca_session; do case "$orca_session" in '
                       f'{quote(prefix)}*) tmux kill-session -t "=$orca_session" || true;; esac; done; '
                       # Account for a child still exiting after tmux drops the session.
                       f'for orca_attempt in 1 2 3 4 5; do '
                       f'orca_running=0; for orca_cwd in /proc/[0-9]*/cwd; do '
                       f'if test "$orca_cwd" -ef {directory}; then orca_running=1; break; fi; done; '
                       'if test "$orca_running" = 0; then exit 0; fi; sleep 1; done; '
                       'echo "Processes are still using this benchmark directory" >&2; exit 1')
            try:
                Group(*hosts, user='ubuntu', connect_kwargs=self.connect,
                      connect_timeout=10).run(cmd, hide=True, timeout=15)
            except GroupException as error:
                raise BenchError('Failed to prepare/stop this benchmark run', FabricError(error))
            return
        delete_logs = CommandMaker.clean_logs() if delete_logs else 'true'
        cmd = [delete_logs, f'({CommandMaker.kill()} || true)']
        try:
            g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
        except GroupException as e:
            raise BenchError('Failed to kill nodes', FabricError(e))

    def _select_hosts(self, bench_parameters):
        # Collocate the primary and its workers on the same machine.
        if bench_parameters.collocate:
            nodes = max(bench_parameters.nodes)

            # Ensure there are enough hosts.
            hosts = self.manager.hosts()
            if sum(len(x) for x in hosts.values()) < nodes:
                return []

            # Select the hosts in different data centers.
            ordered = [ip for row in zip_longest(*hosts.values()) for ip in row if ip is not None]
            return ordered[:nodes]

        # Spawn the primary and each worker on a different machine. Each
        # authority runs in a single data center.
        else:
            primaries = max(bench_parameters.nodes)

            # Ensure there are enough hosts.
            hosts = self.manager.hosts()
            if len(hosts.keys()) < primaries:
                return []
            for ips in hosts.values():
                if len(ips) < bench_parameters.workers + 1:
                    return []

            # Ensure the primary and its workers are in the same region.
            selected = []
            for region in list(hosts.keys())[:primaries]:
                ips = list(hosts[region])[:bench_parameters.workers + 1]
                selected.append(ips)
            return selected

    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        script = f'{command} |& tee {quote(log_file)}'
        if getattr(self, '_run_dir', None):
            name = f'orca-{self._run_id}-{name}'
            script = f'cd {quote(self._run_dir)} || exit 1; set -o pipefail; {script}'
        cmd = f'tmux new -d -s {quote(name)} {quote(script)}'
        c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
        output = c.run(cmd, hide=True)
        self._check_stderr(output)

    @staticmethod
    def _controller_revision():
        # Benchmark parameters and local settings may be edited between runs;
        # deployed native code must still match the controller's HEAD.
        root = join(PathMaker.node_crate_path(), '..')
        result = subprocess.run(
            ['git', 'diff', '--quiet', 'HEAD', '--', '*.rs', 'Cargo.toml',
             '**/Cargo.toml', 'Cargo.lock', '.cargo', 'rust-toolchain',
             'rust-toolchain.toml'], cwd=root,
        )
        if result.returncode:
            raise ExecutionError(
                'Controller Rust sources or build configuration differ from HEAD; '
                'commit or revert them before running fab remote'
            )
        return subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'], cwd=root, text=True,
        ).strip()

    def _update(self, hosts, collocate):
        if collocate:
            ips = list(set(hosts))
        else:
            ips = list(set([x for y in hosts for x in y]))

        revision = self.revision
        repo = quote(self.settings.repo_name)
        branch = quote(self.settings.branch)
        Print.info(f'Building commit {revision} on {len(ips)} machines...')
        cmd = [
            'source "$HOME/.cargo/env"',
            'export RUSTUP_TOOLCHAIN=1.85.0',
            'unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS '
            'CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS TOKIO_WORKER_THREADS',
            f'(cd {repo} '
            '&& (git diff --quiet && git diff --cached --quiet || { '
            'echo "Testbed repository has uncommitted tracked changes; '
            'save or revert them before running fab install." >&2; exit 1; }) '
            f'&& git fetch origin {branch} '
            f'&& git checkout --detach {revision} '
            f'&& test "$(git rev-parse HEAD)" = {revision} '
            f'&& cd node && bash -c {quote(self._build_command())})',
            f'ln -sfn {quote("./" + self.settings.repo_name + "/target/release/node")} ./node',
            f'ln -sfn {quote("./" + self.settings.repo_name + "/target/release/benchmark_client")} ./benchmark_client',
        ]
        with Group(*ips, user='ubuntu', connect_kwargs=self.connect) as group:
            group.run(' && '.join(cmd), hide=False)

    def _build_command(self):
        # Only fab install invokes this; clear success before any rebuild.
        marker = quote(f'../target/release/{self.BUILD_MARKER}')
        return '\n'.join([
            'set -euo pipefail',
            'export PATH="$HOME/.cargo/bin:$PATH"',
            'export RUSTUP_TOOLCHAIN=1.85.0',
            'unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS '
            'CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS TOKIO_WORKER_THREADS',
            f'rm -f -- {marker}',
            CommandMaker.compile(),
            'test -x ../target/release/node',
            'test -x ../target/release/benchmark_client',
            f'printf "%s\\n" {quote(self.revision)} > {marker}',
        ])

    def _build_local(self):
        Print.info(f'Building controller commit {self.revision}...')
        if self._controller_revision() != self.revision:
            raise ExecutionError('Controller commit changed; rerun fab install')
        subprocess.run(['bash', '-c', self._build_command()], check=True,
                       cwd=PathMaker.node_crate_path())
        try:
            if self._controller_revision() != self.revision:
                raise ExecutionError('Controller commit changed during compilation; rerun fab install')
        except (BenchError, ExecutionError, subprocess.SubprocessError, OSError):
            Path(PathMaker.binary_path(), self.BUILD_MARKER).unlink(missing_ok=True)
            raise

    def _binary_check_command(self):
        release = quote(f'{self.settings.repo_name}/target/release')
        return ' && '.join([
            f'test "$(cat {release}/{self.BUILD_MARKER} 2>/dev/null)" = {quote(self.revision)}',
            'test -x ./node',
            'test -x ./benchmark_client',
            f'test ./node -ef {release}/node',
            f'test ./benchmark_client -ef {release}/benchmark_client',
        ])

    def _check_binaries(self, hosts):
        release = Path(PathMaker.binary_path())
        try:
            actual = (release / self.BUILD_MARKER).read_text().strip()
            if actual != self.revision:
                raise ValueError(f'Controller binaries: {actual}; expected {self.revision}')
            for name in ('node', 'benchmark_client'):
                binary = release / name
                if not binary.is_file() or not access(binary, X_OK):
                    raise FileNotFoundError(f'Missing executable: {binary}')
        except (OSError, ValueError) as error:
            raise BenchError('Controller build is missing or outdated; run fab install first', error)
        Print.info(f'Using installed commit {self.revision}; checking {len(hosts)} machines...')
        try:
            with Group(*hosts, user='ubuntu', connect_kwargs=self.connect,
                       connect_timeout=15) as group:
                group.run(self._binary_check_command(), hide=True)
        except GroupException as error:
            raise BenchError('Remote build is missing, outdated or unreachable; run fab install first',
                             FabricError(error))

    def _config(self, hosts, node_parameters, bench_parameters):
        Print.info('Generating configuration files...')
        if self._controller_revision() != self.revision:
            raise ExecutionError('Controller commit changed; run fab install before retrying')

        # Cleanup all local configuration files.
        cmd = CommandMaker.cleanup()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Reuse binaries compiled by fab install.
        cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
        subprocess.run([cmd], shell=True, check=True)

        # Generate configuration files.
        keys = []
        key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
        for filename in key_files:
            cmd = CommandMaker.generate_key(filename).split()
            subprocess.run(cmd, check=True)
            keys += [Key.from_file(filename)]

        names = [x.name for x in keys]

        if bench_parameters.collocate:
            workers = bench_parameters.workers
            addresses = OrderedDict(
                (x, [y] * (workers + 1)) for x, y in zip(names, hosts)
            )
        else:
            addresses = OrderedDict(
                (x, y) for x, y in zip(names, hosts)
            )
        committee = Committee(addresses, self.settings.base_port)
        committee.print(PathMaker.committee_file())

        node_parameters.print(PathMaker.parameters_file())

        # Cleanup all nodes and upload configuration files.
        progress = progress_bar(names, prefix='Uploading config files:')
        for i, name in enumerate(progress):
            for ip in committee.ips(name):
                c = Connection(ip, user='ubuntu', connect_kwargs=self.connect)
                destination = getattr(self, '_remote_config', None)
                if destination:
                    c.run(f'mkdir -p {quote(destination)}', hide=True, timeout=15)
                else:
                    c.run(f'{CommandMaker.cleanup()} || true', hide=True)
                c.put(PathMaker.committee_file(), destination or '.')
                c.put(PathMaker.key_file(i), destination or '.')
                c.put(PathMaker.parameters_file(), destination or '.')

        return committee

    def _read_clock_logs(self, timeout):
        grouped = {}
        for identity in self._clock_clients + self._clock_primaries + self._clock_workers:
            grouped.setdefault(identity[0], []).append(identity)
        def read_host(item):
            host, identities = item
            commands = [f'cd {quote(self._run_dir)} || exit 1']
            for index, (_, logfile) in enumerate(identities):
                session = f'orca-{self._run_id}-{splitext(basename(logfile))[0]}'
                commands += [
                    f'printf "%s\\n" __ORCA_LOG_{index}__',
                    f'tmux has-session -t {quote("=" + session)} 2>/dev/null || '
                    'printf "%s\\n" "ERROR benchmark process exited"',
                    f'if test -f {quote(logfile)}; then '
                    "grep -E 'Benchmark clock|Benchmark input|Primary.*booted on|ERROR|panicked at|address already in use' "
                    f'{quote(logfile)} || true; fi',
                ]
            try:
                with Connection(host, user='ubuntu', connect_kwargs=self.connect,
                                connect_timeout=timeout) as connection:
                    output = connection.run('\n'.join(commands), hide=True, timeout=timeout)
            except (Failure, SSHException, OSError) as error:
                raise ExecutionError(f'Failed to read benchmark process/log status on {host}') from error
            logs = {}
            for index, identity in enumerate(identities):
                marker = f'__ORCA_LOG_{index}__\n'
                if marker not in output.stdout:
                    raise ExecutionError(f'Missing process/log status for {identity}')
                logs[identity] = output.stdout.split(marker, 1)[1].split('__ORCA_LOG_', 1)[0]
            return logs
        logs = {}
        with ThreadPoolExecutor(max_workers=min(32, len(grouped))) as executor:
            for result in executor.map(read_host, grouped.items()):
                logs.update(result)
        return logs

    def _publish_clock(self, plan):
        self._measurement_window = plan
        timing.save_plan(plan, getattr(self, '_collection_directory', None) or PathMaker.logs_path(),
                         PathMaker.results_path(), getattr(self, '_snapshot', None))
        encoded = json.dumps(plan, sort_keys=True, separators=(',', ':')).encode()
        def publish(host):
            temporary = f'{timing.START_PLAN}.{self._run_id}.tmp'
            try:
                with Connection(host, user='ubuntu', connect_kwargs=self.connect,
                                connect_timeout=10) as connection:
                    # Fabric's SFTP transfer uses the same bounded socket as SSH.
                    connection.open()
                    connection.sftp().get_channel().settimeout(10)
                    connection.put(BytesIO(encoded), remote=f'{self._run_dir}/{temporary}')
                    connection.run(
                        f'cd {quote(self._run_dir)} && test ! -e {timing.START_PLAN} && '
                        f'mv -- {quote(temporary)} {timing.START_PLAN}', hide=True, timeout=10,
                    )
            except (Failure, SSHException, OSError) as error:
                raise ExecutionError(f'Failed to publish benchmark plan on {host}') from error
        hosts = list(dict.fromkeys(host for host, _ in self._clock_clients))
        with ThreadPoolExecutor(max_workers=min(32, len(hosts))) as executor:
            list(executor.map(publish, hosts))

    def _wait_for_clients(self, clients, duration):
        """Keep nodes alive for the complete window used by comparison_metrics."""
        if getattr(self, '_clock_enabled', False) is True:
            hosts = sorted({host for host, _ in clients + self._clock_primaries})
            self._check_clock_sync(hosts)
            try:
                return timing.coordinate(
                    clients, self._clock_primaries, self._read_clock_logs, self._publish_clock,
                    duration=duration, warmup=self._clock_settings.warmup,
                    drain=self._clock_settings.drain, run_id=self._run_id,
                )
            except timing.ClockError as error:
                raise ExecutionError(str(error)) from error
        if not clients:
            raise ExecutionError('No benchmark clients were launched')
        timeout = monotonic() + 120
        starts = {}
        while len(starts) < len(clients):
            for host, logfile in clients:
                identity = (host, logfile)
                if identity in starts:
                    continue
                remaining = timeout - monotonic()
                if remaining <= 0:
                    raise ExecutionError('Clients did not start within 120 seconds')
                try:
                    with Connection(host, user='ubuntu', connect_kwargs=self.connect,
                                    connect_timeout=min(10, remaining)) as connection:
                        output = connection.run(
                            f'if test -f {quote(logfile)}; then cat {quote(logfile)}; fi',
                            hide=True, timeout=min(10, remaining),
                        )
                except (Failure, SSHException, OSError) as e:
                    raise ExecutionError(f'Failed to read client startup log {logfile}') from e
                match = search(r'(\d{4}-\S+Z)[^\n]* Start ', output.stdout)
                if match:
                    starts[identity] = datetime.fromisoformat(
                        match[1].replace('Z', '+00:00')
                    ).timestamp()
            if monotonic() >= timeout:
                raise ExecutionError('Clients did not start within 120 seconds')
            if len(starts) < len(clients):
                sleep(0.1)
        deadline = min(starts.values()) + duration
        if max(starts.values()) >= deadline:
            raise ExecutionError('Client startup skew exceeds the measurement window')
        while time() < deadline:
            sleep(min(0.1, max(0, deadline - time())))

    def _check_clock_sync(self, hosts):
        command = ['chronyc', 'waitsync', '10', '0.1', '0', '1']
        try:
            subprocess.run(command, check=True, timeout=15, capture_output=True, text=True)
            with Group(*hosts, user='ubuntu', connect_kwargs=self.connect,
                       connect_timeout=10) as group:
                group.run(' '.join(command), hide=True, timeout=15)
        except (OSError, subprocess.SubprocessError, GroupException) as error:
            raise ExecutionError('UTC clocks are not synchronized within 100 ms; check chrony') from error

    def _begin_clock_collection(self):
        target = environ.get('ORCA_BENCHMARK_LOGS_DIRECTORY')
        self._collection_directory = Path(target or PathMaker.logs_path())
        if target is None:
            subprocess.run(CommandMaker.clean_logs(), shell=True, check=True,
                           stderr=subprocess.DEVNULL)
        self._collection_directory.mkdir(parents=True, exist_ok=True)
        if any(self._collection_directory.iterdir()):
            raise ExecutionError(f'Attempt log directory is not empty: {self._collection_directory}')

    @staticmethod
    def _log_identities(committee, active):
        for i, addresses in enumerate(committee.workers_addresses(0)):
            if i in active:
                for worker, address in addresses:
                    host = Committee.ip(address)
                    yield host, PathMaker.client_log_file(i, worker)
                    yield host, PathMaker.worker_log_file(i, worker)
        for i, address in enumerate(committee.primary_addresses(0)):
            if i in active:
                yield Committee.ip(address), PathMaker.primary_log_file(i)

    def _collect_clock_logs(self):
        identities = {}
        for host, logfile in self._expected_clock_logs:
            identities.setdefault(host, []).append(logfile)

        def collect_host(item):
            host, logfiles = item
            pending = list(logfiles)
            errors = {}
            try:
                with Connection(host, user='ubuntu', connect_kwargs=self.connect,
                                connect_timeout=10) as connection:
                    connection.open()
                    connection.sftp().get_channel().settimeout(10)
                    for logfile in logfiles:
                        target = self._collection_directory / basename(logfile)
                        partial = target.with_suffix('.log.partial')
                        try:
                            connection.get(self._remote_log(logfile), local=str(partial))
                            partial.replace(target)
                        except (Failure, SSHException, OSError) as error:
                            errors[logfile] = f'{host}: {error}'
                        pending.remove(logfile)
            except (Failure, SSHException, OSError) as error:
                errors.update((logfile, f'{host}: {error}') for logfile in pending)
            return errors

        errors = {}
        with ThreadPoolExecutor(max_workers=min(32, len(identities))) as executor:
            for result in executor.map(collect_host, identities.items()):
                errors.update(result)
        return errors

    def _release_run_storage(self, hosts, retain_logs):
        if not fullmatch(r'\.orca-benchmark/[0-9a-f]{32}/[0-9a-f]{32}', self._run_dir):
            raise ExecutionError(f'Unexpected remote benchmark directory: {self._run_dir}')
        directory = quote(self._run_dir)
        if retain_logs:
            command = (f'if test -d {directory}; then find {directory} -mindepth 1 -maxdepth 1 '
                       '! -name logs -exec rm -rf -- {} +; fi')
        else:
            command = f'rm -rf -- {directory}'
        with Group(*hosts, user='ubuntu', connect_kwargs=self.connect,
                   connect_timeout=10) as group:
            group.run(command, hide=True, timeout=30)

    def _finish_clock_run(self, hosts, failure):
        issues = {}
        try:
            self.kill(hosts=hosts, delete_logs=False)
        except Exception as error:
            issues['stop'] = str(error)
        try:
            missing = self._collect_clock_logs()
            if missing:
                issues['logs'] = missing
        except Exception as error:
            issues['logs'] = str(error)
        if 'stop' not in issues:
            try:
                self._release_run_storage(hosts, retain_logs='logs' in issues)
            except Exception as error:
                issues['cleanup'] = str(error)
        if issues:
            detail = {'run_id': self._run_id, 'remote_directory': self._run_dir, **issues}
            (self._collection_directory / 'collection-issues.json').write_text(
                json.dumps(detail, indent=2, sort_keys=True) + '\n', encoding='utf-8')
            error = ExecutionError(f'Benchmark log collection or teardown failed for {self._run_dir}: {issues}')
            if failure is None:
                raise error
            Print.warn(str(error))

    def _run_single(self, rate, committee, bench_parameters, node_parameters, debug=False,
                    adversary_config=None):
        self._run_started = False
        failure = None
        try:
            return self._run_single_inner(rate, committee, bench_parameters, node_parameters,
                                          debug, adversary_config)
        except BaseException as error:
            failure = error
            raise
        finally:
            if self._run_started:
                try:
                    if self._collection_directory is not None:
                        self._finish_clock_run(committee.ips(), failure)
                    else:
                        self.kill(hosts=committee.ips(), delete_logs=False)
                except Exception as error:
                    if failure is None:
                        raise
                    Print.warn(f'Failed to stop benchmark run {self._run_id}: {error}')
                finally:
                    self._run_started = False

    def _run_single_inner(self, rate, committee, bench_parameters, node_parameters, debug=False,
                          adversary_config=None):
        faults = bench_parameters.faults
        active = set(active_launch_ids(committee.size(), adversary_config))
        self._clock_enabled = bench_parameters.common_clock
        self._clock_settings = bench_parameters
        self._measurement_window = None
        self._clock_clients, self._clock_primaries, self._clock_workers = [], [], []
        self._run_id = uuid4().hex
        self._run_dir = (f'{getattr(self, "_remote_config", ".orca-benchmark")}/{self._run_id}'
                         if self._clock_enabled else None)
        self._collection_directory = None
        if self._clock_enabled and getattr(self, '_remote_config', None):
            self._begin_clock_collection()
            self._expected_clock_logs = list(self._log_identities(committee, active))
        self._run_started = True

        # Kill any potentially unfinished run and delete logs.
        hosts = committee.ips()
        self.kill(hosts=hosts, delete_logs=True)

        # Retain the original per-worker rate; A1 omits clients on crashed authorities.
        Print.info('Booting clients...')
        workers_addresses = {i: rows for i, rows in enumerate(committee.workers_addresses(0))
                             if i in active}
        rate_share = ceil(rate / committee.workers())
        names = list(committee.json['authorities'])
        if adversary_config is None:
            silence_slot_ms = client_silence_slot_ms(
                node_parameters.json['max_header_delay']
            )
            silence_schedules = build_client_schedules(
                names, faults, bench_parameters.warmup + bench_parameters.duration, silence_slot_ms
            )
        else:
            silence_slot_ms = 0
            silence_schedules = dict.fromkeys(names, '')
        clients = []
        for i, addresses in workers_addresses.items():
            for (id, address) in addresses:
                host = Committee.ip(address)
                cmd = CommandMaker.run_client(
                    address,
                    bench_parameters.tx_size,
                    rate_share,
                    [x for y in workers_addresses.values() for _, x in y],
                    silence_schedules[names[i]],
                    silence_slot_ms,
                    start_plan=timing.START_PLAN if self._clock_enabled else None,
                )
                log_file = PathMaker.client_log_file(i, id)
                self._background_run(host, cmd, log_file)
                clients.append((host, log_file))
        self._clock_clients = clients

        # Start only authorities included in the launch plan.
        Print.info('Booting primaries...')
        for i, address in enumerate(committee.primary_addresses(0)):
            if i not in active:
                continue
            host = Committee.ip(address)
            cmd = CommandMaker.run_primary(
                PathMaker.key_file(i),
                PathMaker.committee_file(),
                PathMaker.db_path(i),
                PathMaker.parameters_file(),
                debug=debug,
                faults=faults,
                adversary_config=adversary_config,
            )
            log_file = PathMaker.primary_log_file(i)
            self._background_run(host, cmd, log_file)
            self._clock_primaries.append((host, log_file))

        # A1 omits workers together with their primary.
        Print.info('Booting workers...')
        for i, addresses in workers_addresses.items():
            for (id, address) in addresses:
                host = Committee.ip(address)
                cmd = CommandMaker.run_worker(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i, id),
                    PathMaker.parameters_file(),
                    id,  # The worker's id.
                    debug=debug
                )
                log_file = PathMaker.worker_log_file(i, id)
                self._background_run(host, cmd, log_file)
                self._clock_workers.append((host, log_file))

        # Use the same Start event and fixed cutoff as comparison_metrics.
        duration = bench_parameters.duration
        Print.info(f'Running benchmark ({duration} sec)...')
        self._wait_for_clients(clients, duration)

    def _logs(self, committee, faults, adversary=None):
        collection = getattr(self, '_collection_directory', None)
        if collection is not None:
            Print.info('Parsing logs and computing performance...')
            return LogParser.process(str(collection), faults=faults, adversary=adversary,
                                     committee_size=committee.size())
        active = set(active_launch_ids(committee.size(), adversary))
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
        if getattr(self, '_measurement_window', None):
            timing.write_plan(Path(PathMaker.logs_path()) / timing.MEASUREMENT_WINDOW,
                              self._measurement_window)

        # Download log files.
        workers_addresses = [(i, rows) for i, rows in enumerate(committee.workers_addresses(0))
                             if i in active]
        progress = progress_bar(workers_addresses, prefix='Downloading workers logs:')
        for i, addresses in progress:
            for id, address in addresses:
                host = Committee.ip(address)
                c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
                c.get(
                    self._remote_log(PathMaker.client_log_file(i, id)),
                    local=PathMaker.client_log_file(i, id)
                )
                c.get(
                    self._remote_log(PathMaker.worker_log_file(i, id)),
                    local=PathMaker.worker_log_file(i, id)
                )

        primary_addresses = [(i, address) for i, address in enumerate(committee.primary_addresses(0))
                             if i in active]
        progress = progress_bar(primary_addresses, prefix='Downloading primaries logs:')
        for i, address in progress:
            host = Committee.ip(address)
            c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
            c.get(
                self._remote_log(PathMaker.primary_log_file(i)),
                local=PathMaker.primary_log_file(i)
            )

        # Parse logs and return the parser.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process(PathMaker.logs_path(), faults=faults, adversary=adversary, committee_size=committee.size())

    def _remote_log(self, filename):
        directory = getattr(self, '_run_dir', None)
        return f'{directory}/{filename}' if directory else filename

    def run(self, bench_parameters_dict, node_parameters_dict, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting remote benchmark')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
            adversary = validate_adversary(
                bench_parameters_dict.get('adversary'), 'orca',
                bench_parameters.nodes, bench_parameters.faults,
            )
        except (ConfigError, ValueError) as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        # Select which hosts to use.
        selected_hosts = self._select_hosts(bench_parameters)
        if len(selected_hosts) != max(bench_parameters.nodes):
            raise BenchError('There are not enough instances available', RuntimeError('Run fab info'))

        try:
            self.revision = self._controller_revision()
        except (ExecutionError, subprocess.SubprocessError, OSError) as e:
            raise BenchError('Controller code is not ready for remote benchmarking', e)

        ips = selected_hosts if bench_parameters.collocate else [
            ip for group in selected_hosts for ip in group
        ]
        self._check_binaries(list(dict.fromkeys(ips)))

        self._remote_config = f'.orca-benchmark/{uuid4().hex}' if bench_parameters.common_clock else None
        failure = None
        try:
            self._run_matrix(selected_hosts, bench_parameters, node_parameters,
                             bench_parameters_dict, node_parameters_dict, adversary, debug)
        except BaseException as error:
            failure = error
            raise
        finally:
            if self._remote_config:
                try:
                    self._release_config_storage(list(dict.fromkeys(ips)))
                except Exception as error:
                    if failure is None:
                        raise BenchError('Failed to remove uploaded benchmark configuration', error) from error
                    Print.warn(f'Failed to remove uploaded benchmark configuration: {error}')

    def _release_config_storage(self, hosts):
        if not fullmatch(r'\.orca-benchmark/[0-9a-f]{32}', self._remote_config):
            raise ExecutionError(f'Unexpected remote config directory: {self._remote_config}')
        directory = quote(self._remote_config)
        # Preserve child run logs retained for recovery.
        command = (f'if test -d {directory}; then '
                   f'rm -f -- {directory}/.committee.json {directory}/.parameters.json '
                   f'{directory}/.node-*.json && '
                   f'find {directory} -maxdepth 0 -type d -empty -delete; fi')
        with Group(*hosts, user='ubuntu', connect_kwargs=self.connect,
                   connect_timeout=10) as group:
            group.run(command, hide=True, timeout=15)

    def _run_matrix(self, selected_hosts, bench_parameters, node_parameters,
                    bench_parameters_dict, node_parameters_dict, adversary, debug):
        try:
            committee = self._config(
                selected_hosts, node_parameters, bench_parameters
            )
        except (subprocess.SubprocessError, GroupException, ExecutionError, OSError,
                Failure, SSHException) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to configure nodes', e)

        # Run benchmarks.
        for n in bench_parameters.nodes:
            committee_copy = deepcopy(committee)
            committee_copy.remove_nodes(committee.size() - n)
            adversary_config = resolve_adversary(adversary, committee_copy.json['authorities'])

            for r in bench_parameters.rate:
                Print.heading(f'\nRunning {n} nodes (input rate: {r:,} tx/s)')

                # Run the benchmark.
                for i in range(bench_parameters.runs):
                    Print.heading(f'Run {i+1}/{bench_parameters.runs}')
                    self._snapshot = None
                    try:
                        if adversary_config is not None:
                            snapshot = write_run_snapshot(
                                PathMaker.results_path(), bench_parameters_dict, node_parameters_dict,
                                committee_copy.json['authorities'], adversary_config,
                                n, r, i + 1, protocol='orca-b',
                            )
                            self._snapshot = snapshot
                            Print.info(
                                f"Adversary: {adversary_config['mode']}; fixed Byzantine IDs: "
                                f"{adversary['byzantine_ids']}; seed: {adversary_config['seed']}; "
                                f"parameters: {snapshot}"
                            )
                        self._run_single(
                            r, committee_copy, bench_parameters, node_parameters, debug,
                            adversary_config=adversary_config,
                        )

                        faults = bench_parameters.faults
                        logger = self._logs(committee_copy, faults, adversary=adversary)
                        print(logger.result())
                        result_file = PathMaker.result_file(
                            faults,
                            n, 
                            bench_parameters.workers,
                            bench_parameters.collocate,
                            r, 
                            bench_parameters.tx_size, 
                        )
                        logger.print(result_file)
                        logger.print_comparison(
                            bench_parameters.duration,
                            f'{result_file[:-4]}-run-{i + 1}.comparison.json',
                        )
                    except (OSError, subprocess.SubprocessError, GroupException,
                            Failure, SSHException, ExecutionError, ParseError) as e:
                        if not bench_parameters.common_clock:
                            try:
                                self.kill(hosts=committee_copy.ips())
                            except Exception as cleanup_error:
                                Print.warn(f'Failed to stop benchmark after error: {cleanup_error}')
                        if isinstance(e, GroupException):
                            e = FabricError(e)
                        raise BenchError('Benchmark failed', e) from e
