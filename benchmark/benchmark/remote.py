# Copyright(C) Facebook, Inc. and its affiliates.
from collections import OrderedDict
from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os import environ, pathsep, access, X_OK
from pathlib import Path
from os.path import basename, splitext, expanduser, join
from shlex import quote
from time import sleep
from math import ceil
from copy import deepcopy
import subprocess

from benchmark.config import Committee, Key, NodeParameters, BenchParameters, ConfigError
from benchmark.utils import BenchError, Print, PathMaker, progress_bar
from benchmark.commands import CommandMaker
from benchmark.adversary import active_launch_ids, resolve_adversary, validate_adversary, write_run_snapshot
from benchmark.adversary_schedule import build_client_schedules, client_silence_slot_ms
from benchmark.logs import LogParser, ParseError
from benchmark.instance import InstanceManager


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
            ordered = zip(*hosts.values())
            ordered = [x for y in ordered for x in y]
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
                c.run(f'{CommandMaker.cleanup()} || true', hide=True)
                c.put(PathMaker.committee_file(), '.')
                c.put(PathMaker.key_file(i), '.')
                c.put(PathMaker.parameters_file(), '.')

        return committee

    def _run_single(self, rate, committee, bench_parameters, node_parameters, debug=False,
                    adversary_config=None):
        faults = bench_parameters.faults
        active = set(active_launch_ids(committee.size(), adversary_config))

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
                names, faults, bench_parameters.duration, silence_slot_ms
            )
        else:
            silence_slot_ms = 0
            silence_schedules = dict.fromkeys(names, '')
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
                )
                log_file = PathMaker.client_log_file(i, id)
                self._background_run(host, cmd, log_file)

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

        # Wait for all transactions to be processed.
        duration = bench_parameters.duration
        for _ in progress_bar(range(20), prefix=f'Running benchmark ({duration} sec):'):
            sleep(ceil(duration / 20))
        self.kill(hosts=hosts, delete_logs=False)

    def _logs(self, committee, faults, adversary=None):
        active = set(active_launch_ids(committee.size(), adversary))
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Download log files.
        workers_addresses = [(i, rows) for i, rows in enumerate(committee.workers_addresses(0))
                             if i in active]
        progress = progress_bar(workers_addresses, prefix='Downloading workers logs:')
        for i, addresses in progress:
            for id, address in addresses:
                host = Committee.ip(address)
                c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
                c.get(
                    PathMaker.client_log_file(i, id), 
                    local=PathMaker.client_log_file(i, id)
                )
                c.get(
                    PathMaker.worker_log_file(i, id), 
                    local=PathMaker.worker_log_file(i, id)
                )

        primary_addresses = [(i, address) for i, address in enumerate(committee.primary_addresses(0))
                             if i in active]
        progress = progress_bar(primary_addresses, prefix='Downloading primaries logs:')
        for i, address in progress:
            host = Committee.ip(address)
            c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
            c.get(
                PathMaker.primary_log_file(i), 
                local=PathMaker.primary_log_file(i)
            )

        # Parse logs and return the parser.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process(PathMaker.logs_path(), faults=faults, adversary=adversary, committee_size=committee.size())

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

        # Upload all configuration files.
        try:
            committee = self._config(
                selected_hosts, node_parameters, bench_parameters
            )
        except (subprocess.SubprocessError, GroupException, ExecutionError, OSError) as e:
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
                    try:
                        if adversary_config is not None:
                            snapshot = write_run_snapshot(
                                PathMaker.results_path(), bench_parameters_dict, node_parameters_dict,
                                committee_copy.json['authorities'], adversary_config,
                                n, r, i + 1, protocol='orca-b',
                            )
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
                        logger.print(PathMaker.result_file(
                            faults,
                            n, 
                            bench_parameters.workers,
                            bench_parameters.collocate,
                            r, 
                            bench_parameters.tx_size, 
                        ))
                    except (subprocess.SubprocessError, GroupException, ParseError) as e:
                        self.kill(hosts=selected_hosts)
                        if isinstance(e, GroupException):
                            e = FabricError(e)
                        raise BenchError('Benchmark failed', e) from e
