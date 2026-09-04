# Copyright(C) Facebook, Inc. and its affiliates.
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os.path import basename, splitext
from shlex import quote
from threading import Event, Lock, Thread
from time import sleep
from math import ceil
from copy import deepcopy
import subprocess

from benchmark.config import Committee, Key, NodeParameters, BenchParameters, ConfigError
from benchmark.utils import BenchError, Print, PathMaker, progress_bar
from benchmark.commands import CommandMaker
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
    def __init__(self, ctx):
        self.manager = InstanceManager.make()
        self.settings = self.manager.settings
        try:
            ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                self.manager.settings.key_path
            )
            self.connect = ctx.connect_kwargs
            self._install_print_lock = Lock()
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

    def _print_install_status(self, host, step, status, output=None):
        prefix = f'[INSTALL][{host}][{step}]'
        lines = [f'{prefix} {status}']
        if output:
            for stream, content in output:
                for line in content.rstrip().splitlines():
                    lines.append(f'{prefix}[output] {stream}: {line}')
        with self._install_print_lock:
            print('\n'.join(lines), flush=True)

    def _print_install_progress(self, completed, total, failed):
        with self._install_print_lock:
            print(
                f'[INSTALL][PROGRESS] {completed}/{total} nodes finished '
                f'({failed} failed)',
                flush=True
            )

    def _install_steps(self):
        apt = (
            'sudo timeout --signal=TERM --kill-after=30s 30m '
            'env DEBIAN_FRONTEND=noninteractive '
            'APT_LISTCHANGES_FRONTEND=none NEEDRESTART_MODE=a apt-get '
            '-o Acquire::Retries=5 '
            '-o Acquire::ForceIPv4=true '
            '-o Acquire::http::Timeout=30 '
            '-o Acquire::https::Timeout=30 '
            '-o DPkg::Lock::Timeout=900 '
            '-o Dpkg::Use-Pty=0'
        )
        repo_name = quote(self.settings.repo_name)
        repo_url = quote(self.settings.repo_url)
        branch = quote(self.settings.branch)

        return [
            (
                'wait-cloud-init',
                'if command -v cloud-init >/dev/null 2>&1; then '
                "sudo bash -c 'timeout 900 cloud-init status --wait & "
                'pid=$!; elapsed=0; '
                'while kill -0 "$pid" 2>/dev/null; do '
                'echo "waiting ${elapsed}s for cloud-init"; '
                'sleep 10; elapsed=$((elapsed + 10)); done; '
                "wait \"$pid\"'; fi"
            ),
            (
                'wait-apt-locks',
                "sudo timeout 900 bash -c 'command -v fuser >/dev/null || "
                '{ echo "fuser is required to inspect apt locks" >&2; '
                'exit 1; }; elapsed=0; while fuser '
                '/var/lib/dpkg/lock-frontend /var/lib/dpkg/lock '
                '/var/cache/apt/archives/lock /var/lib/apt/lists/lock '
                '>/dev/null 2>&1; do pids=$(fuser '
                '/var/lib/dpkg/lock-frontend /var/lib/dpkg/lock '
                '/var/cache/apt/archives/lock /var/lib/apt/lists/lock '
                '2>/dev/null | xargs); '
                'echo "waiting ${elapsed}s for apt/dpkg locks; '
                'holders: ${pids:-unknown}"; sleep 10; '
                "elapsed=$((elapsed + 10)); done'"
            ),
            (
                'configure-pending-packages',
                'sudo timeout 900 env DEBIAN_FRONTEND=noninteractive '
                'APT_LISTCHANGES_FRONTEND=none NEEDRESTART_MODE=a '
                'dpkg --configure -a'
            ),
            ('apt-update', f'{apt} update'),
            (
                'install-build-dependencies',
                f'{apt} install -y build-essential cmake curl git '
                'software-properties-common'
            ),
            (
                'enable-universe',
                'sudo env DEBIAN_FRONTEND=noninteractive '
                'timeout --signal=TERM --kill-after=30s 5m '
                'add-apt-repository --yes --no-update universe'
            ),
            ('apt-update-universe', f'{apt} update'),
            (
                'install-clang',
                f'{apt} install -y clang-14 llvm-14 llvm-14-dev '
                'libclang-14-dev'
            ),
            (
                'configure-clang',
                'sudo update-alternatives --install /usr/bin/clang clang '
                '/usr/bin/clang-14 140 && '
                'sudo update-alternatives --install /usr/bin/clang++ clang++ '
                '/usr/bin/clang++-14 140 && '
                'sudo update-alternatives --set clang /usr/bin/clang-14 && '
                'sudo update-alternatives --set clang++ /usr/bin/clang++-14'
            ),
            (
                'install-rustup',
                'if [ -x "$HOME/.cargo/bin/rustup" ]; then '
                'echo "rustup already installed"; else '
                'installer=$(mktemp); '
                'trap \'rm -f "$installer"\' EXIT; '
                'curl --proto "=https" --tlsv1.2 --fail --show-error '
                '--silent --location --retry 5 --retry-delay 2 '
                '--retry-connrefused --retry-max-time 180 '
                '--connect-timeout 15 --max-time 300 '
                '--output "$installer" https://sh.rustup.rs && '
                'timeout 900 sh "$installer" -y; fi'
            ),
            (
                'configure-rust',
                'timeout 900 "$HOME/.cargo/bin/rustup" default stable'
            ),
            (
                'configure-build-environment',
                'touch "$HOME/.cargo/env" && '
                'if ! grep -q "LIBCLANG_PATH=/usr/lib/llvm-14/lib" '
                '"$HOME/.cargo/env"; then '
                "printf '%s\\n' '' "
                "'export PATH=/usr/lib/llvm-14/bin:$PATH' "
                "'export CC=/usr/bin/clang-14' "
                "'export CXX=/usr/bin/clang++-14' "
                "'export CLANG_PATH=/usr/bin/clang-14' "
                "'export LIBCLANG_PATH=/usr/lib/llvm-14/lib' "
                "'export CXXFLAGS=\"-include cstdint\"' "
                '>> "$HOME/.cargo/env"; fi'
            ),
            (
                'sync-repository',
                'retry() { attempt=1; while ! "$@"; do '
                'if [ "$attempt" -ge 3 ]; then '
                'echo "command failed after ${attempt} attempts" >&2; '
                'return 1; fi; '
                'echo "attempt ${attempt} failed; retrying" >&2; '
                'sleep $((attempt * 5)); attempt=$((attempt + 1)); done; }; '
                f'if [ -d {repo_name}/.git ]; then '
                'retry timeout 300 git '
                '-c http.lowSpeedLimit=1024 -c http.lowSpeedTime=60 '
                f'-C {repo_name} fetch --prune origin && '
                f'git -C {repo_name} checkout {branch} && '
                f'git -C {repo_name} merge --ff-only origin/{branch}; '
                f'elif [ -e {repo_name} ]; then '
                f'echo "{self.settings.repo_name} exists but is not a git repository" >&2; '
                'exit 1; else retry timeout 300 git '
                '-c http.lowSpeedLimit=1024 -c http.lowSpeedTime=60 '
                f'clone --branch {branch} --single-branch '
                f'{repo_url} {repo_name}; fi'
            )
        ]

    def _install_host(self, host, steps):
        connection = None
        try:
            for attempt in range(1, 6):
                self._print_install_status(
                    host, 'connect', f'START attempt={attempt}/5'
                )
                connection = Connection(
                    host,
                    user='ubuntu',
                    connect_kwargs=self.connect,
                    connect_timeout=30
                )
                try:
                    connection.open()
                    self._print_install_status(
                        host, 'connect', f'OK attempt={attempt}/5'
                    )
                    break
                except Exception as e:
                    message = str(e).strip()
                    reason = (
                        message.splitlines()[0]
                        if message else type(e).__name__
                    )
                    if attempt == 5:
                        self._print_install_status(
                            host,
                            'connect',
                            'ERROR attempts=5',
                            [('exception', str(e))]
                        )
                        return host, 'connect', reason

                    delay = attempt * 5
                    self._print_install_status(
                        host,
                        'connect',
                        f'RETRY attempt={attempt}/5 delay={delay}s',
                        [('exception', str(e))]
                    )
                    connection.close()
                    connection = None
                    sleep(delay)

            for step, command in steps:
                self._print_install_status(host, step, 'START')
                heartbeat_stop = Event()

                def heartbeat():
                    elapsed = 0
                    while not heartbeat_stop.wait(30):
                        elapsed += 30
                        self._print_install_status(
                            host, step, f'RUNNING elapsed={elapsed}s'
                        )

                heartbeat_thread = Thread(target=heartbeat, daemon=True)
                heartbeat_thread.start()
                try:
                    output_prefix = (
                        f'[INSTALL][{host}][{step}][output] '
                    )
                    script = (
                        'set -o pipefail; '
                        f'{{ {command}; }} 2>&1 | '
                        f"sed -u 's/^/{output_prefix}/'"
                    )
                    result = connection.run(
                        f'bash -lc {quote(script)}',
                        hide=False,
                        in_stream=False,
                        warn=True
                    )
                except Exception as e:
                    result = getattr(e, 'result', None)
                    output = []
                    output.append(('exception', str(e)))
                    self._print_install_status(
                        host, step, 'ERROR', output
                    )
                    message = str(e).strip()
                    reason = (
                        message.splitlines()[0]
                        if message else type(e).__name__
                    )
                    return host, step, reason
                finally:
                    heartbeat_stop.set()
                    heartbeat_thread.join()

                if result.ok:
                    self._print_install_status(host, step, 'OK')
                    continue

                output_lines = [
                    line for line in (result.stdout or '').splitlines()
                    if line.strip()
                ]
                output_tail = ' | '.join(output_lines[-8:])
                reason = f'exit status {result.exited}'
                if output_tail:
                    reason += f'; last output: {output_tail}'
                self._print_install_status(
                    host, step, f'ERROR exit={result.exited}'
                )
                return host, step, reason
        finally:
            if connection is not None:
                connection.close()

        self._print_install_status(host, 'complete', 'OK')
        return None

    def install(self):
        hosts = self.manager.hosts(flat=True)
        if not hosts:
            raise BenchError(
                'Failed to install repo on testbed',
                ExecutionError('No active testbed nodes found')
            )

        Print.info(f'Installing dependencies and repository on {len(hosts)} nodes...')
        steps = self._install_steps()
        failures = []
        completed = 0
        with ThreadPoolExecutor(max_workers=len(hosts)) as executor:
            futures = {
                executor.submit(self._install_host, host, steps): host
                for host in hosts
            }
            for future in as_completed(futures):
                host = futures[future]
                try:
                    failure = future.result()
                except Exception as e:
                    message = str(e).strip()
                    reason = (
                        message.splitlines()[0]
                        if message else type(e).__name__
                    )
                    self._print_install_status(
                        host, 'internal', 'ERROR', [('exception', str(e))]
                    )
                    failure = host, 'internal', reason
                if failure:
                    failures.append(failure)
                completed += 1
                self._print_install_progress(
                    completed, len(hosts), len(failures)
                )

        if failures:
            order = {host: i for i, host in enumerate(hosts)}
            failures.sort(key=lambda failure: order[failure[0]])
            summary = [
                f'Installation failed on {len(failures)} of {len(hosts)} nodes:'
            ]
            summary += [
                f'  {host}: {step} ({reason})'
                for host, step, reason in failures
            ]
            Print.info(
                '\n'.join(f'[INSTALL][SUMMARY] {line}' for line in summary)
            )
            raise BenchError(
                'Failed to install repo on testbed',
                ExecutionError('\n'.join(summary))
            )

        Print.heading(f'Initialized testbed of {len(hosts)} nodes')

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
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
        output = c.run(cmd, hide=True)
        self._check_stderr(output)

    def _update(self, hosts, collocate):
        if collocate:
            ips = list(set(hosts))
        else:
            ips = list(set([x for y in hosts for x in y]))

        Print.info(
            f'Updating {len(ips)} machines (branch "{self.settings.branch}")...'
        )
        cmd = [
            f'(cd {self.settings.repo_name} && git fetch -f)',
            f'(cd {self.settings.repo_name} && git checkout -f {self.settings.branch})',
            f'(cd {self.settings.repo_name} && git pull -f)',
            'source $HOME/.cargo/env',
            f'(cd {self.settings.repo_name}/node && {CommandMaker.compile()})',
            CommandMaker.alias_binaries(
                f'./{self.settings.repo_name}/target/release/'
            )
        ]
        g = Group(*ips, user='ubuntu', connect_kwargs=self.connect)
        g.run(' && '.join(cmd), hide=True)

    def _config(self, hosts, node_parameters, bench_parameters):
        Print.info('Generating configuration files...')

        # Cleanup all local configuration files.
        cmd = CommandMaker.cleanup()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Recompile the latest code.
        cmd = CommandMaker.compile().split()
        subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

        # Create alias for the client and nodes binary.
        cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
        subprocess.run([cmd], shell=True)

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

    def _run_single(self, rate, committee, bench_parameters, node_parameters, debug=False):
        faults = bench_parameters.faults

        # Kill any potentially unfinished run and delete logs.
        hosts = committee.ips()
        self.kill(hosts=hosts, delete_logs=True)

        # Run clients for every worker; dynamically silent workers apply backpressure.
        Print.info('Booting clients...')
        workers_addresses = committee.workers_addresses(0)
        rate_share = ceil(rate / committee.workers())
        names = list(committee.json['authorities'])
        silence_slot_ms = client_silence_slot_ms(
            node_parameters.json['max_header_delay']
        )
        silence_schedules = build_client_schedules(
            names, faults, bench_parameters.duration, silence_slot_ms
        )
        for i, addresses in enumerate(workers_addresses):
            for (id, address) in addresses:
                host = Committee.ip(address)
                cmd = CommandMaker.run_client(
                    address,
                    bench_parameters.tx_size,
                    rate_share,
                    [x for y in workers_addresses for _, x in y],
                    silence_schedules[names[i]],
                    silence_slot_ms,
                )
                log_file = PathMaker.client_log_file(i, id)
                self._background_run(host, cmd, log_file)

        # Run every primary; adversarial authorities are selected per round.
        Print.info('Booting primaries...')
        for i, address in enumerate(committee.primary_addresses(0)):
            host = Committee.ip(address)
            cmd = CommandMaker.run_primary(
                PathMaker.key_file(i),
                PathMaker.committee_file(),
                PathMaker.db_path(i),
                PathMaker.parameters_file(),
                debug=debug,
                faults=faults
            )
            log_file = PathMaker.primary_log_file(i)
            self._background_run(host, cmd, log_file)

        # Run every worker; batch production is paused dynamically.
        Print.info('Booting workers...')
        for i, addresses in enumerate(workers_addresses):
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

    def _logs(self, committee, faults):
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Download log files.
        workers_addresses = committee.workers_addresses(0)
        progress = progress_bar(workers_addresses, prefix='Downloading workers logs:')
        for i, addresses in enumerate(progress):
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

        primary_addresses = committee.primary_addresses(0)
        progress = progress_bar(primary_addresses, prefix='Downloading primaries logs:')
        for i, address in enumerate(progress):
            host = Committee.ip(address)
            c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
            c.get(
                PathMaker.primary_log_file(i), 
                local=PathMaker.primary_log_file(i)
            )

        # Parse logs and return the parser.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process(PathMaker.logs_path(), faults=faults)

    def run(self, bench_parameters_dict, node_parameters_dict, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting remote benchmark')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        # Select which hosts to use.
        selected_hosts = self._select_hosts(bench_parameters)
        if not selected_hosts:
            Print.warn('There are not enough instances available')
            return

        # Update nodes.
        try:
            self._update(selected_hosts, bench_parameters.collocate)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to update nodes', e)

        # Upload all configuration files.
        try:
            committee = self._config(
                selected_hosts, node_parameters, bench_parameters
            )
        except (subprocess.SubprocessError, GroupException) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to configure nodes', e)

        # Run benchmarks.
        for n in bench_parameters.nodes:
            committee_copy = deepcopy(committee)
            committee_copy.remove_nodes(committee.size() - n)

            for r in bench_parameters.rate:
                Print.heading(f'\nRunning {n} nodes (input rate: {r:,} tx/s)')

                # Run the benchmark.
                for i in range(bench_parameters.runs):
                    Print.heading(f'Run {i+1}/{bench_parameters.runs}')
                    try:
                        self._run_single(
                            r, committee_copy, bench_parameters, node_parameters, debug
                        )

                        faults = bench_parameters.faults
                        logger = self._logs(committee_copy, faults)
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
                        Print.error(BenchError('Benchmark failed', e))
                        continue
