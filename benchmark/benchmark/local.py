# Copyright(C) Facebook, Inc. and its affiliates.
import subprocess
from pathlib import Path
from uuid import uuid4
from math import ceil
from time import sleep

from benchmark.adversary import active_launch_ids, resolve_adversary, validate_adversary, write_run_snapshot
from benchmark.commands import CommandMaker
from benchmark.adversary_schedule import build_client_schedules, client_silence_slot_ms
from benchmark.config import Key, LocalCommittee, NodeParameters, BenchParameters, ConfigError
from benchmark.logs import LogParser, ParseError
from benchmark.local_processes import LocalProcesses
from benchmark.utils import Print, BenchError, PathMaker
from benchmark import timing


class LocalBench:
    BASE_PORT = 3000

    def __init__(self, bench_parameters_dict, node_parameters_dict):
        self.processes = LocalProcesses()
        self.snapshot = None
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
            self.adversary = validate_adversary(
                bench_parameters_dict.get('adversary'), 'orca',
                self.bench_parameters.nodes, self.bench_parameters.faults,
            )
            self.benchmark_input = dict(bench_parameters_dict)
        except (ConfigError, ValueError) as e:
            raise BenchError('Invalid nodes or bench parameters', e)

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file):
        self.processes.start(command, log_file)

    def _kill_nodes(self):
        self.processes.stop()

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting local benchmark')

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            Print.info('Setting up testbed...')
            nodes, rate = self.nodes[0], self.rate[0]
            self.snapshot = None
            run_id = uuid4().hex

            # Cleanup all files.
            cmd = f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            # An unremoved plan must never release clients before this run is ready.
            if self.common_clock:
                Path(timing.START_PLAN).unlink(missing_ok=True)
            sleep(0.5)  # Removing the store may take time.

            # Recompile the latest code.
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Generate configuration files.
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(nodes)]
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]

            names = [x.name for x in keys]
            committee = LocalCommittee(names, self.BASE_PORT, self.workers)
            committee.print(PathMaker.committee_file())

            self.node_parameters.print(PathMaker.parameters_file())
            adversary_config = resolve_adversary(self.adversary, names)
            active = set(active_launch_ids(nodes, adversary_config))
            if adversary_config is not None:
                snapshot = write_run_snapshot(
                    PathMaker.results_path(), self.benchmark_input, self.node_parameters.json,
                    names, adversary_config, nodes, rate, 1, protocol='orca-b-local',
                )
                self.snapshot = snapshot
                Print.info(f"Adversary: {adversary_config['mode']}; parameters: {snapshot}")

            # Run the clients (they will wait for the nodes to be ready).
            workers_addresses = {i: rows for i, rows in enumerate(committee.workers_addresses(0))
                                 if i in active}
            rate_share = ceil(rate / committee.workers())
            if adversary_config is None:
                silence_slot_ms = client_silence_slot_ms(
                    self.node_parameters.json['max_header_delay']
                )
                silence_schedules = build_client_schedules(
                    names, self.faults, self.warmup + self.duration, silence_slot_ms
                )
            else:
                silence_slot_ms = 0
                silence_schedules = dict.fromkeys(names, '')
            for i, addresses in workers_addresses.items():
                for (id, address) in addresses:
                    cmd = CommandMaker.run_client(
                        address,
                        self.tx_size,
                        rate_share,
                        [x for y in workers_addresses.values() for _, x in y],
                        silence_schedules[names[i]],
                        silence_slot_ms,
                        start_plan=timing.START_PLAN if self.common_clock else None,
                    )
                    log_file = PathMaker.client_log_file(i, id)
                    self._background_run(cmd, log_file)

            # Run the primaries (except the faulty ones).
            primary_logs = []
            for i, address in enumerate(committee.primary_addresses(0)):
                if i not in active:
                    continue
                cmd = CommandMaker.run_primary(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i),
                    PathMaker.parameters_file(),
                    debug=debug,
                    faults=self.faults,
                    adversary_config=adversary_config,
                )
                log_file = PathMaker.primary_log_file(i)
                self._background_run(cmd, log_file)
                primary_logs.append(Path(log_file))

            # Run the workers (except the faulty ones).
            for i, addresses in workers_addresses.items():
                for (id, address) in addresses:
                    cmd = CommandMaker.run_worker(
                        PathMaker.key_file(i),
                        PathMaker.committee_file(),
                        PathMaker.db_path(i, id),
                        PathMaker.parameters_file(),
                        id,  # The worker's id.
                        debug=debug
                    )
                    log_file = PathMaker.worker_log_file(i, id)
                    self._background_run(cmd, log_file)

            # Wait for all transactions to be processed.
            Print.info(f'Running benchmark ({self.duration} sec)...')
            if self.common_clock:
                def publish(plan):
                    timing.save_plan(plan, PathMaker.logs_path(), PathMaker.results_path(), self.snapshot)
                    timing.write_plan(timing.START_PLAN, plan)
                self.processes.clock = dict(primaries=primary_logs, publish=publish,
                    warmup=self.warmup, drain=self.drain, run_id=run_id)
            self.processes.wait(self.duration)
            self._kill_nodes()

            # Parse logs and return the parser.
            Print.info('Parsing logs...')
            return LogParser.process(PathMaker.logs_path(), faults=self.faults,
                                     adversary=self.adversary, committee_size=nodes)

        except (OSError, subprocess.SubprocessError, ParseError, RuntimeError) as e:
            raise BenchError('Failed to run benchmark', e)
        finally:
            self._kill_nodes()
