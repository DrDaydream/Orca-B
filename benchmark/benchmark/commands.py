# Copyright(C) Facebook, Inc. and its affiliates.
from os import environ
from os.path import join

from benchmark.utils import PathMaker


class CommandMaker:

    @staticmethod
    def cleanup():
        return (
            f'rm -r .db-* ; rm .*.json ; mkdir -p {PathMaker.results_path()}'
        )

    @staticmethod
    def clean_logs():
        return f'rm -r {PathMaker.logs_path()} ; mkdir -p {PathMaker.logs_path()}'

    @staticmethod
    def compile():
        return 'cargo build --quiet --release --features benchmark'

    @staticmethod
    def generate_key(filename):
        assert isinstance(filename, str)
        return f'./node generate_keys --filename {filename}'

    @staticmethod
    def run_primary(keys, committee, store, parameters, debug=False, faults=0):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        rule3_behavior = environ.get('ORCA_RULE3_BEHAVIOR', 'mixed').lower()
        if rule3_behavior not in {'mixed', 'silent', 'participate'}:
            raise ValueError('ORCA_RULE3_BEHAVIOR must be mixed, silent, or participate')
        adversary_seed = environ.get('ORCA_ADVERSARY_SEED', '0')
        if (not adversary_seed.isascii() or not adversary_seed.isdigit()
                or int(adversary_seed) > 2**64 - 1):
            raise ValueError('ORCA_ADVERSARY_SEED must be an unsigned 64-bit integer')
        return (f'ORCA_FAULTS={faults} ORCA_RULE3_BEHAVIOR={rule3_behavior} '
                f'ORCA_ADVERSARY_SEED={adversary_seed} '
                f'./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} primary')

    @staticmethod
    def run_worker(keys, committee, store, parameters, id, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} worker --id {id}')

    @staticmethod
    def run_client(address, size, rate, nodes, silence_schedule='', silence_slot_ms=0):
        assert isinstance(address, str)
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0
        assert isinstance(nodes, list)
        assert all(isinstance(x, str) for x in nodes)
        assert isinstance(silence_schedule, str)
        assert all(value in {'0', '1'} for value in silence_schedule)
        assert isinstance(silence_slot_ms, int) and silence_slot_ms >= 0
        nodes = f'--nodes {" ".join(nodes)}' if nodes else ''
        silence = (
            f'--silence-schedule {silence_schedule} '
            f'--silence-slot-ms {silence_slot_ms}'
            if silence_schedule else ''
        )
        return (f'./benchmark_client {address} --size {size} --rate {rate} '
                f'{silence} {nodes}')

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'benchmark_client')
        return f'rm node ; rm benchmark_client ; ln -s {node} . ; ln -s {client} .'
