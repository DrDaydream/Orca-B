# Copyright(C) Facebook, Inc. and its affiliates.
from fabric import task
from invoke.exceptions import Exit
from benchmark.adversary import local_parameters, remote_parameters

from benchmark.local import LocalBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print
from benchmark.plot import Ploter, PlotError
from benchmark.instance import InstanceManager
from benchmark.remote import Bench, BenchError


@task(help={
    'adversary': 'Mode: none/A0, silent/A1 (full-run crashes), targeted/A3, network/A5; omitted keeps legacy behavior.',
    'header-size': 'Override the local header size in bytes.',
    'warmup': 'Seconds of input before the common measurement window (default 0).',
    'drain': 'Seconds to observe commits after input stops (default 0).',
    'common-clock': 'Use an acknowledged common UTC window (true by default).',
    'bullshark-alternating': 'Bullshark: true or false for alternate unavailable leader slots.',
})
def local(ctx, debug=False, nodes=None, workers=None, rate=None, tx_size=None,
          duration=None, faults=None, header_size=None, adversary='', byzantine=0,
          byzantine_ids='', seed=1, start_round=1, attack_rounds=0,
          delay_ms=200, max_pending_messages=4096, max_pending_bytes=67108864,
          bullshark_alternating='true', warmup=0, drain=0, common_clock='true'):
    ''' Run benchmarks on localhost '''
    bench_params = {
        'faults': 0,
        'nodes': 4,
        'workers': 1,
        'rate': 50_000,
        'tx_size': 512,
        'duration': 20,
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200  # ms
    }
    try:
        bench_params = local_parameters(
            bench_params, protocol='orca', nodes=nodes, workers=workers,
            rate=rate, tx_size=tx_size, duration=duration, faults=faults,
            adversary=adversary, byzantine=byzantine, byzantine_ids=byzantine_ids,
            seed=seed, start_round=start_round, attack_rounds=attack_rounds,
            delay_ms=delay_ms, max_pending_messages=max_pending_messages,
            max_pending_bytes=max_pending_bytes, bullshark_alternating=bullshark_alternating,
            warmup=warmup, drain=drain, common_clock=common_clock,
        )
        if header_size is not None:
            node_params['header_size'] = int(header_size)
        if node_params['header_size'] < 1:
            raise ValueError('header-size must be positive')
    except ValueError as e:
        raise Exit(str(e), code=2) from e
    try:
        ret = LocalBench(bench_params, node_params).run(debug)
        print(ret.result())
        ret.print_comparison(bench_params['duration'], 'logs/comparison.json')
    except BenchError as e:
        Print.error(e)
        raise


@task
def create(ctx, nodes=2):
    ''' Create a testbed'''
    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    ''' Destroy the testbed '''
    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max=2):
    ''' Start at most `max` machines per data center '''
    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    ''' Stop all machines '''
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    ''' Display connect information about all the available machines '''
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    ''' Install the codebase on all machines '''
    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)
        raise


@task(help={
    'adversary': 'Explicit mode: none/A0, silent/A1 (full-run crashes), targeted/A3, network/A5; omitted preserves legacy behavior.',
    'faults': 'Legacy faults (original default when mode omitted); explicit modes default to 0.',
    'delay-ms': 'Selected message delay in milliseconds, 1..86400000.',
    'byzantine': 'Fixed count (A1: last launch IDs; A3/A5: first IDs), at most floor((nodes-1)/3).',
    'byzantine-ids': 'Comma-separated zero-based launch IDs; mutually exclusive with byzantine.',
    'bullshark-alternating': 'Bullshark only: true or false; alternate normal/unavailable leader slots.',
    'attack-rounds': 'A3/A5 round window; 0 is unbounded. A1 requires 0 and start-round=1.',
    'rate': 'Input transactions per second; comma-separated values run a sweep.',
    'header-size': 'Override the remote header size in bytes.',
    'warmup': 'Seconds of input before the common measurement window (default 0).',
    'drain': 'Seconds to observe commits after input stops (default 0).',
    'common-clock': 'Use an acknowledged common UTC window (true by default).',
})
def remote(ctx, debug=False, nodes=20, workers=1, rate='50000', tx_size=512,
           duration=80, faults=None, runs=1, adversary='', byzantine=0,
           byzantine_ids='', seed=1, start_round=1, attack_rounds=0,
           delay_ms=200, max_pending_messages=4096, max_pending_bytes=67108864,
           bullshark_alternating='true', warmup=0, drain=0, common_clock='true',
           header_size=None):
    ''' Prepare the AWS testbed and run one benchmark '''
    try:
        bench_params = remote_parameters(
            nodes=nodes, workers=workers, rate=rate, tx_size=tx_size,
            duration=duration, faults=faults, runs=runs,
            default_faults=0, protocol='orca', adversary=adversary,
            byzantine=byzantine, byzantine_ids=byzantine_ids, seed=seed,
            start_round=start_round, attack_rounds=attack_rounds, delay_ms=delay_ms,
            max_pending_messages=max_pending_messages, max_pending_bytes=max_pending_bytes,
            bullshark_alternating=bullshark_alternating,
            warmup=warmup, drain=drain, common_clock=common_clock,
        )
        if header_size is not None and (isinstance(header_size, bool) or int(header_size) < 1):
            raise ValueError('header-size must be positive')
    except ValueError as e:
        raise Exit(str(e), code=2) from e
    node_params = {
        'header_size': 20,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200  # ms
    }
    try:
        if header_size is not None:
            node_params['header_size'] = int(header_size)
        Bench(ctx).run(bench_params, node_params, debug)
    except BenchError as e:
        Print.error(e)
        raise


@task
def plot(ctx):
    ''' Plot performance using the logs generated by "fab remote" '''
    plot_params = {
        'faults': [0],
        'nodes': [10, 20, 50],
        'workers': [1],
        'collocate': True,
        'tx_size': 512,
        'max_latency': [3_500, 4_500]
    }
    try:
        Ploter.plot(plot_params)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))


@task
def kill(ctx):
    ''' Stop execution on all machines '''
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    ''' Print a summary of the logs '''
    try:
        print(LogParser.process('./logs', faults='?').result())
    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))
