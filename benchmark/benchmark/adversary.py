"""Explicit benchmark adversaries: parameter validation and primary launch config."""
from datetime import datetime, timezone
import json
from math import ceil
from pathlib import Path
from shlex import quote

U64_MAX = 2**64 - 1
MODES = {'none': 'none', 'silent': 'silent', 'targeted': 'targeted',
         'network': 'network', 'a0': 'none', 'a1': 'silent',
         'a3': 'targeted', 'a5': 'network'}
DEFAULTS = dict(seed=1, start_round=1, rounds=0, delay_ms=200,
                max_pending_messages=4096, max_pending_bytes=67108864,
                bullshark_alternating=True)


def _integer(value, name, minimum=0):
    if isinstance(value, bool) or not (
            isinstance(value, int) or
            isinstance(value, str) and value.isascii() and value.isdigit()):
        raise ValueError(f'{name} must be an integer')
    value = int(value)
    if not minimum <= value <= U64_MAX:
        raise ValueError(f'{name} must be between {minimum} and {U64_MAX}')
    return value


def _boolean(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in ('true', 'false'):
        return value.lower() == 'true'
    raise ValueError('bullshark-alternating must be true or false')


def validate_adversary(options, nodes, faults):
    """Validate before any AWS access, also for callers of Bench.run."""
    if options is None:
        return None
    if not isinstance(options, dict):
        raise ValueError('adversary must be a parameter dictionary')
    mode = MODES.get(str(options.get('mode', '')).lower())
    if mode is None:
        raise ValueError('adversary must be none, silent, targeted, network, A0, A1, A3 or A5')
    config = dict(mode=mode)
    for name, default in DEFAULTS.items():
        value = options.get(name, default)
        if name == 'bullshark_alternating':
            config[name] = _boolean(value)
        else:
            minimum = 1 if name in ('start_round', 'delay_ms', 'max_pending_messages',
                                     'max_pending_bytes') else 0
            config[name] = _integer(value, name.replace('_', '-'), minimum)
    if config['delay_ms'] > 86_400_000:
        raise ValueError('delay-ms must be between 1 and 86400000')
    if config['rounds'] and config['start_round'] > U64_MAX - config['rounds']:
        raise ValueError('start-round + attack-rounds exceeds the unsigned 64-bit range')
    if mode == 'silent':
        if faults:
            raise ValueError('A1 requires --faults 0; select crashed nodes with --byzantine or --byzantine-ids')
        if config['start_round'] != 1 or config['rounds'] != 0:
            raise ValueError('A1 is a full-run crash: require --start-round 1 --attack-rounds 0')
        config['fault_model'] = 'crash-stop'
        config['bullshark_alternating'] = False
    ids = options.get('byzantine_ids', [])
    if not isinstance(ids, (list, tuple)):
        raise ValueError('byzantine_ids must be launch node IDs')
    ids = [_integer(value, 'byzantine ID') for value in ids]
    if len(set(ids)) != len(ids):
        raise ValueError('byzantine-ids must not contain duplicates')
    if mode == 'none' and ids:
        raise ValueError('adversary none requires an empty Byzantine set')
    if faults:
        raise ValueError('Explicit Orca adversary modes require --faults 0; use --byzantine or --byzantine-ids')
    for n in nodes:
        if len(ids) > (n - 1) // 3:
            raise ValueError(f'Fixed Byzantine set exceeds f=floor((n-1)/3) for {n} nodes')
        if any(i >= n for i in ids):
            raise ValueError(f'byzantine-ids must be between 0 and {n - 1}')
    if mode in ('silent', 'targeted') and not ids:
        raise ValueError('This mode requires --byzantine or --byzantine-ids')
    if 'crash_last' in options:
        count = _integer(options['crash_last'], 'crash count', 1)
        if mode != 'silent' or count != len(ids):
            raise ValueError('Invalid trailing crash group')
        config['crash_last'] = count
    config['byzantine_ids'] = ids
    return config


def remote_parameters(*, nodes, workers, rate, tx_size, duration, faults, runs,
                      default_faults, collocate=True, adversary='', byzantine=0,
                      byzantine_ids='', seed=1, start_round=1, attack_rounds=0,
                      delay_ms=200, max_pending_messages=4096,
                      max_pending_bytes=67108864, bullshark_alternating='true'):
    """Parse Fabric values without constructing an AWS manager."""
    mode = str(adversary).strip().lower() if adversary is not None else ''
    if mode and mode not in MODES:
        raise ValueError('adversary must be none, silent, targeted, network, A0, A1, A3 or A5')
    nodes = [nodes] if not isinstance(nodes, (tuple, list)) else nodes
    if not nodes:
        raise ValueError('At least one committee size is required')
    nodes = [_integer(n, 'nodes', 2) for n in nodes]
    faults = (default_faults if not mode else 0) if faults in (None, '') else faults
    faults = _integer(faults, 'faults')
    if faults >= min(nodes):
        raise ValueError('Require 0 <= faults < nodes')
    rates = rate if isinstance(rate, (list, tuple)) else str(rate).split(',')
    params = dict(
        faults=faults, nodes=nodes, workers=_integer(workers, 'workers', 1),
        collocate=collocate, rate=[_integer(str(x).strip(), 'rate') for x in rates],
        tx_size=_integer(tx_size, 'tx-size', 9),
        duration=_integer(duration, 'duration', 1), runs=_integer(runs, 'runs', 1),
    )
    count = _integer(byzantine, 'byzantine')
    ids = ([_integer(x.strip(), 'byzantine ID') for x in byzantine_ids.split(',')]
           if byzantine_ids else [])
    if count and ids:
        raise ValueError('Choose either --byzantine or --byzantine-ids')
    if count > (min(nodes) - 1) // 3:
        raise ValueError('Fixed Byzantine set exceeds f=floor((n-1)/3)')
    if count:
        ids = list(range(min(nodes) - count, min(nodes))) if MODES.get(mode) == 'silent' else list(range(count))
    options = dict(mode=mode, byzantine_ids=ids, seed=seed, start_round=start_round,
                   rounds=attack_rounds, delay_ms=delay_ms,
                   max_pending_messages=max_pending_messages,
                   max_pending_bytes=max_pending_bytes,
                   bullshark_alternating=_boolean(bullshark_alternating))
    if not mode:
        if ids or any(options[name] != default for name, default in DEFAULTS.items()):
            raise ValueError('Adversary parameters require an explicit --adversary mode')
        return params
    params['adversary'] = validate_adversary(options, nodes, faults)
    if count and MODES[mode] == 'silent' and len(set(nodes)) > 1:
        params['adversary']['crash_last'] = count
    return params


def local_parameters(defaults, **options):
    """Reuse remote validation, retaining editable local workload defaults."""
    values = dict(defaults)
    values.update({key: value for key, value in options.items() if value is not None})
    parsed = remote_parameters(**values, default_faults=0, runs=1)
    if len(parsed['rate']) != 1:
        raise ValueError('local accepts one rate per run; use remote for a rate sweep')
    parsed['nodes'] = parsed['nodes'][0]
    parsed['rate'] = parsed['rate'][0]
    parsed.pop('runs')
    parsed.pop('collocate')
    return parsed


def resolve_adversary(options, launch_authorities):
    """Map zero-based launch IDs to full keys, preserving committee insertion order."""
    if options is None:
        return None
    names = list(launch_authorities)
    if not all(isinstance(name, str) and name for name in names) or len(set(names)) != len(names):
        raise ValueError('Launch authority keys must be nonempty and unique')
    ids = options['byzantine_ids']
    if 'crash_last' in options:
        ids = list(range(len(names) - options['crash_last'], len(names)))
    if any(i < 0 or i >= len(names) for i in ids):
        raise ValueError('Byzantine launch ID is outside this committee')
    config = {name: value for name, value in options.items() if name not in ('byzantine_ids', 'crash_last')}
    config['byzantine_authorities'] = [names[i] for i in ids]
    if config.get('fault_model') == 'crash-stop':
        config['byzantine_ids'] = list(ids)
    return config


def active_launch_ids(nodes, adversary=None):
    """Keep original launch IDs; A1 omits primary, workers and clients together."""
    missing = set()
    if adversary and adversary.get('fault_model') == 'crash-stop':
        missing.update(adversary['byzantine_ids'])
    if any(type(i) is not int or i < 0 or i >= nodes for i in missing):
        raise ValueError('Inactive authority outside the committee')
    active = [i for i in range(nodes) if i not in missing]
    if not active:
        raise ValueError('At least one authority must be started')
    return active


def crash_population(directory, primary_ids, adversary=None, committee_size=None):
    """Validate a complete A1 log population, including when reparsing an archive."""
    directory = Path(directory)
    saved_path = directory / 'measurement-population.json'
    if adversary and adversary.get('fault_model') == 'crash-stop':
        crashed = adversary['byzantine_ids']
        if committee_size is None:
            raise ValueError('A1 log parsing requires the original committee size')
    elif adversary is None and saved_path.exists():
        saved = json.loads(saved_path.read_text(encoding='utf-8'))
        if saved.get('fault_model') != 'crash-stop':
            return None
        if saved.get('schema_version') != 2:
            raise ValueError('Unknown crash population version')
        crashed = saved['crashed_primary_ids']
        if committee_size is not None and committee_size != saved['committee_size']:
            raise ValueError('Committee size differs from saved population')
        committee_size = saved['committee_size']
        if sorted(saved['primary_ids']) != sorted(primary_ids):
            raise ValueError('Saved crash population does not match primary logs')
    else:
        return None
    if (type(committee_size) is not int or committee_size < 2 or not isinstance(crashed, list)
            or not crashed or len(set(crashed)) != len(crashed)
            or any(type(i) is not int or i < 0 or i >= committee_size for i in crashed)):
        raise ValueError('Invalid crashed authority IDs or committee size')
    expected = set(range(committee_size)) - set(crashed)
    if set(primary_ids) != expected or len(primary_ids) != len(expected):
        raise ValueError('A1 primary logs must contain exactly the surviving authorities')
    logged_workers = {}
    for kind in ('client', 'worker'):
        actual = {}
        for path in directory.glob(f'{kind}-*.log'):
            parts = path.stem.split('-')
            if len(parts) != 3:
                raise ValueError(f'Invalid {kind} log identity: {path.name}')
            ident, worker = map(int, parts[1:])
            actual.setdefault(ident, set()).add(worker)
        logged_workers[kind] = actual
        if set(actual) != expected or len({tuple(sorted(v)) for v in actual.values()}) != 1:
            raise ValueError(f'A1 {kind} logs do not cover the surviving authorities')
    if logged_workers['client'] != logged_workers['worker']:
        raise ValueError('A1 client and worker log identities differ')
    return dict(schema_version=2, fault_model='crash-stop', committee_size=committee_size,
                primary_ids=primary_ids, excluded_primary_ids=[],
                crashed_primary_ids=sorted(crashed))


def adversary_environment(config):
    if config is None:
        return ''
    if config.get('fault_model') == 'crash-stop':
        # The launcher has already omitted the crashed authorities.
        config = dict(mode='none', bullshark_alternating=False)
    encoded = json.dumps(config, separators=(',', ':'), sort_keys=True)
    return f'env BENCH_ADVERSARY_CONFIG={quote(encoded)} '


def write_run_snapshot(directory, benchmark_parameters, node_parameters,
                       launch_authorities, config, nodes, rate, run, *, protocol):
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
    output = Path(directory) / f'{stamp}-{protocol}-n{nodes}-r{rate}-run{run}.parameters.json'
    output.parent.mkdir(parents=True, exist_ok=True)
    snapshot = dict(
        protocol=protocol, created_utc=stamp, nodes=nodes, rate=rate, run=run,
        benchmark_parameters=benchmark_parameters, node_parameters=node_parameters,
        launch_authorities=list(launch_authorities), adversary=config,
    )
    active = active_launch_ids(nodes, config)
    workers = benchmark_parameters['workers']
    snapshot.update(fault_model=config.get('fault_model', 'runtime'),
                    active_launch_ids=active,
                    inactive_launch_ids=[i for i in range(nodes) if i not in active],
                    offered_rate_tps=ceil(rate / (nodes * workers)) * len(active) * workers)
    snapshot['aba_scheduling'] = 'A5-only' if protocol.startswith('orca-b') else 'not-applicable'
    output.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    return str(output)
