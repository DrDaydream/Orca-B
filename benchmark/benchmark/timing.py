"""One UTC measurement plan, armed only after all surviving nodes are ready."""
import json
from pathlib import Path
import re
from time import monotonic, sleep, time_ns
from uuid import uuid4

START_PLAN = 'logs/benchmark-start.json'
MEASUREMENT_WINDOW = 'measurement-window.json'
STOP_EVENT = r'Benchmark input stopped at_us (\d+) released_samples (\d+)$'
READY_TIMEOUT = 120
LEAD_US = 30_000_000
ACK_LEAD_US = 5_000_000
MAX_LATENESS_US = 500_000
EVIDENCE_GRACE_US = 5_000_000


class ClockError(RuntimeError):
    pass


def now_us():
    return time_ns() // 1000


def create_plan(duration, warmup=0, drain=0, *, run_id=None):
    for label, value in (('duration', duration), ('warmup', warmup), ('drain', drain)):
        if type(value) is not int or value < (1 if label == 'duration' else 0):
            raise ClockError(f'{label} must be an integer >= {1 if label == "duration" else 0}')
    load = now_us() + LEAD_US
    start = load + warmup * 1_000_000
    end = start + duration * 1_000_000
    return dict(schema=2, run_id=run_id or uuid4().hex, load_us=load,
                start_us=start, end_us=end, drain_end_us=end + drain * 1_000_000)


def write_plan(filename, plan):
    """Readers see either no file or the complete plan, never partial JSON."""
    target = Path(filename)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + '.' + plan['run_id'] + '.tmp')
    temporary.write_text(json.dumps(plan, sort_keys=True, separators=(',', ':')) + '\n',
                         encoding='utf-8')
    temporary.replace(target)


def save_plan(plan, logs_directory, results_directory, snapshot=None):
    write_plan(Path(logs_directory) / MEASUREMENT_WINDOW, plan)
    write_plan(Path(results_directory) / f"{plan['run_id']}.measurement-window.json", plan)
    if snapshot:
        target = Path(snapshot)
        document = json.loads(target.read_text(encoding='utf-8'))
        document['measurement_window'] = plan
        target.write_text(json.dumps(document, indent=2, sort_keys=True) + '\n', encoding='utf-8')


def _single(log, pattern, identity):
    matches = re.findall(pattern, log)
    if len(matches) > 1:
        raise ClockError(f'Duplicate clock event in {identity}')
    return matches[0] if matches else None


# env_logger prefixes a level with a timestamp. Rust main errors and the
# remote process-exit sentinel may be bare lines. A WARN describing an OS
# error is expected when a forwarding attempt reaches a crashed authority.
_FAILURE_LOG_LINE = re.compile(
    r'^(?:(?:\[?\d{4}-\S+\s+)?ERROR\b.*|Error:.*'
    r'|.*(?i:panicked at|address already in use).*)$',
    re.MULTILINE,
)


def _healthy(logs):
    for identity, log in logs.items():
        failure = _FAILURE_LOG_LINE.search(log)
        if failure:
            raise ClockError(f'Benchmark process/log failure in {identity}: {failure.group()[:1000]}')


def coordinate(clients, primaries, read_logs, publish, *, duration, warmup=0, drain=0,
               run_id=None):
    """Callbacks own process/SSH I/O; every observation has a bounded timeout."""
    if not clients or not primaries:
        raise ClockError('No benchmark clients or primaries were launched')
    deadline = monotonic() + READY_TIMEOUT
    pending = list(clients) + list(primaries)
    while True:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise ClockError(f'Processes not ready within {READY_TIMEOUT} seconds: {pending}')
        logs = read_logs(min(10, remaining))
        _healthy(logs)
        pending = [name for name in clients if 'Benchmark clock ready' not in logs.get(name, '')]
        pending += [name for name in primaries
                    if not re.search(r'Primary[^\n]*booted on', logs.get(name, ''))]
        if not pending:
            break
        sleep(min(0.1, max(0, deadline - monotonic())))

    plan = create_plan(duration, warmup, drain, run_id=run_id)
    publish(plan)
    pending = list(clients)
    while pending:
        remaining = (plan['load_us'] - ACK_LEAD_US - now_us()) / 1_000_000
        if remaining <= 0:
            raise ClockError(f'Clients must acknowledge the plan at least 5 seconds before load: {pending}')
        logs = read_logs(min(10, remaining))
        _healthy(logs)
        for identity in list(pending):
            encoded = _single(logs.get(identity, ''), r'Benchmark clock armed (\{[^\n]*\})', identity)
            if encoded:
                try:
                    armed = json.loads(encoded)
                except ValueError as error:
                    raise ClockError(f'Invalid armed plan in {identity}') from error
                if armed != plan:
                    raise ClockError(f'Client {identity} acknowledged a different plan/run')
                if now_us() > plan['load_us'] - ACK_LEAD_US:
                    raise ClockError(f'Client {identity} did not acknowledge 5 seconds before load')
                pending.remove(identity)
        if pending:
            sleep(min(0.1, max(0, remaining)))

    started, stopped = set(), set()
    # A zero drain still allows the final stop log to be flushed. The measurement
    # cutoff remains exactly drain_end_us, regardless of controller teardown time.
    evidence_end = max(plan['drain_end_us'], plan['end_us'] + EVIDENCE_GRACE_US)
    while True:
        poll_started_us = now_us()
        logs = read_logs(10)
        _healthy(logs)
        for identity in clients:
            log = logs.get(identity, '')
            start = _single(log, r'Benchmark input started at_us (\d+) lateness_us (\d+)', identity)
            if start:
                actual, lateness = map(int, start)
                if (actual < plan['load_us'] or actual - plan['load_us'] > MAX_LATENESS_US
                        or lateness != actual - plan['load_us']):
                    raise ClockError(f'Client {identity} started early or late: {actual}, lateness {lateness}')
                started.add(identity)
            stop = _single(log, '(?m)' + STOP_EVENT, identity)
            if stop:
                if int(stop[0]) < plan['end_us']:
                    raise ClockError(f'Client {identity} stopped outside the input cutoff: {stop[0]}')
                stopped.add(identity)
        now = now_us()
        if now > plan['load_us'] + EVIDENCE_GRACE_US and len(started) != len(clients):
            raise ClockError(f'Missing input-start evidence: {set(clients) - started}')
        if (poll_started_us >= plan['drain_end_us'] and len(stopped) == len(clients)
                and len(started) == len(clients)):
            return plan
        # A poll started before the deadline can still contain a pre-stop snapshot.
        if poll_started_us >= evidence_end and len(stopped) != len(clients):
            raise ClockError(f'Missing input-stop evidence: {set(clients) - stopped}')
        if now < plan['drain_end_us']:
            sleep(min(1, (plan['drain_end_us'] - now) / 1_000_000))
        elif len(stopped) != len(clients):
            sleep(min(1, max(0, (evidence_end - now) / 1_000_000)))
