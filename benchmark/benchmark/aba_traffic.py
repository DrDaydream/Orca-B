import re
from statistics import mean
from typing import NamedTuple


_COUNTER = re.compile(r'Benchmark ABA bytes at_us \d+ elapsed_us (\d+) sent (\d+) received (\d+)')


class CounterWindow(NamedTuple):
    seconds: float
    sent: int
    received: int


def counter_window(log):
    """Measure the complete snapshot interval using the primary's monotonic clock."""
    first = previous = None
    for match in _COUNTER.finditer(log):
        sample = tuple(map(int, match.groups()))
        if previous is not None and any(x < y for x, y in zip(sample, previous)):
            return None  # A counter or the monotonic clock restarted inside this log.
        if first is None:
            first = sample
        previous = sample
    if first is None or previous[0] == first[0]:
        return None
    return CounterWindow((previous[0] - first[0]) / 1_000_000,
                         previous[1] - first[1], previous[2] - first[2])


def format_aba_traffic(windows):
    lines = [' ABA dedicated channel traffic (first to last counter snapshot):']
    send_rates, receive_rates = [], []
    for replica, window in sorted(windows.items()):
        if window is None:
            lines.append(f' ABA replica {replica}: N/A (insufficient or discontinuous snapshots)')
            continue
        sent_rate = window.sent / window.seconds
        received_rate = window.received / window.seconds
        send_rates.append(sent_rate)
        receive_rates.append(received_rate)
        lines.append(f' ABA replica {replica}: sent {window.sent:,} B, received {window.received:,} B, '
                     f'send {sent_rate:,.3f} B/s, receive {received_rate:,.3f} B/s, '
                     f'window {window.seconds:,.3f} s')
    if send_rates and len(send_rates) == len(windows):
        lines.append(f' Mean per active replica: send {mean(send_rates):,.3f} B/s, '
                     f'receive {mean(receive_rates):,.3f} B/s')
    else:
        lines.append(' Mean per active replica: N/A')
    return '\n'.join(lines) + '\n'
