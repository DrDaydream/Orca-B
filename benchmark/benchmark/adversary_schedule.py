from argparse import ArgumentParser
from base64 import b64decode
from hashlib import sha512
from json import load
from math import ceil
from os import environ


DOMAIN = b'narwhal-dynamic-adversary-v1'


def client_silence_slot_ms(default):
    value = int(environ.get('ORCA_CLIENT_SILENCE_SLOT_MS', str(default)))
    if value <= 0:
        raise ValueError('ORCA_CLIENT_SILENCE_SLOT_MS must be positive')
    return value


def _score(authority, round_number, seed, domain):
    payload = (
        DOMAIN
        + seed.to_bytes(8, 'little')
        + round_number.to_bytes(8, 'little')
        + bytes([domain])
        + authority
    )
    return sha512(payload).digest()[:32]


def build_client_schedules(names, faults, duration, slot_ms):
    mode = environ.get('ORCA_CLIENT_DURING_SILENCE', 'send').lower()
    if mode not in {'send', 'pause'}:
        raise ValueError('ORCA_CLIENT_DURING_SILENCE must be send or pause')
    behavior = environ.get('ORCA_RULE3_BEHAVIOR', 'mixed').lower()
    if behavior not in {'mixed', 'silent', 'participate'}:
        raise ValueError('ORCA_RULE3_BEHAVIOR must be mixed, silent, or participate')
    seed = int(environ.get('ORCA_ADVERSARY_SEED', '0'))
    if seed < 0 or seed > 2**64 - 1:
        raise ValueError('ORCA_ADVERSARY_SEED must be an unsigned 64-bit integer')
    if slot_ms <= 0:
        raise ValueError('ORCA_CLIENT_SILENCE_SLOT_MS must be positive')

    authorities = [(name, b64decode(name)) for name in names]
    if any(len(raw) != 32 for _, raw in authorities):
        raise ValueError('Invalid authority public key')
    ordered = sorted(raw for _, raw in authorities)
    faults = min(max(faults, 0), len(ordered))
    slots = ceil((duration * 1_000 + 5_000) / slot_ms)
    schedules = {name: [] for name in names}

    for round_number in range(1, slots + 1):
        selected = set(
            sorted(ordered, key=lambda key: (_score(key, round_number, seed, 0), key))
            [:faults]
        )
        leader = ordered[round_number % len(ordered)]
        for name, authority in authorities:
            silent = mode == 'pause' and authority in selected
            if silent and authority == leader:
                if behavior == 'participate':
                    silent = False
                elif behavior == 'mixed':
                    silent = _score(authority, round_number, seed, 1)[0] & 1 == 0
            schedules[name].append('1' if silent else '0')

    return {name: ''.join(bits) for name, bits in schedules.items()}


def main():
    parser = ArgumentParser()
    parser.add_argument('--committee', required=True)
    parser.add_argument('--faults', required=True, type=int)
    parser.add_argument('--duration', required=True, type=int)
    parser.add_argument('--slot-ms', required=True, type=int)
    parser.add_argument('--key-files', nargs='*')
    args = parser.parse_args()
    with open(args.committee, 'r') as file:
        names = list(load(file)['authorities'])
    schedules = build_client_schedules(names, args.faults, args.duration, args.slot_ms)
    output_names = names
    if args.key_files:
        output_names = []
        for filename in args.key_files:
            with open(filename, 'r') as file:
                output_names.append(load(file)['name'])
    for name in output_names:
        print(schedules[name])


if __name__ == '__main__':
    main()
