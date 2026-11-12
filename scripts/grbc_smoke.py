"""Run real GRBC primaries/workers on loopback, checking payload commits and agreement.

Build first: cargo build -p node --features benchmark
Run: python scripts/grbc_smoke.py --scenario all
Also supports one absent authority (silent) and one late authority (late).
Uses fresh temporary stores, keeps logs, and terminates only its own children.
"""
import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import re
import socket
import struct
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, default=Path('target/debug/node.exe' if os.name == 'nt' else 'target/debug/node'))
    parser.add_argument('--scenario', choices=['all', 'silent', 'late'], default='all')
    parser.add_argument('--nodes', type=int, choices=[4, 10], default=4)
    parser.add_argument('--duration', type=float, default=12)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    root = args.output.resolve() if args.output else Path(tempfile.mkdtemp(prefix='orca-grbc-'))
    root.mkdir(parents=True, exist_ok=True)
    flags = subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
    env = {k: v for k, v in os.environ.items() if not k.startswith('ORCA_')}
    env.update(RUST_LOG='info', TOKIO_WORKER_THREADS='2')
    held_ports = []

    def address():
        sock = socket.socket()
        sock.bind(('127.0.0.1', 0))
        held_ports.append(sock)
        return f'127.0.0.1:{sock.getsockname()[1]}'

    authorities, names = {}, []
    for node in range(args.nodes):
        key = root / f'key-{node}.json'
        subprocess.run([str(binary), 'generate_keys', '--filename', str(key)], check=True, creationflags=flags)
        name = json.loads(key.read_text())['name']
        names.append(name)
        authorities[name] = {
            'stake': 1,
            'primary': {key: address() for key in ['primary_to_primary', 'aba_to_aba', 'worker_to_primary']},
            'workers': {'0': {key: address() for key in ['primary_to_worker', 'transactions', 'worker_to_worker']}},
        }
    (root / 'committee.json').write_text(json.dumps({'authorities': authorities}))
    (root / 'parameters.json').write_text(json.dumps({
        'header_size': 32, 'max_header_delay': 100, 'gc_depth': 200,
        'sync_retry_delay': 200, 'sync_retry_nodes': 3,
        'batch_size': 4096, 'max_batch_delay': 50,
    }))
    for sock in held_ports:
        sock.close()
    children, logs, sockets, started = [], [], {}, []

    def start(node):
        started.append(node)
        for role in ['primary', 'worker']:
            log = (root / f'{role}-{node}.log').open('wb')
            logs.append(log)
            command = [str(binary), '-vv', 'run', '--keys', str(root / f'key-{node}.json'),
                       '--committee', str(root / 'committee.json'), '--parameters', str(root / 'parameters.json'),
                       '--store', str(root / f'store-{role}-{node}'), role]
            if role == 'worker':
                command.extend(['--id', '0'])
            children.append(subprocess.Popen(command, stdout=log, stderr=log, env=env, creationflags=flags))

    sent = 0
    try:
        for node in range(args.nodes if args.scenario == 'all' else args.nodes - 1):
            start(node)
        begin = time.monotonic()
        while time.monotonic() - begin < args.duration:
            elapsed = time.monotonic() - begin
            if args.scenario == 'late' and elapsed > 3 and args.nodes - 1 not in started:
                start(args.nodes - 1)
            assert all(child.poll() is None for child in children), f'Node exited; see {root}'
            for node in started:
                if node not in sockets:
                    port = int(authorities[names[node]]['workers']['0']['transactions'].split(':')[-1])
                    try:
                        sockets[node] = socket.create_connection(('127.0.0.1', port), timeout=0.1)
                    except OSError:
                        continue
                if elapsed < args.duration - 2:
                    transaction = b'\x00' + struct.pack('>Q', sent) + bytes([node + 1]) * 503
                    sockets[node].sendall(struct.pack('>I', len(transaction)) + transaction)
                    sent += 1
            time.sleep(0.05)
    finally:
        stopped_at = time.time()
        for sock in sockets.values():
            sock.close()
        for child in children:
            child.terminate()
        for child in children:
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        for log in logs:
            log.close()

    records, sequences = [], []
    for node in started:
        log = (root / f'primary-{node}.log').read_text(errors='replace')
        sequence = re.findall(r'Header committed round (\d+) digest (\S+)', log)
        payloads = re.findall(r'Committed B\d+\(.+?\) -> (\S+)', log)
        assert len(sequence) >= 8 and payloads, f'No sustained payload commits at node {node}; see {root}'
        assert not re.search(r'panicked|Malformed header|Invalid signature|ERROR', log), f'Protocol error at node {node}; see {root}'
        sequences.append(sequence)
        commit_times = re.findall(r'\[([^ ]+) INFO +consensus\] Header committed round', log)
        assert commit_times, f'No timestamped commits at node {node}; see {root}'
        last_commit = datetime.fromisoformat(commit_times[-1].replace('Z', '+00:00')).timestamp()
        progress_age = max(0, stopped_at - last_commit)
        assert progress_age <= max(3, args.duration / 4), f'Node {node} stopped making progress {progress_age:.1f}s before shutdown; see {root}'
        records.append({'node': node, 'committed_blocks': len(sequence), 'payload_commits': len(payloads), 'last_round': int(sequence[-1][0]), 'last_commit_age_seconds': round(progress_age, 3)})
    shared = min(map(len, sequences))
    assert all(sequence[:shared] == sequences[0][:shared] for sequence in sequences), f'Commit order differs; see {root}'
    result = {'scenario': args.scenario, 'committee_size': args.nodes, 'duration_seconds': args.duration, 'sent_transactions': sent, 'common_commit_prefix': shared,
              'max_round_lag': max(record['last_round'] for record in records) - min(record['last_round'] for record in records),
              'nodes': records, 'logs': str(root), 'passed': True}
    (root / 'result.json').write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
