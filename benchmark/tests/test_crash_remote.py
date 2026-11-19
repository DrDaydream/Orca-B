"""Remote launch/download selection, with AWS and SSH replaced at the boundary."""
from base64 import b64encode
from collections import OrderedDict
from contextlib import ExitStack
from pathlib import Path
import shlex
import unittest
from unittest.mock import MagicMock, patch

from benchmark.adversary import remote_parameters, resolve_adversary
from benchmark.config import Committee, BenchParameters, NodeParameters
from benchmark.remote import Bench


class CrashRemoteTests(unittest.TestCase):
    def test_launch_and_download_use_same_original_ids_for_every_mode(self):
        protocol = 'bullshark' if 'bullshark' in str(Path(__file__).resolve()) else 'orca'
        names = [b64encode(bytes([i]) * 32).decode() for i in range(10)]
        for mode, ids in [('A1', ''), ('A1', '1,4,8'), ('A3', ''), ('A5', '')]:
            with self.subTest(mode=mode, ids=ids), ExitStack() as stack:
                params = remote_parameters(nodes=10, workers=2, rate='10000', tx_size=512,
                    duration=60, faults=0, runs=1, default_faults=0, protocol=protocol,
                    adversary=mode, byzantine=0 if ids else 3, byzantine_ids=ids)
                config = resolve_adversary(params['adversary'], names)
                missing = set(params['adversary']['byzantine_ids']) if mode == 'A1' else set()
                active = sorted(set(range(10)) - missing)
                committee = Committee(OrderedDict((name, [f'192.0.2.{i+1}'] * 3)
                                      for i, name in enumerate(names)), 3000)
                node = NodeParameters(dict(header_size=20, max_header_delay=200, gc_depth=50,
                    sync_retry_delay=10000, sync_retry_nodes=3, batch_size=500000, max_batch_delay=200, max_concurrent_requests=500000))
                bench = Bench.__new__(Bench)
                bench.connect = {}
                bench.kill = MagicMock()
                bench._wait_for_clients = MagicMock()
                launched = []
                bench._background_run = lambda host, cmd, log: launched.append((cmd, Path(log).name))
                stack.enter_context(patch('benchmark.remote.sleep'))
                stack.enter_context(patch('benchmark.remote.Print'))
                stack.enter_context(patch('benchmark.remote.progress_bar', side_effect=lambda rows, **_: rows))
                args = [10000, committee, BenchParameters(params)]
                if protocol == 'orca':
                    args.append(node)
                bench._run_single(*args, adversary_config=config)
                expected = {f'primary-{i}.log' for i in active}
                expected |= {f'{kind}-{i}-{w}.log' for i in active for w in range(2)
                             for kind in ('worker', 'client')}
                self.assertEqual({name for _, name in launched}, expected)
                clients = [cmd for cmd, name in launched if name.startswith('client-')]
                self.assertEqual(len(clients), len(active) * 2)
                for cmd in clients:
                    tokens = shlex.split(cmd)
                    self.assertEqual(tokens[tokens.index('--rate') + 1], '500')
                    for i in missing:
                        self.assertNotIn(f'192.0.2.{i+1}:', cmd)
                connection = stack.enter_context(patch('benchmark.remote.Connection'))
                stack.enter_context(patch('benchmark.remote.subprocess.run'))
                parser = stack.enter_context(patch('benchmark.remote.LogParser.process'))
                bench._logs(committee, 0, config)
                downloaded = {Path(call.args[0]).name for call in connection.return_value.get.call_args_list}
                self.assertEqual(downloaded, expected)
                self.assertEqual(parser.call_args.kwargs['committee_size'], 10)
                self.assertEqual(parser.call_args.kwargs['adversary'], config)


if __name__ == '__main__':
    unittest.main()
