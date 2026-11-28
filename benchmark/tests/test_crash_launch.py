"""Exercise the real local runner; replace only processes, waits and key generation."""
from base64 import b64encode
from contextlib import ExitStack
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from benchmark.adversary import remote_parameters
from benchmark.local import LocalBench


class CrashLaunchTests(unittest.TestCase):
    def test_a1_starts_only_surviving_primary_worker_client_ids(self):
        protocol = 'bullshark' if 'bullshark' in str(Path(__file__).resolve()) else 'orca'
        names = [b64encode(bytes([i]) * 32).decode() for i in range(10)]
        node = dict(header_size=20, max_header_delay=200, gc_depth=50,
                    sync_retry_delay=10000, sync_retry_nodes=3, batch_size=500000, max_batch_delay=200, max_concurrent_requests=500000)
        for ids, workers, expected in [('', 1, list(range(7))), ('1,4,8', 2, [0, 2, 3, 5, 6, 7, 9])]:
            params = remote_parameters(nodes=10, workers=workers, rate='10000', tx_size=512,
                duration=60, faults=0, runs=1, default_faults=0, protocol=protocol,
                adversary='A1', byzantine=0 if ids else 3, byzantine_ids=ids)
            with self.subTest(ids=ids), TemporaryDirectory() as directory, ExitStack() as stack:
                for target in ('subprocess.run', 'sleep', 'LocalCommittee.print',
                               'NodeParameters.print', 'LogParser.process'):
                    stack.enter_context(patch('benchmark.local.' + target))
                stack.enter_context(patch('benchmark.local.Key.from_file',
                    side_effect=[SimpleNamespace(name=name) for name in names]))
                stack.enter_context(patch('benchmark.local.PathMaker.results_path', return_value=directory))
                bench = LocalBench(params, node)
                bench.processes = MagicMock()
                bench._kill_nodes = lambda: None
                launched = []
                bench._background_run = lambda command, log: launched.append((command, Path(log).name))
                bench.run()
                bench.processes.configure_tokio.assert_called_once_with(len(launched))
                self.assertEqual({name for _, name in launched},
                    {f'primary-{i}.log' for i in expected}
                    | {f'{kind}-{i}-{j}.log' for i in expected for j in range(workers)
                       for kind in ('worker', 'client')})
                snapshot = json.loads(next(Path(directory).glob('*.parameters.json')).read_text())
                self.assertEqual(snapshot['active_launch_ids'], expected)
                self.assertEqual(len(snapshot['launch_authorities']), 10)
                self.assertEqual(snapshot['offered_rate_tps'], 7000)


if __name__ == '__main__':
    unittest.main()
