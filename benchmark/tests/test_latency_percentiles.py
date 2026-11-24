import unittest
from multiprocessing.pool import ThreadPool
from unittest.mock import patch

from benchmark.logs import LogParser


def summary(commits, sent, received):
    client = ('Transactions size: 512\nTransactions rate: 10\n'
              '[1970-01-01T00:00:00Z INFO] Start benchmark\n' + sent)
    primary = ('Header size set 1000\nMax header delay set 200\n'
               'Garbage collection depth set 50\nSync retry delay set 10000\n'
               'Sync retry nodes set 3\nBatch size set 500000\n'
               'Max batch delay set 200\nbooted on 127.0.0.1\n')
    for digest in ('a', 'b', 'c', 'd', 'uncommitted'):
        primary += f'[1970-01-01T00:00:01Z INFO] Created B1(k) -> {digest}=\n'
    for digest, ready_ms in commits:
        primary += f'Committed B1(k) -> {digest}= @ {ready_ms} commit 11000\n'
    worker = 'booted on 127.0.0.1\n' + received
    with patch('benchmark.logs.Pool', lambda: ThreadPool(1)):
        return LogParser([client], [primary], [worker]).result().splitlines()


class LatencyPercentileTests(unittest.TestCase):
    def test_nearest_rank_uses_the_same_samples_as_each_mean(self):
        lines = summary(
            [('a', 2000), ('b', 5000), ('c', 3000), ('d', 9000)],
            '[1970-01-01T00:00:00.500Z INFO] sample transaction 1\n'
            '[1970-01-01T00:00:00.250Z INFO] sample transaction 2\n'
            '[1970-01-01T00:00:00Z INFO] sample transaction 3\n'
            '[1970-01-01T00:00:01.250Z INFO] sample transaction 4\n'
            '[1970-01-01T00:00:00Z INFO] sample transaction 5\n',
            'Batch a= contains sample tx 1\nBatch a= contains sample tx 2\n'
            'Batch c= contains sample tx 3\nBatch d= contains sample tx 4\n'
            'Batch uncommitted= contains sample tx 5\n',
        )
        for line in (
            ' Consensus latency: 3,750 ms', ' Consensus latency P50: 2,000 ms',
            ' Consensus latency P99: 8,000 ms', ' End-to-end latency: 3,500 ms',
            ' End-to-end latency P50: 1,750 ms', ' End-to-end latency P99: 7,750 ms',
        ):
            self.assertIn(line, lines)

    def test_no_commits_and_no_samples(self):
        lines = summary([], '', '')
        for metric in ('Consensus', 'End-to-end'):
            self.assertIn(f' {metric} latency: 0 ms', lines)
            self.assertIn(f' {metric} latency P50: N/A', lines)
            self.assertIn(f' {metric} latency P99: N/A', lines)

    def test_single_zero_latency_is_an_observation(self):
        lines = summary([('a', 2000)],
                        '[1970-01-01T00:00:02Z INFO] sample transaction 1\n',
                        'Batch a= contains sample tx 1\n')
        for percentile in ('P50', 'P99'):
            self.assertIn(f' Consensus latency {percentile}: 1,000 ms', lines)
            self.assertIn(f' End-to-end latency {percentile}: 0 ms', lines)
