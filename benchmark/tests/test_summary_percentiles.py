import unittest

from benchmark.measurements import Measurements
from test_clock_measurements import PLAN, client, primary, worker


class CommitWindowPercentileTests(unittest.TestCase):
    def test_quantiles_pool_transaction_replica_observations_in_the_commit_window(self):
        measurements = Measurements([client()], [
            primary({'batchA=': 3_000_000, 'batchB=': 7_000_000}),
            primary({'batchA=': 2_000_000, 'batchB=': 8_750_000}),
        ], [worker()])

        commit = measurements.clock_metrics(PLAN)['commit_window']

        # The first replica's samples are 2 s and 4 s, the second's is 1 s.
        # Its later 5.75 s sample belongs to the drain, outside this window.
        self.assertEqual(commit['mean_replica_release_to_final_ms'], 2000)
        pooled = commit.get('pooled_release_to_final')
        self.assertIsNotNone(pooled)
        self.assertEqual(pooled['count'], 3)
        self.assertEqual(pooled['p50_ms'], 2000)
        self.assertEqual(pooled['p99_ms'], 4000)

    def test_replica_with_no_selected_samples_has_no_weight_in_pooled_quantiles(self):
        measurements = Measurements([client()], [
            primary({'batchA=': 4_000_000}), primary({}),
        ], [worker()])

        commit = measurements.clock_metrics(PLAN)['commit_window']

        self.assertEqual(commit['matched_samples'], 1)
        self.assertFalse(commit['coverage_complete'])
        self.assertIsNone(commit['mean_replica_release_to_final_ms'])
        self.assertIsNotNone(commit['pooled_release_to_final'])
        self.assertEqual(commit['pooled_release_to_final']['p50_ms'], 3000)
        self.assertEqual(commit['pooled_release_to_final']['p99_ms'], 3000)

    def test_missing_or_invalid_selected_sample_withholds_pooled_quantiles(self):
        missing_ingress = ''.join(line for line in worker().splitlines(keepends=True)
                                  if 'Benchmark sample ingress id 1 ' not in line)
        for worker_log, second_commit in ((missing_ingress, 6_000_000), (worker(), 3_000_000)):
            with self.subTest(second_commit=second_commit):
                measurements = Measurements([client()], [
                    primary({'batchA=': 4_000_000, 'batchB=': second_commit}),
                ], [worker_log])
                commit = measurements.clock_metrics(PLAN)['commit_window']
                self.assertEqual(commit['selected_samples'], 2)
                self.assertEqual(commit['matched_samples'], 1)
                self.assertIsNone(commit['pooled_release_to_final'])

    def test_empty_pool_has_no_quantiles(self):
        measurements = Measurements([client()], [primary({})], [worker()])
        commit = measurements.clock_metrics(PLAN)['commit_window']
        self.assertEqual(commit['matched_samples'], 0)
        self.assertIsNone(commit['pooled_release_to_final'])
