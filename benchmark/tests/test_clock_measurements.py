import json
from contextlib import redirect_stdout
import io
from pathlib import Path
import tempfile
import unittest

from benchmark.measurements import Measurements
from benchmark.logs import LogParser, ParseError


EPOCH_US = 1_000_000_000_000_000
PLAN = {
    'schema': 2,
    'run_id': 'run-clock-1',
    'load_us': EPOCH_US,
    'start_us': EPOCH_US + 2_000_000,
    'end_us': EPOCH_US + 8_000_000,
    'drain_end_us': EPOCH_US + 9_000_000,
}


def line(us, message):
    seconds, micros = divmod(us, 1_000_000)
    from datetime import datetime, timezone
    stamp = datetime.fromtimestamp(seconds, timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    return f'{stamp}.{micros:06d}Z INFO node: {message}\n'


def client(releases=(1_000_000, 3_000_000)):
    armed = json.dumps(PLAN, separators=(',', ':'))
    log = line(EPOCH_US, 'Transactions size: 100 B')
    log += line(EPOCH_US, 'Transactions rate: 1000 tx/s')
    log += line(EPOCH_US, 'Start sending transactions')
    log += line(EPOCH_US, 'Benchmark latency instrumentation version 1')
    log += line(EPOCH_US, 'Benchmark clock ready')
    log += line(EPOCH_US, f'Benchmark clock armed {armed}')
    log += line(EPOCH_US + 200_000,
                f'Benchmark input started at_us {EPOCH_US + 200_000} lateness_us 200000')
    for ident, offset in enumerate(releases):
        log += line(EPOCH_US + offset,
                    f'Benchmark sample released id {ident} at_us {EPOCH_US + offset}')
        log += line(EPOCH_US + offset, f'Sending sample transaction {ident}')
    log += line(EPOCH_US + 8_300_000,
                f'Benchmark input stopped at_us {EPOCH_US + 8_300_000} released_samples {len(releases)}')
    return log


def worker():
    log = line(EPOCH_US, 'Worker booted on 127.0.0.1:3001')
    log += line(EPOCH_US + 1_100_000,
                f'Benchmark sample ingress id 0 at_us {EPOCH_US + 1_100_000}')
    log += line(EPOCH_US + 3_100_000,
                f'Benchmark sample ingress id 1 at_us {EPOCH_US + 3_100_000}')
    for batch, size, ident in [('batchA=', 100, 0), ('batchB=', 200, 1), ('batchC=', 300, None)]:
        samples = [ident] if ident is not None else []
        log += line(EPOCH_US + 1_500_000, f'Benchmark batch {batch} bytes {size} samples {samples}')
        log += line(EPOCH_US + 1_500_000, f'Batch {batch} contains {size} B')
        if ident is not None:
            log += line(EPOCH_US + 1_500_000, f'Batch {batch} contains sample tx {ident}')
    return log


def primary(finals):
    configs = ['Header size set to 1000 B', 'Max header delay set to 200 ms',
               'Garbage collection depth set to 50', 'Sync retry delay set to 10000 ms',
               'Sync retry nodes set to 3', 'Batch size set to 500000 B',
               'Max batch delay set to 200 ms', 'Primary booted on 127.0.0.1:3000']
    log = ''.join(line(EPOCH_US, value) for value in configs)
    for batch, offset in finals.items():
        stamp = EPOCH_US + offset
        log += line(stamp, f'Committed B1(author) -> {batch} @ {stamp // 1000} '
                           f'commit {stamp // 1000} commit_us {stamp}')
    return log


class ClockMeasurementTests(unittest.TestCase):
    def measurements(self, release_logs=None):
        return Measurements(
            [client() if release_logs is None else release_logs],
            [primary({'batchA=': 4_000_000, 'batchB=': 6_000_000,
                      'batchC=': 8_000_000}),
             primary({'batchA=': 5_000_000, 'batchB=': 7_000_000,
                      'batchC=': 8_000_000})],
            [worker()],
        )

    def test_clock_windows_keep_warmup_release_and_exclude_end_boundary(self):
        result = self.measurements().clock_metrics(PLAN)

        self.assertEqual(result['schema'], 2)
        self.assertEqual(result['run_id'], 'run-clock-1')
        self.assertEqual(result['release_window']['released_samples'], 1)
        self.assertEqual(result['release_window']['window_start_us'], PLAN['start_us'])
        self.assertEqual(result['release_window']['window_end_us'], PLAN['end_us'])
        self.assertEqual(result['release_window']['commit_cutoff_us'], PLAN['drain_end_us'])
        commit = result['commit_window']
        self.assertEqual([row['matched_samples'] for row in commit['replicas']], [2, 2])
        self.assertEqual([row['release_to_final']['mean_ms'] for row in commit['replicas']],
                         [3000, 4000])
        self.assertEqual(commit['mean_replica_release_to_final_ms'], 3500)
        self.assertEqual([row['final_bytes'] for row in commit['replicas']], [300, 300])
        self.assertAlmostEqual(commit['mean_replica_final_tps'], 0.5)
        self.assertTrue(commit['coverage_complete'])

    def test_missing_warmup_release_fails_before_estimating_latency(self):
        broken = client().replace(
            line(EPOCH_US + 1_000_000,
                 f'Benchmark sample released id 0 at_us {EPOCH_US + 1_000_000}'), '')
        with self.assertRaisesRegex(ValueError, 'sample release sequence'):
            self.measurements(broken).clock_metrics(PLAN)

    def test_mean_of_means_keeps_an_uneven_replica_population(self):
        metrics = Measurements([client()],
                               [primary({'batchA=': 4_000_000, 'batchB=': 7_000_000}),
                                primary({'batchA=': 6_000_000})], [worker()])
        result = metrics.clock_metrics(PLAN)['commit_window']
        # Replica means are (3s + 4s)/2 = 3.5s and 5s. Equal weight gives 4.25s,
        # unlike the pooled observations (3s + 4s + 5s)/3 = 4s.
        self.assertEqual(result['mean_replica_release_to_final_ms'], 4250)
        self.assertEqual(result['matched_samples'], 3)
        self.assertEqual([row['matched_samples'] for row in result['replicas']], [2, 1])

    def test_empty_replica_keeps_zero_tps_and_null_latency_in_aggregate(self):
        metrics = Measurements([client()],
                               [primary({'batchA=': 4_000_000}), primary({})], [worker()])
        result = metrics.clock_metrics(PLAN)['commit_window']
        self.assertIsNone(result['mean_replica_release_to_final_ms'])
        self.assertEqual(result['mean_replica_final_tps'], 1 / 12)
        self.assertEqual(result['replicas_with_samples'], 1)
        self.assertEqual(result['expected_replicas'], 2)
        self.assertEqual(result['replicas'][1]['final_tps'], 0)
        self.assertIsNone(result['replicas'][1]['release_to_final']['mean_ms'])
        self.assertEqual(result['status'], 'partial')
        self.assertFalse(result['coverage_complete'])

    def test_release_start_and_drain_end_are_half_open(self):
        released = client((1_000_000, 2_000_000))
        metrics = Measurements([released],
                               [primary({'batchA=': 2_000_000, 'batchB=': 9_000_000}),
                                primary({'batchA=': 2_000_000, 'batchB=': 8_999_999})],
                               [worker()])
        result = metrics.clock_metrics(PLAN)
        release, commit = result['release_window'], result['commit_window']
        self.assertEqual(release['released_samples'], 1)
        self.assertEqual([row['completed_samples'] for row in release['replicas']], [0, 1])
        self.assertEqual([row['unfinished_samples'] for row in release['replicas']], [1, 0])
        self.assertEqual([row['matched_samples'] for row in commit['replicas']], [1, 1])
        self.assertEqual(commit['mean_replica_release_to_final_ms'], 1000)

    def test_clocked_batch_bytes_do_not_depend_on_legacy_lines(self):
        worker_log = worker().replace(line(EPOCH_US + 1_500_000,
                                          'Batch batchA= contains 100 B'), '')
        result = Measurements([client()], [primary({'batchA=': 4_000_000})],
                              [worker_log]).clock_metrics(PLAN)['commit_window']
        self.assertTrue(result['coverage_complete'])
        self.assertTrue(result['throughput_coverage_complete'])
        self.assertEqual(result['replicas'][0]['final_bytes'], 100)

    def test_clocked_metrics_reject_missing_final_timestamp_and_runtime_failure(self):
        for log in ('Committed B1(author) -> batchA=\n',
                    primary({'batchA=': 4_000_000}) + line(EPOCH_US, 'ERROR storage failed')):
            with self.subTest(log=log), self.assertRaises(ValueError):
                Measurements([client()], [log], [worker()]).clock_metrics(PLAN)

    def test_missing_ingress_and_invalid_times_remain_explicitly_partial(self):
        missing = worker().replace(line(EPOCH_US + 1_100_000,
                                        f'Benchmark sample ingress id 0 at_us {EPOCH_US + 1_100_000}'), '')
        for log, key in ((missing, 'missing_ingress_samples'),
                         (worker().replace(str(EPOCH_US + 1_100_000),
                                           str(EPOCH_US - 100_000)), 'invalid_time_samples')):
            with self.subTest(key=key):
                result = Measurements([client()], [primary({'batchA=': 4_000_000})],
                                      [log]).clock_metrics(PLAN)['commit_window']
                self.assertEqual(result['replicas'][0][key], 1)
                self.assertIsNone(result['mean_replica_release_to_final_ms'])
                self.assertFalse(result['coverage_complete'])

    def test_same_sample_counter_in_distinct_source_pairs_is_not_merged(self):
        other_worker = worker().replace('batchA=', 'otherA=').replace('batchB=', 'otherB=')
        other_worker = other_worker.replace('batchC=', 'otherC=')
        result = Measurements([client(), client()],
                              [primary({'batchA=': 4_000_000, 'otherA=': 6_000_000})],
                              [worker(), other_worker]).clock_metrics(PLAN)['commit_window']
        self.assertEqual(result['replicas'][0]['matched_samples'], 2)
        self.assertEqual(result['mean_replica_release_to_final_ms'], 4000)

    def test_ambiguous_sample_mapping_sizes_and_timestamps_are_rejected(self):
        cases = [
            (client(), worker() + line(EPOCH_US, 'Benchmark batch conflicting= bytes 100 samples [0]')),
            (client(), worker() + line(EPOCH_US, 'Benchmark batch batchA= bytes 900 samples [0]')),
            (client() + line(EPOCH_US, f'Benchmark sample released id 0 at_us {EPOCH_US + 1_000_001}'), worker()),
        ]
        for client_log, worker_log in cases:
            with self.subTest(worker_log=worker_log), self.assertRaisesRegex(ValueError, 'Ambiguous'):
                Measurements([client_log], [primary({'batchA=': 4_000_000})],
                             [worker_log]).clock_metrics(PLAN)

    def test_malformed_release_event_is_not_silently_dropped(self):
        malformed = client() + line(EPOCH_US, 'Benchmark sample released id nope at_us nope')
        with self.assertRaisesRegex(ValueError, 'Malformed sample'):
            self.measurements(malformed).clock_metrics(PLAN)

    def test_plan_and_per_client_clock_markers_are_required_and_unambiguous(self):
        missing = client().replace('Benchmark clock ready\n', '')
        with self.assertRaisesRegex(ValueError, 'clock ready'):
            self.measurements(missing).clock_metrics(PLAN)

        duplicate = client() + line(EPOCH_US, 'Benchmark clock ready')
        with self.assertRaisesRegex(ValueError, 'ambiguous'):
            self.measurements(duplicate).clock_metrics(PLAN)

        wrong_plan = dict(PLAN, run_id='another-run')
        with self.assertRaisesRegex(ValueError, 'does not match'):
            self.measurements().clock_metrics(wrong_plan)

    def test_missing_atomic_worker_record_in_drain_is_rejected(self):
        log = ''.join(line for line in worker().splitlines(keepends=True)
                      if 'Benchmark batch batchB=' not in line)
        with self.assertRaisesRegex(ValueError, 'batch observation'):
            Measurements([client()], [primary({'batchA=': 4_000_000, 'batchB=': 8_500_000})],
                         [log]).clock_metrics(PLAN)

    def test_legacy_worker_lines_cannot_remove_an_atomic_batch_sample(self):
        log = ''.join(line for line in worker().splitlines(keepends=True)
                      if 'Batch batchA= contains sample tx 0' not in line)
        result = Measurements([client()], [primary({'batchA=': 4_000_000, 'batchB=': 6_000_000})],
                              [log]).clock_metrics(PLAN)
        self.assertEqual(result['commit_window']['replicas'][0]['matched_samples'], 2)

    def test_missing_unfinished_sample_release_is_rejected(self):
        log = ''.join(line for line in client().splitlines(keepends=True)
                      if 'Benchmark sample released id 1 ' not in line)
        with self.assertRaisesRegex(ValueError, 'sample release sequence'):
            self.measurements(log).clock_metrics(PLAN)

    def test_clock_never_uses_log_envelope_or_millisecond_final_for_commit_us(self):
        original = primary({'batchA=': 4_000_000})
        for suffix in ('', ' commit_us 1000000004000000x', ' commit_us 1000000005000000'):
            log = original.replace(' commit_us 1000000004000000', suffix)
            with self.subTest(suffix=suffix), self.assertRaisesRegex(ValueError, 'final commit'):
                Measurements([client()], [log], [worker()]).clock_metrics(PLAN)

    def test_clock_plan_rejects_invalid_types_and_nonpositive_window(self):
        for plan in (dict(PLAN, schema=1), dict(PLAN, start_us=True),
                     dict(PLAN, end_us=PLAN['start_us'])):
            with self.subTest(plan=plan), self.assertRaises(ValueError):
                self.measurements().clock_metrics(plan)

    def test_process_loads_plan_and_comparison_json_uses_its_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'measurement-window.json').write_text(json.dumps(PLAN))
            (root / 'client-0-0.log').write_text(client())
            (root / 'worker-0-0.log').write_text(worker())
            (root / 'primary-0.log').write_text(primary(
                {'batchA=': 4_000_000, 'batchB=': 6_000_000, 'batchC=': 8_000_000}))
            (root / 'primary-1.log').write_text(primary(
                {'batchA=': 5_000_000, 'batchB=': 7_000_000, 'batchC=': 8_000_000}))
            parser = LogParser.process(str(root))
            result = parser.clock_metrics()

            self.assertEqual(result['plan'], PLAN)
            self.assertEqual(result['commit_window']['mean_replica_release_to_final_ms'], 3500)
            self.assertIn('Clocked measurement window', parser.result())

            output = root / 'comparison.json'
            with redirect_stdout(io.StringIO()):
                parser.print_comparison(10, str(output))
            saved = json.loads(output.read_text())
            self.assertEqual(saved['schema'], 2)
            self.assertEqual(saved['run_id'], PLAN['run_id'])
            self.assertIn('release_window', saved)
            self.assertIn('commit_window', saved)
            report = root / 'result.txt'
            parser.print(str(report))
            jsonl = json.loads(Path(str(report) + '.metrics.jsonl').read_text())
            self.assertEqual(jsonl, saved)
            self.assertIn('End-to-end latency: 3,500.000 ms', report.read_text())

    def test_malformed_saved_plan_is_rejected_by_process(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'measurement-window.json').write_text('{')
            with self.assertRaises(ParseError):
                LogParser.process(str(root))

    def test_clocked_logs_without_the_saved_plan_cannot_fall_back_to_legacy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'client-0-0.log').write_text(client())
            (root / 'worker-0-0.log').write_text(worker())
            (root / 'primary-0.log').write_text(primary({'batchA=': 4_000_000}))
            with self.assertRaisesRegex(ParseError, 'measurement-window'):
                LogParser.process(str(root))

    def test_saved_plan_with_missing_logs_reports_missing_measurement_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'measurement-window.json').write_text(json.dumps(PLAN))
            with self.assertRaisesRegex(ParseError, 'Missing.*logs'):
                LogParser.process(str(root))

    def test_planned_reports_preserve_launch_ids_and_excluded_population(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'measurement-window.json').write_text(json.dumps(PLAN))
            (root / 'client-2-0.log').write_text(client())
            (root / 'worker-2-0.log').write_text(worker())
            (root / 'primary-2.log').write_text(primary({'batchA=': 4_000_000}))
            (root / 'primary-10.log').write_text(primary({'batchA=': 3_000_000}))
            (root / 'measurement-population.json').write_text(json.dumps({
                'schema_version': 1, 'primary_ids': [2, 10], 'excluded_primary_ids': [10]}))
            result = LogParser.process(str(root)).clock_metrics()
            self.assertEqual(result['included_primary_ids'], [2])
            self.assertEqual(result['excluded_primary_ids'], [10])
            self.assertEqual(result['release_window']['replicas'][0]['replica'], 2)
            self.assertEqual(result['commit_window']['replicas'][0]['replica'], 2)
            self.assertEqual(result['commit_window']['expected_replicas'], 1)
            self.assertEqual(result['commit_window']['mean_replica_release_to_final_ms'], 3000)


if __name__ == '__main__':
    unittest.main()
