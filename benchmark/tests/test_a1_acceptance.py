"""Offline acceptance tests; no Fabric, AWS credentials or running nodes required."""
import importlib.util
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch
from contextlib import redirect_stdout


SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'a1_acceptance.py'
spec = importlib.util.spec_from_file_location('a1_acceptance', SCRIPT)
acceptance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(acceptance)


def sample_report(case, latency=1000, completed=8, released=10):
    start = 1_000_000_000
    plan = dict(schema=2, run_id='test-run', load_us=start,
                start_us=start + case['warmup_s'] * 1_000_000,
                end_us=start + (case['warmup_s'] + case['duration_s']) * 1_000_000,
                drain_end_us=start + (case['warmup_s'] + case['duration_s']
                                     + case['drain_s']) * 1_000_000)
    commit_rows, release_rows = [], []
    for ident in case['active_launch_ids']:
        commit_rows.append(dict(replica=ident, replica_log=f'primary-{ident}.log',
                                final_batches=10, final_bytes=512 * 6000,
                                final_tps=6000 / case['duration_s'], matched_samples=8,
                                release_to_final=dict(count=8, mean_ms=latency),
                                selected_samples=8, missing_release_samples=0,
                                missing_ingress_samples=0, invalid_time_samples=0,
                                missing_batch_sizes=[], throughput_coverage_complete=True,
                                coverage_complete=True))
        release_rows.append(dict(replica=ident, replica_log=f'primary-{ident}.log',
                                 released_samples=released, completed_samples=completed,
                                 unfinished_samples=released - completed,
                                 not_received_samples=0, missing_ingress_samples=0,
                                 invalid_time_samples=0, paired_samples=completed,
                                 completion_ratio=completed / released,
                                 release_to_final=dict(count=completed, mean_ms=latency)))
    n = len(commit_rows)
    return dict(schema=2, run_id=plan['run_id'], plan=plan,
                commit_window=dict(status='available', coverage_complete=True,
                                   window_start_us=plan['start_us'], window_end_us=plan['end_us'],
                                   window_seconds=case['duration_s'], throughput_coverage_complete=True,
                                   expected_replicas=n, replicas_with_samples=n,
                                   matched_samples=8 * n, replicas=commit_rows,
                                   mean_replica_release_to_final_ms=latency,
                                   mean_replica_final_tps=6000 / case['duration_s']),
                release_window=dict(status='available', source_pairs=n,
                                    instrumented_source_pairs=n,
                                    window_start_us=plan['start_us'],
                                    window_end_us=plan['end_us'],
                                    commit_cutoff_us=plan['drain_end_us'],
                                    released_samples=released, replicas=release_rows))


class MatrixTests(unittest.TestCase):
    def test_exact_toolkit_rounding_and_a1_population(self):
        cases = acceptance.cases()
        self.assertEqual(len(cases), 30)
        expected = {10: [10000, 30000, 50000, 70000, 90000],
                    20: [10000, 30000, 50000, 70000, 90000],
                    35: [9800, 30100, 49700, 70000, 90300]}
        for nodes, rates in expected.items():
            selected = [c for c in cases if c['protocol'] == 'Orca-A' and c['nodes'] == nodes]
            self.assertEqual([c['input_rate_tps'] for c in selected], rates)
            for case in selected:
                f = (nodes - 1) // 3
                self.assertEqual(case['byzantine_ids'], list(range(nodes - f, nodes)))
                self.assertEqual(case['active_launch_ids'], list(range(nodes - f)))
                self.assertEqual(case['offered_rate_tps'], case['input_rate_tps'] // nodes * (nodes - f))
                self.assertEqual((case['warmup_s'], case['duration_s'], case['drain_s']), (10, 60, 10))
                self.assertEqual(case['header_size_bytes'], 20)

    def test_command_only_calls_remote_with_explicit_parameters(self):
        cmd = acceptance.command(acceptance.cases()[0])
        self.assertEqual(cmd[:2], ['fab', 'remote'])
        for flag, value in {'--adversary': 'A1', '--faults': '0', '--byzantine': '3',
                            '--warmup': '10', '--duration': '60', '--drain': '10',
                            '--header-size': '20', '--seed': '7', '--workers': '1',
                            '--tx-size': '512', '--runs': '1'}.items():
            self.assertEqual(cmd[cmd.index(flag) + 1], value)

    def test_plan_is_portable_and_does_not_create_output_or_launch(self):
        with tempfile.TemporaryDirectory() as tmp:
            output, printed = Path(tmp) / 'never-created', io.StringIO()
            with patch.object(acceptance.subprocess, 'Popen') as launch, redirect_stdout(printed):
                result = acceptance.main(['--root', '/home/ubuntu/experiments', '--output', str(output),
                                          '--protocol', 'Orca-A', '--nodes', '35', '--dry-run'])
            self.assertEqual(result, 0)
            self.assertIn('cd /home/ubuntu/experiments/Orca-A/benchmark', printed.getvalue())
            self.assertFalse(output.exists())
            launch.assert_not_called()


class MetricTests(unittest.TestCase):
    def setUp(self):
        self.case = acceptance.cases()[0]

    def test_all_replicas_contribute_with_visible_unfinished_samples(self):
        result = acceptance.evaluate(sample_report(self.case), self.case)
        self.assertTrue(result['passed'])
        self.assertEqual(result['release_completion_ratio_min'], .8)
        self.assertEqual(result['release_unfinished_samples_sum'], 14)
        self.assertEqual(result['commit_final_tps_mean'], 100)
        self.assertEqual(result['replicas'], 7)

    def test_low_completion_is_disclosed_without_undocumented_threshold(self):
        result = acceptance.evaluate(sample_report(self.case, completed=1, released=100), self.case)
        self.assertTrue(result['passed'])
        self.assertEqual(result['release_completion_ratio_min'], .01)
        self.assertIn('conditional', result['interpretation'])

    def test_missing_replica_cannot_pass(self):
        report = sample_report(self.case)
        report['commit_window']['replicas'].pop()
        with self.assertRaisesRegex(ValueError, 'replica'):
            acceptance.evaluate(report, self.case)

    def test_zero_completion_or_missing_instrumentation_cannot_pass(self):
        for change in ('zero', 'instrumentation', 'invalid_time', 'infinite'):
            with self.subTest(change=change):
                report = sample_report(self.case)
                if change == 'zero':
                    report['commit_window']['replicas'][0]['matched_samples'] = 0
                elif change == 'instrumentation':
                    report['release_window']['instrumented_source_pairs'] = 6
                elif change == 'invalid_time':
                    report['release_window']['replicas'][0]['invalid_time_samples'] = 1
                else:
                    report['commit_window']['replicas'][0]['release_to_final']['mean_ms'] = float('inf')
                with self.assertRaises(ValueError):
                    acceptance.evaluate(report, self.case)

    def test_threshold_is_strict_and_each_replica_mean_is_reported(self):
        report = sample_report(self.case, latency=3000)
        result = acceptance.evaluate(report, self.case)
        self.assertFalse(result['passed'])
        self.assertEqual(result['commit_e2e_ms_max'], 3000)

    def test_release_disagreement_is_flagged(self):
        report = sample_report(self.case)
        for row in report['release_window']['replicas']:
            row['release_to_final']['mean_ms'] = 5000
        result = acceptance.evaluate(report, self.case)
        self.assertTrue(result['passed'])
        self.assertTrue(result['window_disagreement'])
        self.assertEqual(result['release_e2e_ms_mean'], 5000)

    def test_wrong_run_id_and_window_cannot_pass(self):
        for change in ('run_id', 'end_us'):
            report = sample_report(self.case)
            if change == 'run_id':
                report['run_id'] = 'stale'
            else:
                report['plan']['end_us'] += 1
            with self.assertRaises(ValueError):
                acceptance.evaluate(report, self.case)

    def test_forged_aggregate_cannot_override_replica_metrics(self):
        report = sample_report(self.case, latency=5000)
        report['commit_window']['mean_replica_release_to_final_ms'] = 1
        with self.assertRaisesRegex(ValueError, 'aggregate'):
            acceptance.evaluate(report, self.case)

    def test_commit_window_labels_and_diagnostics_must_match_evidence(self):
        for key, value in [('window_start_us', 0), ('window_end_us', 0), ('window_seconds', 61),
                            ('throughput_coverage_complete', False)]:
            with self.subTest(key=key):
                report = sample_report(self.case)
                report['commit_window'][key] = value
                with self.assertRaises(ValueError):
                    acceptance.evaluate(report, self.case)
        for key, value in [('missing_release_samples', 1), ('missing_ingress_samples', 1),
                            ('invalid_time_samples', 1), ('missing_batch_sizes', ['missing']),
                            ('selected_samples', 9), ('throughput_coverage_complete', False)]:
            with self.subTest(key=key):
                report = sample_report(self.case)
                report['commit_window']['replicas'][0][key] = value
                with self.assertRaises(ValueError):
                    acceptance.evaluate(report, self.case)


class ArchiveTests(unittest.TestCase):
    def synthetic_archive(self, root, case=None, run_id='native-synthetic-1'):
        """Build real, parseable evidence without Fabric, sockets or an EC2 instance."""
        case = case or acceptance.cases()[5]  # n=20 exercises lexicographic log order.
        plan = sample_report(case)['plan']
        plan['run_id'] = run_id
        archive = root / acceptance.case_key(case) / ('attempt-' + run_id)
        logs = archive / 'logs'
        logs.mkdir(parents=True)
        parser_path = SCRIPT.parents[1] / 'benchmark' / 'measurements.py'
        shutil.copy2(parser_path, archive / 'measurement_parser.py')
        shutil.copy2(SCRIPT, archive / 'acceptance_runner.py')
        (archive / 'settings.json').write_text('{}\n', encoding='utf-8')
        ident = dict(source_sha256={'benchmark/measurements.py': acceptance.sha(parser_path)},
                     runner_sha256=acceptance.sha(SCRIPT),
                     settings_sha256=acceptance.sha(archive / 'settings.json'))
        released = plan['start_us'] + 1_000_000
        ingress = released + 100_000
        final = released + 1_000_000
        for node in case['active_launch_ids']:
            client = f'Transactions size: 512 B\nBenchmark latency instrumentation version 1\n'
            client += f'Transactions rate: {case["input_rate_tps"] // case["nodes"]} tx/s\n'
            client += f'Node address: 127.0.0.1:{3000 + node}\n'
            client += f'Benchmark clock ready\nBenchmark clock armed {json.dumps(plan)}\n'
            client += f'Benchmark input started at_us {plan["load_us"]} lateness_us 0\n'
            client += f'Benchmark sample released id 0 at_us {released}\n'
            client += f'Benchmark input stopped at_us {plan["end_us"]} released_samples 1\n'
            (logs / f'client-{node}-0.log').write_text(client, encoding='utf-8')
            (logs / f'worker-{node}-0.log').write_text(
                f'Benchmark latency instrumentation version 1\n'
                f'Benchmark sample ingress id 0 at_us {ingress}\n'
                f'Benchmark batch batch{node}= bytes 512 samples [0]\n'
                f'Batch batch{node}= contains 512 B\nBatch batch{node}= contains sample tx 0\n', encoding='utf-8')
            (logs / f'primary-{node}.log').write_text(''.join(
                f'Committed B1(author) -> batch{source}= @ {final // 1000} commit {final // 1000} commit_us {final}\n'
                for source in case['active_launch_ids']), encoding='utf-8')
        acceptance.save(logs / 'measurement-window.json', plan)
        native = acceptance.reparse(archive, case, plan)
        snapshot = dict(protocol=case['protocol'].lower(), nodes=case['nodes'],
                        rate=case['input_rate_tps'], run=1, fault_model='crash-stop',
                        active_launch_ids=case['active_launch_ids'], inactive_launch_ids=case['byzantine_ids'],
                        offered_rate_tps=case['offered_rate_tps'], measurement_window=plan,
                        benchmark_parameters=dict(nodes=[case['nodes']], rate=[case['input_rate_tps']],
                                                  duration=case['duration_s'], workers=1, tx_size=512,
                                                  faults=0, runs=1, collocate=True, common_clock=True,
                                                  warmup=case['warmup_s'], drain=case['drain_s']),
                        node_parameters={**acceptance.NODE_PARAMETERS, 'header_size': case['header_size_bytes']},
                        adversary=dict(mode='silent', fault_model='crash-stop', seed=7,
                                       start_round=1, rounds=0, byzantine_ids=case['byzantine_ids'],
                                       bullshark_alternating=False),
                        launch_authorities=[f'authority{i}' for i in reversed(range(case['nodes']))],
                        aba_scheduling='A5-only' if case['protocol'] == 'Orca-B' else 'not-applicable')
        acceptance.save(archive / 'native-results/run.comparison.json', native)
        acceptance.save(archive / 'native-results/run.parameters.json', snapshot)
        acceptance.save(archive / 'public-config/parameters.json', snapshot['node_parameters'])
        acceptance.save(archive / 'public-config/committee.json', dict(authorities={
            name: {'workers': {'0': {'transactions': f'127.0.0.1:{3000 + node}'}}}
            for node, name in enumerate(snapshot['launch_authorities'])}))
        acceptance.save(archive / 'acceptance.json', acceptance.evaluate(native, case))
        meta = dict(status='complete', returncode=0, case=case, identity=ident,
                    attempt=archive.name, run_id=run_id, started_us=plan['load_us'] - 1,
                    finished_us=plan['drain_end_us'] + 1, previous_log_sha256={},
                    fresh_log_sha256={p.name: acceptance.sha(p) for p in logs.glob('*.log')})
        acceptance.save(archive / 'metadata.json', meta)
        acceptance.save(archive.parent / 'selected.json', meta)
        acceptance.seal(archive)
        return archive, case, ident

    def test_native_raw_log_archive_revalidates_and_resume_does_not_launch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive, case, ident = self.synthetic_archive(root)
            result = acceptance.audit_archive(archive, case, ident)
            self.assertTrue(result['passed'])
            self.assertEqual(result['replicas'], 14)
            self.assertEqual(result['commit_e2e_ms_mean'], 1000)
            with patch.object(acceptance.subprocess, 'Popen') as launch:
                acceptance.run_case(case, root / 'unused-bench', root, ident)
            launch.assert_not_called()

    def test_sealed_archive_with_stale_log_or_window_still_fails(self):
        for reason in ('log', 'window', 'native_summary', 'identity'):
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as tmp:
                archive, case, ident = self.synthetic_archive(Path(tmp))
                meta = acceptance.read(archive / 'metadata.json')
                if reason == 'log':
                    meta['previous_log_sha256'] = meta['fresh_log_sha256']
                elif reason == 'window':
                    meta['started_us'] += 100
                elif reason == 'native_summary':
                    path = archive / 'native-results/run.comparison.json'
                    native = acceptance.read(path)
                    native['commit_window']['mean_replica_release_to_final_ms'] = 2000
                    for row in native['commit_window']['replicas']:
                        row['release_to_final']['mean_ms'] = 2000
                    acceptance.save(path, native)
                    acceptance.save(archive / 'acceptance.json', acceptance.evaluate(native, case))
                else:
                    ident = {**ident, 'settings_sha256': 'different-settings'}
                acceptance.save(archive / 'metadata.json', meta)
                acceptance.seal(archive)
                with self.assertRaises(ValueError):
                    acceptance.audit_archive(archive, case, ident)

    def test_full_report_requires_distinct_valid_runs_for_every_case(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan, identities = acceptance.cases(), {}
            for index, case in enumerate(plan):
                _, _, ident = self.synthetic_archive(root, case, f'run-{index}')
                identities[case['protocol']] = ident
            campaign = dict(schema=1, cases=plan, identities=identities)
            report = acceptance.build_report(root, campaign)
            self.assertTrue(report['complete'], report)
            self.assertTrue(report['passed'], report)
            second_selected = root / acceptance.case_key(plan[1]) / 'selected.json'
            original = acceptance.read(second_selected)
            self.synthetic_archive(root, plan[1], 'run-0')
            duplicate = acceptance.build_report(root, campaign)
            self.assertFalse(duplicate['passed'])
            self.assertEqual(duplicate['invalid_cases'], 1)
            self.assertIn('Duplicate native run_id', duplicate['rows'][1]['error'])
            acceptance.save(second_selected, original)
            first = root / acceptance.case_key(plan[0]) / 'selected.json'
            first.unlink()
            report = acceptance.build_report(root, campaign)
            self.assertFalse(report['complete'])
            self.assertFalse(report['passed'])
            self.assertEqual(report['missing_cases'], 1)

    def test_run_archives_fresh_native_evidence_without_aws(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture, case, ident = self.synthetic_archive(root / 'fixture')
            bench = root / 'test-repo' / 'benchmark'
            (bench / 'benchmark').mkdir(parents=True)
            shutil.copy2(fixture / 'measurement_parser.py', bench / 'benchmark/measurements.py')
            shutil.copy2(fixture / 'settings.json', bench / 'settings.json')
            plan = acceptance.read(fixture / 'logs/measurement-window.json')
            clock = [plan['load_us'] * 1000 - 1000]

            class FakeFabric:
                returncode = 0

                def __init__(self, *args, env, **kwargs):
                    self.logs = Path(env['ORCA_BENCHMARK_LOGS_DIRECTORY'])

                def wait(self, timeout):
                    shutil.copytree(fixture / 'logs', self.logs, dirs_exist_ok=True)
                    shutil.copytree(fixture / 'native-results', bench / 'results')
                    shutil.copy2(fixture / 'public-config/committee.json', bench / '.committee.json')
                    shutil.copy2(fixture / 'public-config/parameters.json', bench / '.parameters.json')
                    clock[0] = plan['drain_end_us'] * 1000 + 1000
                    return 0

                def poll(self):
                    return 0

            output = root / 'acceptance'
            with patch.object(acceptance, 'identity', return_value=ident), \
                 patch.object(acceptance, 'git', return_value=''), \
                 patch.object(acceptance.time, 'time_ns', side_effect=lambda: clock[0]), \
                 patch.object(acceptance.subprocess, 'Popen', side_effect=FakeFabric) as launch, \
                 redirect_stdout(io.StringIO()):
                acceptance.run_case(case, bench, output, ident)
            self.assertEqual(launch.call_count, 1)
            selected = acceptance.read(output / acceptance.case_key(case) / 'selected.json')
            self.assertEqual(selected['status'], 'complete')
            result = acceptance.audit_archive(acceptance.selected_archive(output, case, selected), case, ident)
            self.assertTrue(result['passed'])

    def test_failure_before_fabric_launch_cannot_archive_previous_logs(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            fixture, case, ident = self.synthetic_archive(root / 'fixture')
            bench = root / 'repo' / 'benchmark'
            (bench / 'benchmark').mkdir(parents=True)
            shutil.copy2(fixture / 'measurement_parser.py', bench / 'benchmark/measurements.py')
            shutil.copy2(fixture / 'settings.json', bench / 'settings.json')
            shutil.copytree(fixture / 'logs', bench / 'logs')
            with patch.object(acceptance, 'identity', return_value={}), \
                 patch.object(acceptance, 'git', return_value=''), \
                 self.assertRaisesRegex(ValueError, 'identity changed'):
                acceptance.run_case(case, bench, root / 'archive', ident)
            attempt = next((root / 'archive' / acceptance.case_key(case)).glob('attempt-*'))
            self.assertEqual(list((attempt / 'logs').glob('*')), [])

    def test_archived_client_rate_and_target_must_match_the_case(self):
        for declaration in ('Transactions rate: 1 tx/s', 'Node address: 127.0.0.1:9'):
            with self.subTest(declaration=declaration), tempfile.TemporaryDirectory() as folder:
                archive, case, ident = self.synthetic_archive(Path(folder))
                path = archive / 'logs/client-0-0.log'
                name = declaration.split(':', 1)[0]
                log = path.read_text()
                path.write_text(''.join(line + '\n' for line in log.splitlines()
                                        if not line.startswith(name + ':')) + declaration + '\n')
                metadata = acceptance.read(archive / 'metadata.json')
                metadata['fresh_log_sha256'][path.name] = acceptance.sha(path)
                acceptance.save(archive / 'metadata.json', metadata)
                acceptance.seal(archive)
                with self.assertRaisesRegex(ValueError, 'configured rate|worker target'):
                    acceptance.audit_archive(archive, case, ident)

    def test_hash_manifest_detects_changed_missing_and_added_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'log.txt').write_text('original', encoding='utf-8')
            acceptance.seal(root)
            acceptance.verify_seal(root)
            (root / 'log.txt').write_text('modified', encoding='utf-8')
            with self.assertRaises(ValueError):
                acceptance.verify_seal(root)
            (root / 'log.txt').write_text('original', encoding='utf-8')
            (root / 'extra.txt').write_text('extra', encoding='utf-8')
            with self.assertRaises(ValueError):
                acceptance.verify_seal(root)

    def test_missing_full_case_set_is_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            campaign = dict(schema=1, cases=acceptance.cases(), identities={})
            result = acceptance.build_report(root, campaign)
            self.assertFalse(result['passed'])
            self.assertFalse(result['complete'])
            self.assertEqual(result['planned_cases'], 30)
            self.assertEqual(result['missing_cases'], 30)

    def test_changed_campaign_options_do_not_resume_old_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            acceptance.open_campaign(root, acceptance.cases(), 'common-clock')
            with self.assertRaisesRegex(ValueError, 'campaign'):
                acceptance.open_campaign(root, acceptance.cases(warmup=0), 'common-clock')

    def test_no_existing_directory_is_silently_adopted_as_campaign(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'old-results.json').write_text('{}', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'empty'):
                acceptance.open_campaign(root, acceptance.cases(), 'common-clock')


if __name__ == '__main__':
    unittest.main()
