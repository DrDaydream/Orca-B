"""Shared benchmark clock checks; exercise real transitions with a virtual clock."""
import json
import unittest
from unittest.mock import patch

from benchmark import timing


class TimingTests(unittest.TestCase):
    def run_clock(self, *, delays=None, bad_plan=False, late=0, missing=None,
                  stop_late=0, missing_stop=False, observe_late=False, straddle=False,
                  primary_message=''):
        clock = [1_000_000_000]
        published = []
        ready_at = {'client-0': 0, 'client-2': 2, 'primary-0': 1, 'primary-2': 3}
        ready_at.update(delays or {})
        def read_logs(timeout):
            elapsed = (clock[0] - 1_000_000_000) / 1e6
            output = {}
            for name, delay in ready_at.items():
                log = ''
                if elapsed >= delay and name != missing:
                    log = ('Benchmark clock ready\n' if name.startswith('client')
                           else 'Primary abc successfully booted on 127.0.0.1\n')
                if published and name.startswith('client') and name != missing:
                    plan = dict(published[0])
                    if bad_plan and name == 'client-2':
                        plan['run_id'] = 'wrong-run'
                    log += 'Benchmark clock armed ' + json.dumps(plan) + '\n'
                    if clock[0] >= plan['load_us'] + (2_000_000 if observe_late else 0):
                        actual = plan['load_us'] + late
                        log += f'Benchmark input started at_us {actual} lateness_us {late}\n'
                    if clock[0] >= plan['end_us'] + stop_late and not missing_stop:
                        log += f"Benchmark input stopped at_us {plan['end_us'] + stop_late} released_samples 1\n"
                output[name] = log
            output['primary-0'] += primary_message
            if published and straddle:
                cutoff = published[0]['drain_end_us']
                if cutoff - 1_000_000 <= clock[0] < cutoff:
                    # The first host answers before it exits. The second host
                    # delays returning the combined status until after cutoff.
                    clock[0] = cutoff + 100_000
                elif clock[0] >= cutoff:
                    output['primary-0'] += 'ERROR benchmark process exited\n'
            return output
        def advance(seconds):
            clock[0] += max(1, int(seconds * 1e6))
        with patch.object(timing, 'now_us', side_effect=lambda: clock[0]), \
                patch.object(timing, 'monotonic', side_effect=lambda: clock[0] / 1e6), \
                patch.object(timing, 'sleep', side_effect=advance):
            plan = timing.coordinate(
                ['client-0', 'client-2'], ['primary-0', 'primary-2'],
                read_logs, published.append, duration=10, warmup=2, drain=3,
                run_id='this-run',
            )
        return plan, clock[0]

    def test_every_primary_and_transport_ready_before_future_plan_and_full_drain(self):
        plan, stopped = self.run_clock()
        self.assertEqual(plan, dict(schema=2, run_id='this-run', load_us=1_033_000_000,
                                   start_us=1_035_000_000, end_us=1_045_000_000,
                                   drain_end_us=1_048_000_000))
        self.assertGreaterEqual(stopped, 1_048_000_000)

    def test_wrong_run_acknowledgement_is_rejected(self):
        with self.assertRaisesRegex(timing.ClockError, 'plan|run'):
            self.run_clock(bad_plan=True)

    def test_actual_input_more_than_half_second_late_is_rejected(self):
        with self.assertRaisesRegex(timing.ClockError, 'late'):
            self.run_clock(late=500_001)

    def test_never_ready_primary_has_bounded_failure_and_is_named(self):
        with self.assertRaisesRegex(timing.ClockError, 'primary-2'):
            self.run_clock(missing='primary-2')

    def test_delayed_log_observation_does_not_change_actual_input_start(self):
        plan, _ = self.run_clock(observe_late=True)
        self.assertEqual(plan['load_us'], 1_033_000_000)

    def test_delayed_stop_marker_does_not_extend_saved_cutoff(self):
        plan, stopped = self.run_clock(stop_late=4_000_000)
        self.assertEqual(plan['drain_end_us'], 1_048_000_000)
        self.assertGreaterEqual(stopped, 1_049_000_000)

    def test_a1_retry_warning_allows_the_clocked_run_to_finish(self):
        _, stopped = self.run_clock(primary_message=(
            '[2026-11-20T18:34:17.845Z WARN  network::reliable_sender] '
            'Failed to connect to 127.0.0.1:3015 (retry 0): '
            'Connection refused (os error 111)\n'
        ))
        self.assertGreaterEqual(stopped, 1_048_000_000)

    def test_rust_error_panic_bind_failure_and_process_exit_reject_the_run(self):
        for message in (
                '[2026-11-20T18:34:17.845Z ERROR primary::core] worker storage failed',
                'Error: worker storage failed',
                "thread 'tokio-runtime-worker' panicked at primary/src/core.rs:10:1:",
                'could not bind: Address already in use (os error 98)',
                'ERROR benchmark process exited'):
            with self.subTest(message=message), self.assertRaisesRegex(timing.ClockError, 'primary-0'):
                self.run_clock(primary_message=message + '\n')

    def test_health_poll_straddling_drain_requires_a_fresh_poll(self):
        with self.assertRaisesRegex(timing.ClockError, 'primary-0'):
            self.run_clock(straddle=True)

    def test_missing_stop_marker_rejects_run(self):
        with self.assertRaisesRegex(timing.ClockError, 'input-stop'):
            self.run_clock(missing_stop=True)

    def test_late_plan_publication_cannot_arm_at_last_moment(self):
        clock = [1_000_000_000]
        plan = []
        def publish(value):
            plan.append(value)
            clock[0] += 26_000_000
        def read_logs(timeout):
            return {'p': 'Primary abc booted on 127.0.0.1', 'c': 'Benchmark clock ready\n' +
                    ('Benchmark clock armed ' + json.dumps(plan[0]) if plan else '')}
        with patch.object(timing, 'now_us', side_effect=lambda: clock[0]), \
                patch.object(timing, 'monotonic', side_effect=lambda: clock[0] / 1e6):
            with self.assertRaisesRegex(timing.ClockError, '5 seconds|acknowledge'):
                timing.coordinate(['c'], ['p'], read_logs, publish,
                                  duration=1, run_id='late-publish')


if __name__ == '__main__':
    unittest.main()
