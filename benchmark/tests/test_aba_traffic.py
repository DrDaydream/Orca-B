import unittest

from benchmark.aba_traffic import counter_window, format_aba_traffic


def snapshot(wall, elapsed, sent, received):
    return f'Benchmark ABA bytes at_us {wall} elapsed_us {elapsed} sent {sent} received {received}\n'


class AbaTrafficTests(unittest.TestCase):
    def test_directional_bytes_and_monotonic_rates_with_idle_time(self):
        log = (snapshot(10, 1000, 16, 20) + snapshot(15, 1001000, 36, 60)
               + snapshot(25, 2001000, 36, 60))
        window = counter_window(log)
        self.assertEqual((window.sent, window.received, window.seconds), (20, 40, 2))
        idle = counter_window(snapshot(1, 0, 0, 0) + snapshot(2, 1000000, 0, 0))
        report = format_aba_traffic({2: window, 9: idle})
        self.assertIn('replica 2: sent 20 B, received 40 B', report)
        self.assertIn('replica 9: sent 0 B, received 0 B', report)
        self.assertIn('Mean per active replica: send 5.000 B/s, receive 10.000 B/s', report)

    def test_missing_snapshots_resets_and_old_logs(self):
        first = snapshot(10, 100, 10, 20)
        last = snapshot(11, 200, 30, 40)
        for invalid in ('', first, first + first, first + snapshot(10, 150, 9, 30) + last,
                        first + snapshot(11, 50, 10, 20) + last,
                        first + snapshot(11, 150, 20, 15) + last):
            with self.subTest(invalid=invalid):
                self.assertIsNone(counter_window(invalid))
        report = format_aba_traffic({2: counter_window(first + last), 6: None})
        self.assertIn('replica 6: N/A', report)
        self.assertIn('Mean per active replica: N/A', report)
