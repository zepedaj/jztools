from unittest import TestCase
from jztools.parallelization import ThreadParallelizer, ParArgs
from numpy import testing as npt
import numpy as np
import time
import threading
from jztools.pacers import pacers as mdl
from jztools.unittest import speed_test

PERIOD_XTIME = 1.1


class TestRatePacer(TestCase):
    # @speed_test
    def test_rate(self):
        nominal_rate = 100  # 10 ms period
        nominal_period = 1 / nominal_rate
        times = []
        num_iters = 10
        num_threads = 10

        def run(th_id, pacer, iters=num_iters):
            for k in range(iters):
                with pacer:
                    times.append((th_id, time.time()))

        #
        pacer = mdl.RatePacer(nominal_rate)
        threads = [
            threading.Thread(target=run, args=(th_id, pacer))
            for th_id in range(num_threads)
        ]
        [_t.start() for _t in threads]
        [_t.join() for _t in threads]
        #
        times.sort(key=lambda x: x[1])
        times = np.array(times)
        periods = np.diff(times[:, 1])

        self.assertLess(periods.max(), PERIOD_XTIME * nominal_period)
        npt.assert_allclose(periods >= nominal_period, True)

        # Compute actual rate, test within 5% of nominal period
        true_period = (max(times[:, 1]) - min(times[:, 1])) / (
            num_iters * num_threads - 1
        )
        self.assertLess(true_period, PERIOD_XTIME * nominal_period)
        self.assertGreater(true_period, nominal_period)
        # print('ACTUAL PERIOD',  true_period)
        # print('ACTUAL RATE',  1.0/true_period)

    def test_rate_heavy(self):
        nominal_rate = 50
        num_threads = 10
        num_calls = 200

        def run(pacer):
            with pacer:
                return time.time()

        #
        pacer = mdl.RatePacer(nominal_rate)

        #
        call_times = [
            nc
            for _, nc in ThreadParallelizer(
                max_workers=num_threads, do_raise=True, verbose=False
            ).run(run, ParArgs([pacer] * num_calls))
        ]
        self.assertEqual(len(call_times), num_calls)
        #
        call_times.sort()
        rates = 1.0 / np.diff(call_times)
        #
        npt.assert_array_less(rates, nominal_rate)
        npt.assert_array_equal(np.array(rates) >= 0.95 * nominal_rate, True)


class TestConcurrencyPacer(TestCase):
    def test_max_calls(self):
        max_calls = 5
        current_calls = []

        def run(th_id, pacer, iters=100):
            for k in range(iters):
                with pacer:
                    current_calls.append(pacer.current_calls)
                    time.sleep(0.001)

        #
        pacer = mdl.ConcurrencyPacer(max_calls)

        # Run single
        self.assertEqual(pacer.max_calls, max_calls)
        self.assertEqual(pacer.current_calls, 0)
        run(-1, pacer)
        self.assertEqual(pacer.max_calls, max_calls)
        self.assertEqual(pacer.current_calls, 0)

        # Run threads
        self.assertEqual(pacer.max_calls, max_calls)
        self.assertEqual(pacer.current_calls, 0)
        threads = [
            threading.Thread(target=run, args=(th_id, pacer)) for th_id in range(10)
        ]
        [_t.start() for _t in threads]
        [_t.join() for _t in threads]
        #
        npt.assert_array_less(current_calls, max_calls + 1)
        self.assertEqual(pacer.max_calls, max_calls)
        self.assertEqual(pacer.current_calls, 0)
        #
        self.assertEqual(max(current_calls), max_calls)

    def test_max_calls_heavy(self):
        max_calls = 50
        num_threads = 500
        num_calls = 2000
        current_calls = [0]
        lock = threading.Lock()

        def run(pacer):
            with pacer:
                with lock:
                    current_calls[0] += 1
                    val = current_calls[0]
                time.sleep(0.01)
                with lock:
                    current_calls[0] -= 1
                return val

        #
        pacer = mdl.ConcurrencyPacer(max_calls)

        #
        num_concurrent = [
            nc
            for _, nc in ThreadParallelizer(
                max_workers=num_threads, do_raise=True, verbose=False
            ).run(run, ParArgs([pacer] * num_calls))
        ]
        self.assertEqual(len(num_concurrent), num_calls)
        npt.assert_array_less(num_concurrent, max_calls + 1)
        self.assertTrue(
            sum([x == max_calls for x in num_concurrent]) > 0.25 * num_threads
        )
        self.assertEqual([0], current_calls)
