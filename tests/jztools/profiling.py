from unittest import TestCase
from jztools.parallelization.threading import ThreadParallelizer
from jztools.parallelization import ParArgs
import numpy as np
from time import sleep
import jztools.profiling as mdl


class TestTime(TestCase):
    def test_elapsed(self):
        D = 1.0
        with mdl.Time() as tmr:
            self.assertAlmostEqual(tmr.elapsed, 0, 2)
            sleep(D)
        self.assertAlmostEqual(tmr.elapsed, D, 2)
        sleep(D)
        with tmr:
            self.assertAlmostEqual(tmr.elapsed, D, 2)
            sleep(D)
        self.assertAlmostEqual(tmr.elapsed, 2 * D, 2)

    def test_avg(self):

        rng = np.random.default_rng(0)

        # With decay, fixed val
        tmr = mdl.Time(decay=0.6)
        val = float(rng.integers(10, 100))
        for k in range(10):
            tmr += val
            self.assertAlmostEqual(tmr.avg, val)

        # No decay, changing val
        tmr = mdl.Time(decay=1.0)
        for k in range(10):
            val = k
            tmr += val
            self.assertAlmostEqual(tmr.avg, np.mean(np.arange(k + 1)))
            self.assertEqual(tmr.max, k)
            self.assertEqual(tmr.min, 0)


class TestCount(TestCase):
    def test_all(self):
        c = mdl.Count(decay=0.6)
        c += 1
        self.assertEqual(c.total, 1)
        c += 7
        self.assertEqual(c.total, 8)
        self.assertEqual(c.avg, (7 + c.decay) / 1.6)


class TestRate(TestCase):
    def test_all(self):
        rt = mdl.Rate()
        self.assertEqual(rt.time.total, 0)
        self.assertEqual(rt.total, 0)
        #
        with self.assertRaisesRegex(Exception, "Need to enter a context"):
            rt += 1
        #
        with rt:
            sleep(2.0)
            rt += 5
        self.assertAlmostEqual(rt.time.total, 2, places=1)
        self.assertAlmostEqual(rt.total, 5 / 2, places=1)
        #
        self.assertRegex(
            str(rt),
            r"min:\d+(\.\d+)? | avg:\d+(\.\d+)? | max:\d+(\.\d+)? | totalx:\d+(\.\d+)?",
        )

    def test_threading(self):
        counts = 20
        delay = 2

        #
        def target(k, rate):
            if k == 0:
                sleep(delay)
            rate += 1

        #
        rt = mdl.Rate().__enter__()
        for _ in ThreadParallelizer().run(
            target, ParArgs(range(counts), [rt] * counts)
        ):
            pass
        rt.__exit__()
        #
        self.assertAlmostEqual(rt.total, counts / delay, places=1)


class TestProfilerGroup(TestCase):
    def test_all(self):
        pg = mdl.ProfilerGroup(
            ("Time", ("timer1", "timer2", "timer3")),
            ("Rate", ("rate1", "rate2")),
            ("Count", ("count1", "count2")),
        )
        tmr1 = pg["timer1"]
        pg["timer1"] += 0.1
        self.assertIs(tmr1, pg["timer1"])
        pg["timer2"] += 0.2
        pg["timer3"] += 0.3
        with pg["rate1"], pg["rate2"]:
            sleep(1.0)
            pg["rate1"] += 1
            pg["rate2"] += 2
        pg["count1"] += 10
        pg["count2"] += 20

        self.assertEqual(pg["timer1"].elapsed, 0.1)
        self.assertEqual(pg["timer2"].elapsed, 0.2)
        self.assertEqual(pg["timer3"].elapsed, 0.3)
        self.assertAlmostEqual(pg["rate1"].total, 1, places=2)
        self.assertAlmostEqual(pg["rate2"].total, 2, places=2)
        self.assertEqual(pg["count1"].total, 10)
        self.assertEqual(pg["count2"].total, 20)
