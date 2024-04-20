from unittest import TestCase
import jztools.timer as mdl
from time import sleep, time


class TestTimer(TestCase):
    def test_elapsed(self):
        D = 1.0
        with mdl.Timer() as tmr:
            sleep(D)
        self.assertAlmostEqual(tmr.elapsed, D, 1)


class TestTimers(TestCase):
    def test_elapsed(self):
        D = 0.1
        p = 2
        #
        timers = mdl.Timers()
        with timers("span1") as t0:
            sleep(D)
        self.assertAlmostEqual(timers.elapsed["span1"], D, p)

        # Start from end time of t0
        with timers("span2") as t1:
            sleep(D)
        self.assertAlmostEqual(timers.elapsed["span2"], D, p)

        # Start from current time
        sleep(D)
        with timers("span3", timers.last_end) as t2:
            sleep(D)
        self.assertAlmostEqual(timers.elapsed["span3"], 2 * D, p)
