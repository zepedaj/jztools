from jztools import humanize as mdl
from unittest import TestCase


class TestFunctions(TestCase):
    def test_oom(self):
        self.assertEqual(mdl.oom(0.1, prefixes="num", unit="s"), "100 ms")
        self.assertEqual(mdl.oom(0.001, prefixes="num", unit="s"), "1 ms")

    def test_time(self):
        self.assertEqual(mdl.secs(0.1), "100 ms")
        self.assertEqual(mdl.secs(0.001), "1 ms")
