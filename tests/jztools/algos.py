from dataclasses import dataclass, field
import random
from jztools import algos
from unittest import TestCase
import numpy as np


class TestHeaps(TestCase):
    def test_min_heap(self):
        mh = algos.MinHeap(N=3)
        [mh.insert(_x) for _x in [100, 0, 20, 10]]
        self.assertEqual(list(mh), [0, 10, 20])

    def test_max_heap(self):
        mh = algos.MaxHeap(N=3)
        [mh.insert(_x) for _x in [-np.inf, 20, 100, 10, 0, 0, -np.inf]]
        self.assertEqual(list(mh), [100, 20, 10])

    def test_key(self):
        mh = algos.MaxHeap(N=3, key=lambda x: x[1])
        [mh.insert(_x) for _x in [(None, 100), (None, 0), (None, 20), (None, 10)]]
        self.assertEqual(list(mh), [(None, 100), (None, 20), (None, 10)])

    def test_ordered(self):
        @dataclass
        class Entry:
            x: str = "a"
            y: float = field(default_factory=lambda: random.gauss(0, 1))

        h = algos.OrderedHeap(key=lambda e: e.y)
        for k in range(20):
            h.insert(Entry())

        values = list(h)
        sorted_values = list(values)
        sorted_values.sort(key=lambda e: e.y)
        assert sorted_values == list(values)
