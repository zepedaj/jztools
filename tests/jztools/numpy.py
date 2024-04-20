import jztools.numpy as mdl
import numpy as np
from unittest import TestCase
import numpy.testing as npt
from collections import deque


class TestCircularArray:
    def test_all(self):
        dt = "f"
        ca = mdl.CircularArray(5, dtype=dt)
        npt.assert_array_equal(ca, np.empty(0, dtype=dt))

        for k in range(3):
            ca.insert(k)

        npt.assert_array_equal(ca.get(), np.arange(3, dtype=dt))
        assert len(ca) == 3

        for k in range(3, 5):
            ca.insert(k)

        npt.assert_array_equal(ca.get(), np.arange(5, dtype=dt))

        assert len(ca) == 5

        for k in range(5, 7):
            ca.insert(k)

        npt.assert_array_equal(ca.get(), np.arange(2, 7, dtype=dt))

        assert ca.posn == 2
        assert len(ca) == 5

        for k in range(7, 10):
            ca.insert(k)

        npt.assert_array_equal(ca.get(), np.arange(5, 10, dtype=dt))
        assert len(ca) == 5

    def test_structured(self):

        dt = np.dtype([("a", "datetime64[m]"), ("b", "f"), ("c", "d")])
        samples = np.empty(5, dtype=dt)
        for fld in dt.names:
            samples[fld] = np.arange(len(samples))
        shifted_samples = deque(samples)

        ca = mdl.CircularArray(len(samples), dtype=dt)

        for smpl in samples:
            ca.insert(smpl)
        npt.assert_array_equal(np.array(shifted_samples), ca.get())

        for smpl in samples:
            ca.insert(smpl)
            shifted_samples.rotate(-1)
            npt.assert_array_equal(np.array(shifted_samples), ca.get())

    def test_structured_batch(self):

        dt = np.dtype([("a", "datetime64[m]"), ("b", "f"), ("c", "d")])
        samples = np.empty(5, dtype=dt)
        for fld in dt.names:
            samples[fld] = np.arange(len(samples))
        shifted_samples = deque(samples)

        ca = mdl.CircularArray(len(samples), dtype=dt)

        ca.batch_insert(samples)
        npt.assert_array_equal(np.array(shifted_samples), ca.get())

        for k, _ in enumerate(samples):

            ca_cpy = ca.copy()
            ca_cpy.batch_insert(samples[: (k + 1)])

            shifted_samples.rotate(-1)
            npt.assert_array_equal(np.array(shifted_samples), ca_cpy.get())

            ca.batch_insert([])

            npt.assert_array_equal(np.array(shifted_samples), ca_cpy.get())


class TestFunctions(TestCase):
    def test_argmax_accumulate(self):
        a = np.array([4, 6, 5, 1, 4, 4, 2, 0, 8, 4])
        a_max, a_argmax = mdl.argmax_accumulate(a)

        npt.assert_equal(np.array([4, 6, 6, 6, 6, 6, 6, 6, 8, 8]), a_max)
        npt.assert_equal(np.array([0, 1, 1, 1, 1, 1, 1, 1, 8, 8]), a_argmax)

    def test_encode_decode(self):
        for arr in [
            np.array([]),
            np.array(0),
            np.datetime64("2020-10-10"),
            np.array(["2020-10-10", "2020-10-11"], dtype="datetime64"),
            np.empty((10, 5, 3)),
            mdl.random_array(
                (10, 5, 3), [("f0", "datetime64"), ("f1", "f"), ("f2", "i")]
            ),
        ]:
            npt.assert_array_equal(arr, mdl.decode_ndarray(mdl.encode_ndarray(arr)))
