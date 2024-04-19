import time
import numpy as np
import abc
from unittest import TestCase
from jztools.parallelization.utils import ParArgs


class _TestParallelizer(abc.ABC):
    @property
    @abc.abstractmethod
    def Parallelizer(self):
        """
        The Parallelizer class being tested.
        """

    @staticmethod
    def _worker(*args, **kwargs):
        time.sleep(0.1)
        return len(args), len(kwargs)

    def test_run(self):
        #
        num_jobs = 1000
        num_threads = 100
        rng = np.random.default_rng()
        #
        for params, result in self.Parallelizer(max_workers=num_threads).run(
            self._worker,
            1,
            ParArgs(
                [rng.integers(5, size=num_jobs)],
                [rng.integers(5, size=num_jobs)],
                [rng.integers(5, size=num_jobs)],
                a=[rng.integers(5, size=num_jobs)],
                b=[rng.integers(5, size=num_jobs)],
            ),
            c=3,
        ):
            self.assertEqual(tuple(map(len, params)), result)

    def test_do_raise_True(self):
        def _worker(*args, **kwargs):
            raise Exception("Error.")

        # with self.assertRaisesRegex(Exception, 'Exception: Error\.$'):
        with self.assertRaisesRegex(Exception, "^Error\\.$"):
            for params, result in self.Parallelizer(do_raise=True).run(_worker, 0, a=1):
                pass

    def test_do_raise_False(self):
        def _worker(*args, **kwargs):
            raise Exception("Error.")

        with self.assertRaisesRegex(Exception, "^Error\\.$"):
            for params, result in self.Parallelizer(do_raise=False).run(
                _worker, 0, a=1
            ):
                raise result.error
