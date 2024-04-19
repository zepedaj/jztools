from jztools.parallelization import threading as mdl, ParArgs
from jztools.parallelization.mock import MockPoolExecutor
from .._parallelizer import _TestParallelizer
import numpy.testing as npt
import jztools.py as pgpy
import itertools as it
import numpy as np
from unittest import TestCase


def rand_tuple(rng):
    return tuple(rng.integers(5, size=rng.integers(1, 7)))


def rand_dict(rng):
    keys = list("abcdefghijklmnopqrstuvwxyz")
    rng.shuffle(keys)
    N = rng.integers(1, 7)
    return dict(zip(keys[:N], range(N)))


class TestParArgs(TestCase):
    def test_only_args(self):
        arg1_seq = [0, 1, 2]
        arg2_seq = ["a", "b", "c"]
        pa = ParArgs(arg1_seq, arg2_seq)
        self.assertEqual(list(pa), list(zip(zip(arg1_seq, arg2_seq), it.repeat({}))))

    def test_only_kwargs(self):
        arg1_seq = [0, 1, 2]
        arg2_seq = ["a", "b", "c"]
        pa = ParArgs(arg1=arg1_seq, arg2=arg2_seq)
        self.assertEqual(
            list(pa),
            list(
                zip(
                    it.repeat(tuple()),
                    [
                        dict(zip(["arg1", "arg2"], _args))
                        for _args in zip(arg1_seq, arg2_seq)
                    ],
                )
            ),
        )

    def test_mixed(self):
        pa = ParArgs([0, 1], [10, 11], a=[20, 21], b=[30, 31])
        self.assertEqual(
            list(pa), [((0, 10), dict(a=20, b=30)), ((1, 11), dict(a=21, b=31))]
        )

    def test_empty(self):
        self.assertEqual(len(list(ParArgs())), 0)

    def test_raises(self):
        with self.assertRaisesRegex(
            Exception, "Non\\-matching argument lengths 2 and 3."
        ):
            ParArgs([0, 1], [10, 11, 12])
        with self.assertRaisesRegex(
            Exception, "Non\\-matching argument lengths 2 and 3."
        ):
            ParArgs(a=[0, 1], b=[10, 11, 12])
        with self.assertRaisesRegex(
            Exception, "Non\\-matching argument lengths 2 and 3."
        ):
            ParArgs([0, 1], b=[10, 11, 12])

        pa = ParArgs((x for x in [0, 1]), b=(x for x in [10, 11, 12]))
        with self.assertRaises(pgpy.StrictZipException):
            list(pa)

    def test_expand_edge(self):
        rng = np.random.default_rng(0)

        # Only keywords
        self.assertEqual(list(ParArgs.expand(c=30)), [(tuple(), {"c": 30})])

        # Only args
        self.assertEqual(list(ParArgs.expand(0)), [((0,), {})])

        # Keywords and args
        self.assertEqual(list(ParArgs.expand(0, c=30)), [((0,), {"c": 30})])

        # ParArgs only
        self.assertEqual(
            list(ParArgs.expand(ParArgs([0, 1], [2, 3]))), [((0, 2), {}), ((1, 3), {})]
        )

        # ParArgs and args
        self.assertEqual(
            list(ParArgs.expand(ParArgs([0, 1], [2, 3]), 4)),
            [((0, 2, 4), {}), ((1, 3, 4), {})],
        )

        # Keyword ParArgs
        self.assertEqual(
            list(ParArgs.expand(ParArgs(a=[0, 1]))),
            [(tuple(), {"a": 0}), (tuple(), {"a": 1})],
        )

    def test_expand(self):
        num_jobs = 20
        rng = np.random.default_rng(0)
        pass

        args = [
            10,
            ParArgs(
                rng.integers(0, 5, size=num_jobs),
                rng.integers(5, 10, size=num_jobs),
                rng.integers(10, 15, size=num_jobs),
                a=rng.integers(15, 20, size=num_jobs),
                b=rng.integers(20, 25, size=num_jobs),
            ),
        ]
        kwargs = dict(c=30)
        all_params = list(ParArgs.expand(*args, **kwargs))
        #
        self.assertTrue(all((len(_args) == 4 for _args, _kwargs in all_params)))
        self.assertTrue(
            all((set(_kwargs.keys()) == set("abc") for _args, _kwargs in all_params))
        )
        #
        arg0 = [_args[0] for _args, _kwargs in all_params]
        npt.assert_array_equal(arg0, 10)
        #
        for _k in [1, 2, 3]:
            arg_k = np.array([_args[_k] for _args, _kwargs in all_params])
            self.assertTrue((arg_k >= (_k - 1) * 5).all() and (arg_k < _k * 5).all())
        #
        for _kw, mult in zip("ab", [15, 20]):
            arg_k = np.array([_kwargs[_kw] for _args, _kwargs in all_params])
            self.assertTrue((arg_k >= mult).all() and (arg_k < mult + 5).all())
        #
        kwarg_c = [_kwargs["c"] for _args, _kwargs in all_params]
        npt.assert_array_equal(kwarg_c, 30)


class TestParallelizer(_TestParallelizer, TestCase):
    Parallelizer = mdl.ThreadParallelizer


class TestPoolExecutor:
    def test_thread_prefix_name(self):
        with mdl.ThreadPoolExecutor() as executor:
            assert (
                executor._thread_name_prefix
                == "tests.jztools.parallelization.threading.general:TestPoolExecutor.test_thread_prefix_name"
            )

    def test_zero_workers(self):
        with mdl.ThreadPoolExecutor() as executor:
            assert isinstance(executor, mdl.ThreadPoolExecutor)
        with mdl.ThreadPoolExecutor(max_workers=1) as executor:
            assert isinstance(executor, mdl.ThreadPoolExecutor)
        with mdl.ThreadPoolExecutor(max_workers=0) as executor:
            assert isinstance(executor, MockPoolExecutor)
