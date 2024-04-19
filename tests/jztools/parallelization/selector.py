from unittest import TestCase, mock
import os
import jztools.parallelization.selector as mdl
import importlib
from jztools.parallelization import (
    MockParallelizer,
    ThreadParallelizer,
    ProcessParallelizer,
    MockPoolExecutor,
    ThreadPoolExecutor,
    ProcessPoolExecutor,
)


class TestHelpers(TestCase):
    def test_partype_selector(self):
        for split_index in range(len(mdl._PARTYPE_ORDER)):
            for preferred_partype in mdl._PARTYPE_ORDER[:split_index]:

                # Preferred is low
                for max_partype in mdl._PARTYPE_ORDER[split_index:]:
                    with mock.patch.dict(os.environ, {mdl.ENV_VAR_NAME: max_partype}):
                        importlib.reload(mdl)
                        self.assertEqual(
                            mdl._partype_selector(preferred_partype), preferred_partype
                        )

                # Preferred too high
                for max_partype in mdl._PARTYPE_ORDER[
                    : mdl._PARTYPE_ORDER.index(preferred_partype)
                ]:
                    with mock.patch.dict(os.environ, {mdl.ENV_VAR_NAME: max_partype}):
                        importlib.reload(mdl)
                        self.assertEqual(
                            mdl._partype_selector(preferred_partype), max_partype
                        )

    def test_Parallelizer(self):
        os.unsetenv(mdl.ENV_VAR_NAME)
        importlib.reload(mdl)
        self.assertIsInstance(mdl.Parallelizer("MOCK"), MockParallelizer)
        self.assertIsInstance(mdl.Parallelizer("THREAD"), ThreadParallelizer)
        self.assertIsInstance(mdl.Parallelizer("PROCESS"), ProcessParallelizer)

        with mock.patch.dict(os.environ, {mdl.ENV_VAR_NAME: "PROCESS"}):
            importlib.reload(mdl)
            self.assertIsInstance(mdl.Parallelizer("MOCK"), MockParallelizer)
            self.assertIsInstance(mdl.Parallelizer("THREAD"), ThreadParallelizer)
            self.assertIsInstance(mdl.Parallelizer("PROCESS"), ProcessParallelizer)

        with mock.patch.dict(os.environ, {mdl.ENV_VAR_NAME: "THREAD"}):
            importlib.reload(mdl)
            self.assertIsInstance(mdl.Parallelizer("MOCK"), MockParallelizer)
            self.assertIsInstance(mdl.Parallelizer("THREAD"), ThreadParallelizer)
            self.assertIsInstance(mdl.Parallelizer("PROCESS"), ThreadParallelizer)

        with mock.patch.dict(os.environ, {mdl.ENV_VAR_NAME: "MOCK"}):
            importlib.reload(mdl)
            self.assertIsInstance(mdl.Parallelizer("MOCK"), MockParallelizer)
            self.assertIsInstance(mdl.Parallelizer("THREAD"), MockParallelizer)
            self.assertIsInstance(mdl.Parallelizer("PROCESS"), MockParallelizer)

    def test_PoolExecutor(self):
        os.unsetenv(mdl.ENV_VAR_NAME)
        importlib.reload(mdl)
        self.assertIsInstance(mdl.PoolExecutor("MOCK"), MockPoolExecutor)
        mdl.PoolExecutor("THREAD")
        self.assertIsInstance(mdl.PoolExecutor("THREAD"), ThreadPoolExecutor)
        self.assertIsInstance(mdl.PoolExecutor("PROCESS"), ProcessPoolExecutor)

        with mock.patch.dict(os.environ, {mdl.ENV_VAR_NAME: "PROCESS"}):
            importlib.reload(mdl)
            self.assertIsInstance(mdl.PoolExecutor("MOCK"), MockPoolExecutor)
            self.assertIsInstance(mdl.PoolExecutor("THREAD"), ThreadPoolExecutor)
            self.assertIsInstance(mdl.PoolExecutor("PROCESS"), ProcessPoolExecutor)

        with mock.patch.dict(os.environ, {mdl.ENV_VAR_NAME: "THREAD"}):
            importlib.reload(mdl)
            self.assertIsInstance(mdl.PoolExecutor("MOCK"), MockPoolExecutor)
            self.assertIsInstance(mdl.PoolExecutor("THREAD"), ThreadPoolExecutor)
            self.assertIsInstance(mdl.PoolExecutor("PROCESS"), ThreadPoolExecutor)

        with mock.patch.dict(os.environ, {mdl.ENV_VAR_NAME: "MOCK"}):
            importlib.reload(mdl)
            self.assertIsInstance(mdl.PoolExecutor("MOCK"), MockPoolExecutor)
            self.assertIsInstance(mdl.PoolExecutor("THREAD"), MockPoolExecutor)
            self.assertIsInstance(mdl.PoolExecutor("PROCESS"), MockPoolExecutor)
