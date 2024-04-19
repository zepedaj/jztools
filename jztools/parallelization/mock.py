from concurrent.futures import Future, Executor
from jztools.py import strict_zip
from concurrent.futures._base import FINISHED
from threading import Lock
from ._base import _Parallelizer


class _Unassigned:
    pass


class MockFuture(Future):
    """
    Mock implementation of :class:`Future`.
    """

    __result = _Unassigned

    def __init__(self, task, lazy=False):
        super().__init__()
        self._state = FINISHED
        self.task = dict(strict_zip(["worker", "args", "kwargs"], task))
        if not lazy:
            self.__build_result()

    def __build_result(self):
        if self.__result is _Unassigned:
            self.__result = self.task["worker"](
                *self.task["args"], **self.task["kwargs"]
            )

    def result(self, timeout=None):
        """"""
        self.__build_result()
        return self.__result


class MockPoolExecutor(Executor):
    """
    https://stackoverflow.com/questions/10434593/dummyexecutor-for-pythons-futures
    """

    def __init__(self, *args, **kwargs):
        self._shutdown = False
        self._shutdownLock = Lock()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass

    def submit(self, fn, *args, **kwargs):
        """
        .. todo:: Computation should not happen here, but rather when checking for results.
        """
        return MockFuture((fn, args, kwargs))

    def shutdown(self, wait=True):
        pass


class MockParallelizer(_Parallelizer):
    PoolExecutor = MockPoolExecutor
