from concurrent.futures import ProcessPoolExecutor as _ProcessPoolExecutor
from .._base import _Parallelizer


class ProcessPoolExecutor(_ProcessPoolExecutor):
    """
    Thin wrapper around :class:`concurrent.futures.ProcessPoolExecutor`.
    """


ProcessPoolExecutor.__init__.__doc__ = ""


class ProcessParallelizer(_Parallelizer):
    PoolExecutor = ProcessPoolExecutor


#
