# Utilities
from jztools.parallelization.mock import MockPoolExecutor
from jztools.py import entity_name, get_caller
from ..utils import ParArgs

from typing import Callable, Union


# ThreadPoolExecutor and ThreadParallelizer
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor
from .._base import _Parallelizer


class _Unassigned:
    pass


class ThreadPoolExecutor(_ThreadPoolExecutor):
    """
    Thin wrapper around :mod:`concurrent.futures.ThreadPoolExecutor`.

    Assignes a default value for `thread_name_prefix` and returns a :class:`MockPoolExecutor` if `max_workers=0`.
    """

    def __init__(self, max_workers=None, thread_name_prefix=_Unassigned, **kwargs):
        super().__init__(
            max_workers=max_workers,
            thread_name_prefix=(
                thread_name_prefix
                if thread_name_prefix is not _Unassigned
                else self.default_thread_name_prefix(3)
            ),
            **kwargs,
        )

    def __new__(cls, **kwargs):
        if kwargs.get("max_workers", None) == 0:
            out = MockPoolExecutor(**kwargs)
        else:
            super_new = super().__new__
            out = super().__new__(
                cls, **({} if super_new is object.__new__ else kwargs)
            )
        return out

    @staticmethod
    def default_thread_name_prefix(offset=1) -> str:
        return entity_name(getattr(obj := get_caller(offset), "__func__", obj))


ThreadPoolExecutor.__init__.__doc__ = ""


class ThreadParallelizer(_Parallelizer):
    """ """

    PoolExecutor = ThreadPoolExecutor

    def __init__(self, *args, thread_name_prefix="", **kwargs):
        super().__init__(*args, thread_name_prefix=thread_name_prefix, **kwargs)

    def run(self, fxn: Union[Callable, Union[Callable], ParArgs], *args, **kwargs):
        # Required for sphinx documentation to show up.
        return super().run(fxn, *args, **kwargs)
