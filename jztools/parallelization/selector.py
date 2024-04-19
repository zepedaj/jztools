"""
Programatic/environment variable-based selection of thread-based parallelization, process-based parallelization, or mock parallelization.
"""

import os
from jztools.validation import check_option

from .threading import ThreadPoolExecutor, ThreadParallelizer
from .multiprocessing import ProcessPoolExecutor, ProcessParallelizer
from .mock import MockPoolExecutor, MockParallelizer

# Parallelization definitions.
_PARTYPE_ORDER = ["MOCK", "THREAD", "PROCESS"]
_PARALLELIZER_CLASSES = {
    "THREAD": ThreadParallelizer,
    "PROCESS": ProcessParallelizer,
    "MOCK": MockParallelizer,
}

_POOL_EXECUTOR_CLASSES = {
    "PROCESS": ProcessPoolExecutor,
    "THREAD": ThreadPoolExecutor,
    "MOCK": MockPoolExecutor,
}
_CLASS_SPECIFIC_KWARGS = {"THREAD": set(["thread_name_prefix"])}

# Choose max parallelization based on envrionment variable.
JZTOOLS_MAX_PARTYPE = (
    check_option(
        ENV_VAR_NAME := "JZTOOLS_MAX_PARTYPE",
        os.getenv(ENV_VAR_NAME),
        valid_options := _PARTYPE_ORDER,
        ignore_list=[None, ""],
    )
    or _PARTYPE_ORDER[-1]
)
"""
Maximum parallelization type returned by :class:`Parallelizer` or :class:`PoolWorker`. Parallelization types are assumed to have the following increasing order: ``MOCK, THREAD, PROCESS``. Setting a max order of ``MOCK`` will change all requested types to ``MOCK``. The value of this variable is set from the eponymous environment variable ``JZTOOLS_MAX_PARTYPE``.
"""
# f"""
# {valid_options} Controls what class will be returned by :class:`Parallelizer` or :class:`PoolWorker`. Defaults to {_PARTYPE_ORDER[-1]}. Will be read from the environment variable JZTOOLS_MAX_PARTYPE.
# """

if JZTOOLS_MAX_PARTYPE != _PARTYPE_ORDER[-1]:
    print(
        f"*** JZTOOLS PARALLELIZATION: Constraining max parallelization to {JZTOOLS_MAX_PARTYPE} ***"
    )


# Helper functions.
def _partype_selector(preferred_type):
    posn = min(
        _PARTYPE_ORDER.index(preferred_type), _PARTYPE_ORDER.index(JZTOOLS_MAX_PARTYPE)
    )
    return _PARTYPE_ORDER[posn]


def _init(classes, preferred_partype, max_workers=None, **kwargs):

    chosen_partype = (
        "MOCK"
        if (isinstance(max_workers, int) and max_workers <= 0)
        else _partype_selector(preferred_partype)
    )
    chosen_class = classes[chosen_partype]

    if preferred_partype != chosen_partype:

        # Remove kwargs used by other classes.
        kept_kwargnames = set(kwargs)
        for other_partype, other_kwargnames in _CLASS_SPECIFIC_KWARGS.items():
            if other_partype != chosen_partype:
                kept_kwargnames -= set(other_kwargnames)

        # Add kwargs for this classes.
        kept_kwargnames = kept_kwargnames.union(
            _CLASS_SPECIFIC_KWARGS.get(chosen_partype, set())
        )
        kwargs = {_key: kwargs[_key] for _key in kept_kwargnames if _key in kwargs}

    return chosen_class(max_workers=max_workers, **kwargs)


# Public functions.
def Parallelizer(preferred_partype, **kwargs):
    """
    Returns a runtime-determined parallelizer. The paralleliztion type is determined by ``preferred_partype`` and the maximum allowable parallelization type :attr:`PBLIG_MAX_PARTYPE`.

    See :class:`~jztools.jztools.parallelization.mock.MockParallelizer`, :class:`~jztools.jztools.parallelization.threading.ThreadParallelizer`, or :class:`~jztools.jztools.parallelization.multiprocessing.ProcessParallelizer`.

    :param preferred_partype: One of ``'MOCK', 'THREAD', 'PROCESS'``. the type of parallelizer returned will be the mininum of this value and :attr:`JZTOOLS_MAX_PARTYPE`.
    :param kwargs: Extra arguments to pass to the requested parallelizer class.
    """
    return _init(_PARALLELIZER_CLASSES, preferred_partype, **kwargs)


def PoolExecutor(preferred_partype, **kwargs):
    """
    Returns a runtime-determined pool executor. The paralleliztion type is determined by ``preferred_partype`` and the maximum allowable parallelization type :attr:`PBLIG_MAX_PARTYPE`.

    :param preferred_partype: *(See the corresponding parameter in* :class:`Parallelizer` *)*
    :param kwargs: Extra arguments to pass to the requested parallelizer class. See :class:`~jztools.jztools.parallelization.mock.MockPoolExecutor`, :class:`~jztools.jztools.parallelization.threading.ThreadPoolExecutor`, or :class:`~jztools.jztools.parallelization.multiprocessing.ProcessPoolExecutor`.
    """
    return _init(_POOL_EXECUTOR_CLASSES, preferred_partype, **kwargs)
