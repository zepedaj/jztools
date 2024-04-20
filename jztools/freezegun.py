"""
Move ``'jzf_interactive_brokers.pacers.pacers'`` to jztools.
"""

# from freezegun import *
from freezegun import configure, freeze_time
from freezegun import __all__ as _fg_all
import pytz
import numpy as np
import functools
from typing import Union


# REQUIRED TO
# 1) KEEP PYTEST BREAKPOINTS FROM NOT DISPLAYING TYPED TEXT
# 2) PYTEST RUNS WITH THE --pdb FROM FROM HANGING.
# SEEE
# https://stackoverflow.com/questions/71584885/ipdb-stops-showing-prompt-text-after-carriage-return
configure(extend_ignore_list=["prompt_toolkit", "tqdm"])


@functools.wraps(freeze_time)
def freeze_time_tz(in_time: Union[str, np.datetime64], timezone: str):
    """

    .. testcode::

        from time import now

        with freeze_time_tz("2020-10-10T10:00", "US/Eastern"):

            assert now(pytz.tzinfo("US/Eastern")).replace(tzinfo=None) == datetime.datetime(2020, 10, 10, 10)

    """
    return freeze_time(pytz.timezone(timezone).localize(np.datetime64(in_time).item()))


__all__ = _fg_all

configure(
    extend_ignore_list=[
        "jzf_interactive_brokers.pacers.pacers",
        "jztools.pacers.pacers",
        "jztools.profiling",
        "jztools.filelock",
        "jztools.timer",
        "queue",
        "prompt_toolkit",  # To make ipdb and pytest work nicely - see  https://stackoverflow.com/questions/71584885/ipdb-stops-showing-prompt-text-after-carriage-return
    ]
)
