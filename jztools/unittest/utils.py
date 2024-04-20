from contextlib import contextmanager
import functools
from freezegun import freeze_time as _freeze_time
import numpy as np
from typing import Union
import pytz

from ..freezegun import (
    freeze_time_tz as freeze_time,
)  # For backwards compatibility (since `freeze_time` renamed to `..freezegun.freeze_time_tz`)


def is_skipped(item):
    """
    Checks if a pytest item for a unittest test was skipped.

    .. todo:: Add a test to ensure this works for new versions of unittest / pytest.
    """
    return bool(getattr(item.obj, "__unittest_skip__", False))


@contextmanager
def swapattr(obj, attr, val):
    old_attr = getattr(obj, attr)
    setattr(obj, attr, val)
    yield obj
    setattr(obj, attr, old_attr)
