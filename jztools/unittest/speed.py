from unittest.case import skip, _id as _unittest_id
import os
from jztools.validation import check_option

SPEED_TESTS = (
    check_option(
        "SPEED_TESTS", os.getenv("SPEED_TESTS"), ["OFF", "ON"], ignore_list=[None, ""]
    )
    or "OFF"
)
"""
Specifies whether to run speed tests. Possible values are 'ON' or 'OFF'. Defaults to 'OFF'.
"""

SPEED_XTIME = float(os.getenv("SPEED_XTIME", 1.5))
"""
Multiple used to carry speed checks using ``runtime<XTIME*nominal_runtime``.
"""


def speed_tests_on():
    return SPEED_TESTS == "ON"


def speed_test(func):
    """
    Unit test decorator used to indicate whether the test is a speed test (possibly requiring specific hardware).
    """
    if not speed_tests_on():
        return skip(f"Speed tests disabled (set SPEED_TESTS='ON').")(func)
    return func
