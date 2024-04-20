import pytz
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Union

TZInfoRep = Union[str, pytz.tzinfo.tzinfo]
""" A string representation of a tzinfo (e.g., ``'US/Eastern'``) or a tzinfo. """


def as_tzinfo(tz_in: TZInfoRep) -> pytz.tzinfo.tzinfo:
    """
    Converts a string to a tzinfo object (or returns the tzinfo object).
    """
    return tz_in if isinstance(tz_in, pytz.tzinfo.tzinfo) else pytz.timezone(tz_in)


def tz_offset(tza, tzb) -> timedelta:
    """
    Returns the timezone offset at the current time as a timedelta of tza relative to tzb.
    """
    dta = datetime.now(as_tzinfo(tza))
    dtb = dta.astimezone(as_tzinfo(tzb))

    return dta.replace(tzinfo=None) - dtb.replace(tzinfo=None)


def dt_from_str(in_dt: str, tz: Optional[TZInfoRep]):
    out_dt = np.datetime64(in_dt).item()
    if tz:
        out_dt = pytz.timezone(tz).localize(out_dt)
    return out_dt
