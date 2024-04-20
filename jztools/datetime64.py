import numpy as np
import re
import warnings
from .datetime import as_tzinfo, TZInfoRep
import pytz
from typing import Union, Tuple, Optional
import datetime

FlexDateTime = Union[np.datetime64, datetime.datetime, str]
"""
A naive date-time as a ``numpy.datetime64`` (string or object) or naive ``datetime``  or timezone-aware ``datetime``. All naive values are assumed to be in UTC by convention.
"""
UTCFlexDateTime = FlexDateTime
"""
The same as :class:`FlexDateTime` but with the UTC convention made explicit.
"""

UTCDateTime64 = np.datetime64
"""
A np.datetime64 in UTC.
"""

_DAY_DTYPE = np.dtype("datetime64[D]")


def date_slot(
    dates: np.ndarray,
    child_resolution: Union[str, np.dtype],
    parent_resolution: Union[str, np.dtype],
    ref_date=np.datetime64("2018-01-01"),
) -> Tuple[np.ndarray, int]:
    """
    Returns an integer specification of the date when counting a specific child time resolution in a given parent time resolution. For example, for child resolution of 1 minute in parent resolution of 1 hour, returns an array of integers in 0,...,59 of the same shape as dates.

    :param dates: A numpy datetime64 array.
    :param child_resolution, parent_resolution: A numpy dtype that is a sub-dtype of ``numpy.timedelta64``, or a string specification of such a dtype. Valid strings include ``'ns'``, ``'us'``, ``'ms'``, ``'s'``, ``'m'``, ``'h'``, ``'D'``, ``'M'``, ``'Y'``. Child resolutions can further be any such string prepended with a string integer to indicate non-unit resolutions (e.g., ``'10m'``). Parent resolutions cannot include an integer. Integer-prepended parent resolutions produce undefined results or raise an exception.
    :param ref_date: The reference date representing the position 0 as a ``numpy.datetime64`` object. Defaults to ``np.datetime64('2018-01-01')`` - a Monday.

    :return: Returns the dates specified as integer positions of the child time resolution in the parent time resolution, and the total number of children. E.g., for child resolution ``'D'`` and parent resoluiton ``'W'``, returns an array integers in the range 0, ..., 6 and a number of children equal to 7.

    """

    if not np.issubdtype(dates.dtype, np.datetime64):
        raise Exception("Need a numpy array of dtype datetime64 or derived.")

    child_resolution = (
        child_resolution
        if np.issubdtype(child_resolution, np.timedelta64)
        else np.dtype(f"np.timedelta64[{child_resolution}]")
    )
    parent_resolution = (
        parent_resolution
        if np.issubdtype(parent_resolution, np.timedelta64)
        else np.dtype(f"np.timedelta64[{parent_resolution}]")
    )
    child_dtype = np.dtype(child_resolution.str.upper())
    num_children = np.array(1, parent_resolution) / np.array(1, child_resolution)

    if (num_children - int(num_children)) != 0:
        raise Exception(
            f"Parent resolution {parent_resolution} does not divide equally "
            f"into child resolution {child_resolution}."
        )
    num_children = int(num_children)

    ref_date = ref_date.astype(child_dtype)
    posns = ((dates.astype(child_dtype) - ref_date).view(dtype="i8")) % num_children
    return posns, num_children


def weekday(dates: np.ndarray, *args):
    """
    Returns the day of the week as an integer in 0,..,6, with 0 corresponding to the day of the week of the specified reference date (defaults to Monday as 0).

    :param ref_date: Same as :func:`get_time_slot`.

    .. testcode::

      from jztools.datetime64 import weekday

      assert weekday(np.array(['2018-01-01'], dtype=='datetime64')) == 0

    """
    return date_slot(
        dates, np.dtype("timedelta64[D]"), np.dtype("timedelta64[W]"), *args
    )[0]


def monthday(dates: np.ndarray):
    """
    Returns the day of the month as an integer in 0,..,30.
    """
    return (
        (dates.astype("datetime64[D]") - dates.astype("datetime64[M]"))
        .view(dtype="timedelta64[D]")
        .view(dtype="i8")
    )


def month(dates: np.ndarray, *args):
    """
    Returns the month as an integer in 0,..,11.
    """
    return date_slot(
        dates, np.dtype("timedelta64[M]"), np.dtype("timedelta64[Y]"), *args
    )[0]


_UNIX_EPOCH = np.datetime64(0, "s")
_ONE_SECOND = np.timedelta64(1, "s")


def as_datetime(
    dt64: np.datetime64, tzinfo: Optional[TZInfoRep] = None
) -> Union[datetime.date, datetime.datetime]:
    """
    Converts the ``np.datetime64`` object to a ``datetime.datetime``, for dtypes containing hour or higher resolution,
    or ``datetime.date`` object otherwise.

    If the returned object is a ``datetime.datetime``, it will be localized to the specified ``tzinfo``, if any.
    """

    out = dt64.item()
    if isinstance(out, int):
        warnings.warn(
            f"Loss of precision when converting datetime64 of dtype {dt64.dtype} to datetime.datetime."
        )
        out = datetime.datetime.utcfromtimestamp(out / 1e9)
    # Note that e.g., np.dtype('datetime64[Y]') < np.dtype('datetime64[D]'),
    # since the day-resolution dtype is higher resolution than the year resolution
    # dtype.
    if tzinfo is not None and dt64.dtype > _DAY_DTYPE:
        out = as_tzinfo(tzinfo).localize(out)
    return out


def as_naive_utc(
    in_datetime: FlexDateTime,
    in_tzinfo: Optional[Union[datetime.tzinfo, str]] = None,
) -> np.datetime64:
    """
    Same as :meth:`as_naive` but with ``out_tzinfo = pytz.UTC``.
    """
    return as_naive(in_datetime, in_tzinfo, pytz.UTC)


def as_naive(
    in_datetime: FlexDateTime,
    in_tzinfo: TZInfoRep = None,
    out_tzinfo: TZInfoRep = None,
) -> np.datetime64:
    """
    Takes an input datetime (as an ``str``, ``datetime`` or ``datetime64``) in the specified input
    timezone and converts it into a naive ``datetime64`` in the specified output timezone.
    The input datetime can be naive or timezone-aware. If naive, the ``in_tzinfo``
    parameter must be supplied. Otherwise, that parameter is ignored.

    :return: If the input contains a time-less date (as a string or a `datetime.date` object, or an ``np.datetime64`` object of dtype ``'datetime64[D]'`` or lower resolution), that same date is returned as a ``np.datetime64`` object
    of dtype 'datetime64[D]' (or of the same dtype, for ``np.datetime64`` inputs. If the input does contain a time, the output dtype is ``np.datetime64[us]``.


    :param in_datetime: A ``FlexDateTime`` with the input datetime.
    :param in_tzinfo: If ``in_datetime`` is naive, this will need to be supplied. Otherwise, it is ignored.
    :param out_tzinfo: The output tzinfo of the supplied datetime object.
    """

    if isinstance(in_datetime, datetime.datetime):
        # Input is a datetime.datetime
        if in_datetime.tzinfo is None:
            # Naive input datetime
            in_datetime = as_tzinfo(in_tzinfo).localize(in_datetime)

    else:
        # String, datetime.date or np.datetime64
        in_datetime64 = np.datetime64(in_datetime)
        in_datetime = in_datetime64.item()
        if not isinstance(in_datetime, datetime.datetime):
            # Returns a non-time date without localization.
            return in_datetime64
        in_datetime = as_tzinfo(in_tzinfo).localize(in_datetime)

    return np.datetime64(
        in_datetime.astimezone(as_tzinfo(out_tzinfo)).replace(tzinfo=None)
    )


def utc_from_eastern(in_datetime: FlexDateTime):
    """
    Converts a datetime from US/Eastern (standard or dalyight saving depending on the time) to UTC.

    This function is undefined for one hour where it is not possible to infer whether the input
    datetime is in standard time daylight savings time. This should not be a problem if the times are
    outside the overlapping hour each year between the two timezones.
    """
    return as_naive_utc(in_datetime, "US/Eastern")


def parse_numpy_time_dtype(val: Union[np.datetime64, np.timedelta64]) -> dict:
    """
    Returns a dictionary with fields 'type' ('datetime64' or 'timedelta64'), 'num' (number of
    date time units) and 'dt_unit' (datetime unit, e.g., 'D', 'm', 's', 'h')
    """
    rem = re.match(
        "^(?P<type>datetime64|timedelta64)\\[(?P<num>\\d*)(?P<dt_unit>[a-zA-Z]+)\\]$",
        str(val),
    )
    if not rem:
        raise Exception(
            f"Unsupported or invalid datetime64/timedelta64 object '{val}'."
        )
    return rem.groupdict()


def datetime64_dtype_to_timedelta64_dtype(dtype):
    spec = parse_numpy_time_dtype(dtype)
    return np.dtype(f"timedelta64[{int(spec['num'] or 1)}{spec['dt_unit']}]")


def time_dtype_to_item(dtype):
    if np.issubdtype(dtype, np.datetime64):
        wrapper = np.datetime64
    elif np.issubdtype(dtype, np.timedelta64):
        wrapper = np.timedelta64
    else:
        raise Exception(f"Invalid dtype {dtype}.")
    return wrapper(np.array(1, dtype))


def now(res="us", tz: TZInfoRep = None):
    """
    :param res: A numpy datetime64 specifier, e.g.,  one of ('H', 'm', 's', 'ms', 'us', 'ns').
    """
    tz = as_tzinfo(tz) if tz is not None else None
    return np.datetime64(datetime.datetime.now(tz).replace(tzinfo=None), res)


def now_str(*args, **kwargs):
    """
    Returns :func:`now` as a string with ``'T'`` substituted by a space.
    """
    return as_str(now(*args, **kwargs))


def as_str(dt64):
    return str(dt64).replace("T", " ")
