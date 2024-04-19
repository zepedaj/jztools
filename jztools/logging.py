from functools import wraps
from dataclasses import dataclass
import logging
from coloredlogs import ColoredFormatter
from jztools.py import ErrorEncoder

# from _pytest.logging import ColoredLevelFormatter
from pathlib import Path
from typing import Callable, Optional, Union, List
from jztools import validation
import os
import contextlib
from time import time
from datetime import timedelta, datetime, timezone
import re
import abc

import logging


LOGGER = logging.getLogger(__name__)


# BaseFormatter = ColoredLevelFormatter
ColorlessFormatter = logging.Formatter
# BaseFormatter = logging.Formatter


# Build formatter based on desired functionality
def get_formatter_class(colored, timezone_aware):
    parents = [logging.Formatter]
    if colored:
        parents.insert(0, ColoredFormatter)
    if timezone_aware:
        parents.insert(0, TimeZoneFormatter)

    class Formatter(*parents):
        pass

    return Formatter


class Filter(abc.ABC):
    @abc.abstractmethod
    def filter(self, record): ...


class AndFilter(Filter):
    """
    Logical `and` of all provided filters.
    """

    def __init__(self, *filters):
        self.filters = filters

    def filter(self, record):
        out = 0 if any((_filter.filter(record) == 0 for _filter in self.filters)) else 1
        return out


class FilterSpec:
    name: List[str]
    patterns: List[re.Pattern]

    def __init__(
        self, name: str, patterns: Union[str, List[str]], level: Optional[str] = None
    ):
        self.name = name.split(".")
        self.patterns = [
            re.compile(x)
            for x in ([patterns] if isinstance(patterns, str) else patterns)
        ]  # getattr(logging, "DEBUG")
        if level:
            if level not in ["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
                raise ValueError(f"Invalid level={level}")
            else:
                self.level = getattr(logging, level)
        else:
            self.level = None


@dataclass
class ReBlockingFilter(Filter):
    """Finds all messages that match the specified filter and blocks them"""

    specs: List[FilterSpec]  # FilterSpec("ibapi.client", ".*SENDING.*")]

    def filter(self, record):
        for spec in self.specs:
            if (
                record.name.split(".")[: len(spec.name)] == spec.name
                and (not spec.level or record.levelno <= spec.level)
                and (
                    any(
                        len(re.findall(pattern, record.getMessage())) > 0
                        for pattern in spec.patterns
                    )
                )
            ):
                return 0
            #
        return 1


class BlockingFilter(Filter):
    def __init__(self, name, level, msg=None):
        """
        Blocks all messages for the given name (and children) at the specified or lower level. If msg is specified, it is interpreted as a regular expression. Records will further need to have messages matching the regex msg to be blocked.

                Usage:
                handler.addFilter(BlockingFilter(...))
        """
        self.name = name.split(".")
        self.level = getattr(logging, level) if isinstance(level, str) else level
        self.msg = None if msg is None else re.compile(msg)

    def filter(self, record):
        if (
            record.name.split(".")[: len(self.name)] == self.name
            and record.levelno <= self.level
            and (self.msg is None or len(re.findall(self.msg, record.getMessage())) > 0)
        ):
            return 0
        #
        return 1


@validation.choices("level", ["INFO", "WARNING", "ERROR", "CRITICAL"], doc=False)
def configure_logging_handler(
    level: str = "ERROR",
    log_filter: Optional[Filter] = None,
    filename: Optional[Union[str, Path]] = None,
    timezone: Optional[timezone] = None,
    fmt: Optional[str] = None,
    datefmt: Optional[str] = None,
    name: Optional[str] = "",
):
    """
    Appends a handler to the specified logging module (the root by default). If a filename is specified, this handler will write to that file. Otherwise, it will write to the console.

    .. note::

      The level will only be in effect if it is higher than or equal to the level of the root logger. You can set the level of the root logger as follows:

      .. code-block::

        import logging # Import Python's logging module
        logging.root.setLevel('INFO')

    :param level: Minimum level logged by the created handler.
    :param log_filter: An optional filter, e.g., :class:`AndFilter` or :class:`BlockingFilter`.
    :param filename: If specified, a file handler is created instead of a console handler. If this is a filename, it is used directly as the log output. If a directory, a default filename is created within that directory using the current time (in the ``datefmt`` format) as the name.
    :param timezone: Log times will be in this timezone (the local timezone by default). If specified, the default ``datefmt`` shows the timezone.
    :param fmt: :class:`logging.Formatter` ``fmt`` parameter.
    :param datefmt: :class:`logging.Formatter` ``datefmt`` parameter.
    :param name: The name of the logging module to configure (the root name ``''`` by default).

    :return filename: Returns ``filename``.
    """

    # Build formatter
    # IF CHANGING THE FORMAT, CHANGE ALSO jzfin/tests/pytest.ini
    fmt = (
        fmt
        or "%(levelname)-8s %(asctime)-23s %(name)s:%(lineno)d(%(threadName)s) %(message)s"
    )
    datefmt = datefmt or f"%Y-%m-%d %H:%M:%S{' %Z' if timezone else ''}"

    _Formatter = get_formatter_class(
        colored=filename is None, timezone_aware=bool(timezone)
    )
    formatter = _Formatter(
        fmt=fmt, datefmt=datefmt, **({"timezone": timezone} if timezone else {})
    )

    # Get default filename
    if filename and (filename := Path(filename)).is_dir():
        filename = filename / (datetime.now(timezone).strftime(datefmt) + ".log")

    # Build handler
    if filename is not None:
        handler = logging.FileHandler(filename)
    else:
        handler = logging.StreamHandler()
    handler.setLevel(level)
    if log_filter:
        handler.addFilter(log_filter)
    handler.setFormatter(formatter)

    # Add to logger
    logging.getLogger(name).addHandler(handler)

    return handler


@contextlib.contextmanager
def log_time(name, logger=LOGGER, severity=logging.INFO):
    """
    with log_time(logger, 'operation_name'):
        ...
    """
    t0 = time()
    logger.log(severity, f">>>>>>>> STARTED {name} <<<<<<<<")
    yield None
    logger.log(
        severity, f"<<<<<<<< FINISHED {name} ({timedelta(seconds=time()-t0)}) >>>>>>>>"
    )


def log_exception(logger, error: Exception, level="error"):
    getattr(logger, level)(ErrorEncoder.encode(error))


def logged_exception(
    logger: logging.Logger,
    fxn: Callable[[], None],
    level: str = "error",
    do_raise=False,
):
    @wraps(fxn)
    def wrapper(*args, **kwargs):
        try:
            fxn(*args, **kwargs)
        except Exception as err:
            log_exception(logger, err, level)
            if do_raise:
                raise

    return wrapper


class TimeZoneFormatter:
    """
    override logging.Formatter to use an aware datetime object

    https://stackoverflow.com/questions/32402502/how-to-change-the-time-zone-in-python-logging/47104004
    """

    def __init__(
        self, *args, timezone: timezone = datetime.now().astimezone().tzinfo, **kwargs
    ):
        """
        :param timezone: A tzinfo object to convert times to.
        """
        self.timezone = timezone
        super().__init__(*args, **kwargs)

    def converter(self, timestamp):
        dt = datetime.fromtimestamp(timestamp).astimezone()
        return dt.astimezone(self.timezone)

    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            try:
                s = dt.isoformat(timespec="milliseconds")
            except TypeError:
                s = dt.isoformat()
        return s
