import logging
from contextlib import contextmanager
from .util import LoopExitRequested

LOGGER = logging.getLogger(__name__)


class TimedOut(Exception):
    pass


class AcquireTimedOut(TimedOut):
    pass


@contextmanager
def lock_or_fail(lock, timeout, message=None, extra=""):
    #
    if message is None:
        message = "Timed out trying to acquire lock" + (
            f" ({extra})." if extra else "."
        )

    if not lock.acquire(timeout=timeout):
        raise AcquireTimedOut(message)
    try:
        yield
    finally:
        lock.release()


@contextmanager
def lock_or_fail_if_not_none(lock, timeout, message=None, extra=""):
    #
    if lock is None:
        yield
    else:
        with lock_or_fail(lock, timeout, message=message, extra=extra):
            yield


def wait_or_fail(condition, timeout, message=None, extra=""):
    """
    Example:

    .. code-block::

      with condition: #Acquires lock
          wait_or_fail(condition,1.0) # Will fail or continue with acquired lock
          ...
    """
    if message is None:
        message = (
            "Timed out waiting for condition notification" + f" ({extra})."
            if extra
            else "."
        )
    success = condition.wait(timeout=timeout)
    if not success:
        raise AcquireTimedOut(message)


def wait_for_or_fail(condition, timeout, predicate, message=None, extra=""):
    """
    Example:

    .. code-block::

      with condition: #Acquires lock
          wait_for_or_fail(condition,1.0, lambda: True) # Will fail or continue with acquired lock
          ...
    """
    if message is None:
        message = "Timed out when waiting for condition notification" + (
            f" ({extra})." if extra else "."
        )
    success = condition.wait_for(predicate, timeout=timeout)
    if not success:
        raise AcquireTimedOut(message)


def wait_for_loop(condition, timeout, predicate, exit_events):
    """
    Loops until the condition is met or the exit event is set. The exit event is checked every timeout seconds.
    Param exit_event is a dictionary of events, with the key being a user-friendly name.
    """

    while True:
        if sources := {
            key: event for key, event in exit_events.items() if event.is_set()
        }:
            raise LoopExitRequested(sources=sources)
        elif condition.wait_for(predicate, timeout=timeout):
            break
