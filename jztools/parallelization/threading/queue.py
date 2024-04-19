"""
Event-interruptible queue put and get loops.

.. _parameters:

.. rubric:: Parameters

.. todo:: Works for multiprocessing too. Move this to another place (not the threading module).
"""

import queue as _queue
from .util import LoopExitRequested
from typing import Dict
from threading import Event

DEFAULT_TIMEOUT = 0.1
"""
Default timeout value used by :func:`put_loop` and :func:`get_loop`.
"""


def check_exit_events(exit_events):
    """
    Raises a: exc: `LoopExitRequested` if one of the exit events is set.
    """
    if sources := {key: event for key, event in exit_events.items() if event.is_set()}:
        raise LoopExitRequested(sources=sources)


def put_loop(
    q, item, exit_events: Dict[str, Event], timeout=DEFAULT_TIMEOUT, pre_check=False
):
    """
    Attempts to put an item in the queue, checking if an exit event was raised every timeout seconds. See: ref: `parameters`.

    :param q: The queue.
    :param item: The item to put in the queue.
    :param timeout: Exit events will be checked every ``timeout`` seconds.
    :param exit_events: A dictionary of events signaling an exit, with the key being a user-friendly name.
    :param pre_check: Checks that none of the exit events are set before attempting to put an item. If ``False``, (the default), this check only happens after a timeout.

    """

    while not pre_check or check_exit_events(exit_events) is None:
        try:
            q.put(item, timeout=timeout)
            break
        except _queue.Full:
            check_exit_events(exit_events)
            pre_check = False
            pass


def get_loop(q, exit_events, timeout=DEFAULT_TIMEOUT, pre_check=False):
    """
    Attempts to get an item from the queue, checking if an exit event was raised every timeout seconds. See: ref: `parameters`.

    :param q: The queue.
    :param timeout: Exit events will be checked every ``timeout`` seconds.
    :param exit_events: A dictionary of events signaling an exit, with the key being a user-friendly name.
    :param pre_check: Checks that none of the exit events are set before attempting to put an item. If ``False``, (the default), this check only happens after a timeout.

    """

    while not pre_check or check_exit_events(exit_events) is None:
        try:
            item = q.get(timeout=timeout)
            return item
        except _queue.Empty:
            check_exit_events(exit_events)
            pre_check = False
            pass
