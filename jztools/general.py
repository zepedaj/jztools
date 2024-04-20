from time import time_ns
from typing import Callable


class NotSingleEntry(Exception):
    def __init__(self, num_entries):
        self.num_entries = num_entries
        super().__init__(f"Expected single entry but found {num_entries}.")


class Debouncer:
    """
    Ensures that a callable is executed only if a given delay has elapsed since the last call.

    .. testcode::

        from jztools.general import Debouncer

        def fxn():
            print('Executed')

        debouncer = Debouncer(5)

        debouncer.call(fxn)
        debouncer.call(fxn)

    .. testoutput::

        Executed

    """

    def __init__(self, delay: float):
        """
        :param delay: Seconds to wait before permitting the next execution.
        """
        self.delay_ns = int(delay * 1e9)
        self.last_call_time_ns = time_ns() - self.delay_ns

    def __call__(self, fxn: Callable):
        current_time_ns = time_ns()
        if current_time_ns - self.last_call_time_ns > self.delay_ns:
            self.last_call_time_ns = current_time_ns
            fxn()
