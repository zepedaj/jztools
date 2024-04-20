from time import time
import warnings
from contextlib import contextmanager
from datetime import timedelta


class Timer:
    def __init__(self, start=None, verbose=False, name=None):
        """
        with Timer() as t0:
            ...
            print(t0.elapsed)
            ...
        print(t0.elapsed)
        with Timer(t0.end) as t1:
            ...
        print(t1.elapsed)
        """
        warnings.warn("This class is deprecated. Use jztools.profiling.Timer instead.")
        self.start = start if start is not None else time()
        self.verbose = verbose
        self.name = name
        self._elapsed = None

    def __enter__(self):
        return self

    @property
    def elapsed(self):
        if self._elapsed is not None:
            return self._elapsed
        else:
            return time() - self.start

    def __exit__(self, *args, **kwargs):
        self.end = time()
        self._elapsed = self.end - self.start
        if self.verbose:
            _name = "" if self.name is None else f" ({self.name})"
            print(f"Elapsed time{_name}: {timedelta(seconds=self.elapsed)}.")


class Timers:
    def __init__(self):
        """
        timers=Timers()
        with timers('span1') as t0:
            ...

        # Start from end time of t0
        with timers('span2') as t1:
            ...

        # Start from current time
        with timers('span3', timers.last_end) as t2:
            ...

        # Get all elapsed times as a dictionary
        timers.elapsed

        """
        warnings.warn("This class is deprecated. Use jztools.profiling.Timer instead.")
        self._timers = {}
        self._last_timer = None

    @contextmanager
    def __call__(self, name, start=None):
        self._timers[name] = Timer(start=start)
        with self._timers[name] as timer:
            yield timer
            self._last_timer = timer

    @property
    def last_end(self):
        if self._last_timer is None:
            return None
        else:
            return self._last_timer.end

    @property
    def start(self):
        return {name: timer.start for name, timer in self._timers.items()}

    @property
    def end(self):
        return {name: timer.end for name, timer in self._timers.items()}

    @property
    def elapsed(self):
        return {name: timer.elapsed for name, timer in self._timers.items()}
