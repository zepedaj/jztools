"""
Thread-safe profiling utilities to measure time, counts and rates.
"""

from functools import wraps
from collections import namedtuple
from contextlib import ExitStack, AbstractContextManager, contextmanager
import time
import threading
from jztools.parallelization.threading import RepeatTimer
from jztools.py import get_caller_name


class Scalar:
    """
    Keeps track of the min, max, and avg updates.

    .. code-block::

        sc = Scalar(decay=1.0)
        sc.update(1.0) #min, avg, max = 1.0, 1.0,  1.0
        sc.update(0.5) #min, avg, max = 0.5, 0.75, 1.0
        sc.update(1.5) #min, avg, max = 0.5, 1.5,  1.5

    """

    max = -float("inf")
    """ Maximum value of updates."""
    min = float("inf")
    """ Minimum value of updates."""
    _weight = 0.0
    updates = 0
    """ Total number of updates. """
    avg = 0.0
    """ Running average of update values. """
    _dflt_format_string = "min:{min:#0.4g} | avg:{avg:#0.4g} | max:{max:#0.4g}"

    def __init__(self, decay=0.6, format_string=None, lock=None):
        """
        :param decay: Decay to use in weighted average.
        """

        self.decay = decay
        self.format_string = format_string or self._dflt_format_string
        self._lock = lock or threading.RLock()

    def update(self, val):
        """
        All object updates are assumed to be done through this method. Even Time calls :meth:`update` in the :meth:`__exit__` method when used as a context manager.
        """

        with self._lock:
            self.max = max(self.max, val)
            self.min = min(self.min, val)
            self.updates += 1
            #
            old_weight = self._weight
            self._weight = self.decay * self._weight + 1
            self.avg = (self.decay * old_weight * self.avg + val) / self._weight

            return self

    def __str__(self, context={}):
        _context = {key: getattr(self, key) for key in ["min", "avg", "max"]}
        _context.update(context)
        return self.format_string.format(**_context)


class Count(Scalar):
    """
    Similar to :class:`Scalar` but also accumulates updates and exposes a += operator.
    Values can be real numbers and not only integers. In effect, the class accumulates
    whatever values are passed to it.

    .. code-block::

        sc = Count(decay=1.0)
        # the update() method and += operator are equivalent
        sc += 1.0 # min, avg, max, total = 1.0, 1.0,  1.0, 1.0
        sc += 0.5 # min, avg, max, total = 0.5, 0.75, 1.0, 1.5
        sc += 1.5 # min, avg, max, total = 0.5, 1.5,  1.5, 3.0
    """

    total = 0.0
    """ Sum of all updates. """

    _dflt_format_string = Scalar._dflt_format_string + " | total:{total:#0.4g}"

    def __iadd__(self, val):
        self.update(val)
        return self

    def update(self, val):
        with self._lock:
            self.total += val
            super().update(val)

    def __str__(self, context={}):
        _context = {"total": self.total}
        _context.update(context)
        return super().__str__(_context)


class time_and_print(AbstractContextManager):
    """
    Times a function or code context and prints the time.

    As context manager:
    with time_and_print():
        ...

    As decorator:
    @time_and_print()
    def function(): pass

    """

    def __init__(self, msg=None, bang="*" * 10, signal_enter=False):
        self.msg = msg
        self.bang = bang
        self.timer = None
        self.signal_enter = signal_enter

    def __enter__(self):
        self.msg = self.msg or get_caller_name(2)
        if self.timer is not None:
            raise Exception("Expected self.timer to be None but it is not.")
        self.timer = Time()
        if self.signal_enter:
            print(" ".join([self.bang, f"{self.msg}: Entered.", self.bang]))
        self.timer.__enter__()
        return self

    def __exit__(self, *args):
        self.timer.__exit__()
        print(" ".join([self.bang, f"{self.msg}: {self.timer.elapsed}", self.bang]))
        self.timer = None

    def copy(self):
        return time_and_print(
            msg=self.msg, bang=self.bang, signal_enter=self.signal_enter
        )

    def __call__(self, function):
        self.msg = self.msg or str(function)

        @wraps(function)
        def call(*args, **kwargs):
            with self.copy():
                return function(*args, **kwargs)

        return call


class Time(Count, AbstractContextManager):
    """
        Context manager that accumulates wall time and keeps track of statistics (min,max,avg,total) of each update. Updates happen with each call to in-place adition or context manager exit.

    Attempting to enter an already-entered context manager will raise an error.

        .. code-block::

            t = Time()

            # First update.
            with t:
                sleep(1) # t.elapsed == 1

            # No update
            sleep(1) # t.elapsed == 1

            # Second update.
            with t:
                sleep(1) # t.elapsed == 2


            # Third update.
            t += 2 # t.elapsed == 4


            #
            with t:
                with t: # Raises an error.
                    pass
    """

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.start = None

    def __enter__(self):
        with self._lock:
            if self.start is not None:
                raise Exception("Can not re-start a running timer.")
            self.start = time.time()
            return self

    @property
    def elapsed(self):
        """
        Similar to self.total, except that in can be called inside a context and will include
        partial context time. Outside a context, same as total.
        """
        with self._lock:
            if self.start is None:
                # Outside a context.
                return self.total
            else:
                # Within a context.
                return self.total + time.time() - self.start

    def __exit__(self, *args):
        with self._lock:
            if self.start is not None:
                self += time.time() - self.start
                self.start = None

    @property
    def active(self):
        return self.start is not None


_RatePair = namedtuple("_RatePair", ["time", "count"])


class Rate(Scalar, AbstractContextManager):
    """
    Count that also keeps track of rate. Counts can be real numbers and not just integers.
    Rates are computed every "update_interval" seconds using a spawned thread and tracked using a running average with decay (as any :meth:`Scalar`).

    .. code-block::

        #
        r = Rate()
        with r:
            time.sleep(2.0)
            r+=3
            # r.total ~= 3/2

    """

    def __init__(self, *args, update_interval=1.0, **kwargs):
        """
        Besides the arguments of :class:`Scalar`, also accepts

        :param  update_interval: Determines how often the spawned thread will update rate.

        """
        super().__init__(*args, **kwargs)
        self.time = Time(*args, lock=self._lock, **kwargs)
        self.count = Count(*args, lock=self._lock, **kwargs)
        self.enter_rate_pair = None
        self.last_rate_pair = None
        self.update_interval = update_interval

    @property
    def total(self):
        return 0 if self.time.elapsed == 0 else self.count.total / self.time.elapsed

    def __enter__(self):
        with self._lock:
            if self.active:
                raise Exception("Can not re-start a running rate counter.")
            self.time.__enter__()
            self.enter_rate_pair = _RatePair(
                time=self.time.elapsed, count=self.count.total
            )
            self._rate_updater = RepeatTimer(
                action=self._update_rate, interval=self.update_interval
            )
            self._rate_updater.start()
            return self

    def __exit__(self, *args):
        with self._lock:
            try:
                self.time.__exit__()
                self.enter_rate_pair = None
            finally:
                self._rate_updater.stop()
                self._rate_updater.join()

    @property
    def active(self):
        return self.enter_rate_pair is not None

    def update(self, k):
        with self._lock:
            if not self.active:
                raise Exception("Need to enter a context.")
            self.count += k

    def __iadd__(self, k):
        self.update(k)
        return self

    def _update_rate(self):
        with self._lock:
            if not self.active:
                pass
            ref = self.last_rate_pair or self.enter_rate_pair
            new = _RatePair(self.time.elapsed, self.count.total)
            delta = new.time - ref.time  # self.time.elapsed - ref.time
            rate = 0 if delta == 0 else (new.count - ref.count) / delta
            super().update(rate)
            self.last_rate_pair = new

    def __str__(self):
        return super().__str__({"total": self.total})


class ProfilerGroup:
    _supported = {"Scalar": Scalar, "Count": Count, "Time": Time, "Rate": Rate}

    def __init__(self, *args, **kwargs):
        """
        .. code-block::

            pg = ProfilerGroup(
                ('Time', ['timer1', 'timer2']),
                ('Count', ['counter3', 'counter4']),
                ('Rate', ['rate5', 'rate6']),
                timer3 = Time(), rate7 = Rate())

            with pg:
                # Enters all members.
                ...

        """
        self._members = {
            _name: self._supported[which]() for which, names in args for _name in names
        }

        # Check if any were specified twice.
        doubly_specified = set(kwargs).intersection(self._members)
        if doubly_specified:
            raise Exception(f"Keys {doubly_specified} where specified twice.")
        #
        self._members.update(kwargs)

    def keys(self):
        return self._members.keys()

    def __getitem__(self, idx):
        return self._members[idx]

    def __setitem__(self, idx, val):
        self._members[idx] = val

    def __enter__(self):
        with ExitStack() as stack:
            [
                stack.enter_context(prof)
                for prof in self._members.values()
                if isinstance(prof, AbstractContextManager)
            ]
            self.exit_stack = stack.pop_all().close
            return self

    def __exit__(self, *args):
        self.exit_stack()
