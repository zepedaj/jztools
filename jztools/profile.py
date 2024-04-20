import cProfile
from functools import wraps
from pstats import SortKey


class profile:
    """

    Usage as a decorator -- the call stats will accumulate and be dumped with every call.

    .. code-block::

        @profile('output_file.prof')
        def function_to_profile(...):
            ...

    Usage as a context manager:

    .. code-block::

        with profile('output_file.prof'):
            ...

    """

    def __init__(self, filename):
        self.filename = filename
        self.profiler = cProfile.Profile()

    # @profile decorator
    def __call__(self, fxn):
        """ """

        @wraps(fxn)
        def wrapper(*args, **kwargs):
            with self.profiler:
                out = fxn(*args, **kwargs)
                self.profiler.dump_stats(self.filename)
                return out

        return wrapper

    # Context manager support
    def __enter__(self):
        self.profiler.__enter__()
        return self.profiler

    def __exit__(self, *args, **kwargs):
        self.profiler.dump_stats(self.filename)
        self.profiler.__exit__(*args, **kwargs)
