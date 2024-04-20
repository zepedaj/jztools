from contextlib import contextmanager, _GeneratorContextManager
from functools import wraps
from inspect import isgeneratorfunction
from threading import Lock
from typing import Callable, ContextManager, Dict, Generator, Type, Union
import os


@contextmanager
def environ(values: Dict[str, str], clear: bool = False) -> Generator[None, None, None]:
    """
    Temporarily updates the enviornment variables using the specified values.
    :param values: New environment variable values as a dictionary
    """
    _environ = dict(os.environ)  # or os.environ.copy()
    try:
        if clear:
            os.environ.clear()
        os.environ.update(values)
        yield
    finally:
        os.environ.clear()
        os.environ.update(_environ)


class _Unassigned:
    pass


class reentrant_context_manager:
    """
    Class decorator or generator function decorator (a substitue for `contextlib.contextmanager`) that
    keeps track of number of entries to ensure a single one executes, returning that same object in every
    new context entry.

    If the wrapped generator or class takes parameters, these are only used in the top-most entry and ignored
    in all nested entries.
    """

    def __new__(cls, cm: Union[Type[ContextManager], Callable[..., Generator]]):
        if isinstance(cm, type):
            # Class decorator
            # Create reentrant_context_manager
            self = super().__new__(cls)
            self._nested_contexts = 0
            self._results = _Unassigned
            self._lock = Lock()
            self.orig_enter = cm.__enter__
            self.orig_exit = cm.__exit__

            # Patch input class
            cm.__enter__ = lambda cm_self: self.__enter__(cm_self)
            cm.__exit__ = lambda cm_self, *args, **kwargs: self.__exit__(
                cm_self, *args, **kwargs
            )
            return cm

        elif isgeneratorfunction(cm):
            # Generator decorator (replaces contextlib.contextmanager)

            # TODO: also isgenerator(cm) for generator objects?

            @wraps(cm)
            def helper(*args, **kwds):
                return _ReentrantGeneratorContextManager(cm, args, kwds)

            return helper

        else:
            raise Exception("Unexpected case.")

    def __enter__(self, cm_self):
        with self._lock:
            if self._nested_contexts == 0:
                self._results = self.orig_enter(cm_self)
            self._nested_contexts += 1
        return self._results

    def __exit__(self, cm_self, *args, **kwargs):
        with self._lock:
            if self._nested_contexts <= 0:
                raise Exception("Attempted to exit non-entered context.")
            self._nested_contexts -= 1
            if self._nested_contexts == 0:
                self.orig_exit(cm_self, *args, **kwargs)


@reentrant_context_manager
class _ReentrantGeneratorContextManager(_GeneratorContextManager):
    pass
