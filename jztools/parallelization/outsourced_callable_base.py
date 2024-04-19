from concurrent.futures import Executor, Future
from dataclasses import dataclass, field
from typing import Callable

from typing import List
from jztools.logging import log_exception


@dataclass
class OutsourcedCallable:
    """

    Handy when a callable needs to be passed but it should execute in a different e.g., thread.

    Example:

    .. testcode::

        from jztools.parallelization.threading import ThreadOutsourcedCallable
        from concurrent.futures import ThreadPoolExecutor

        def my_callable(value):
            print("Output of `my_callable`:", value)

        with ThreadPoolExecutor() as executor:

            wrapped_callable = ThreadOutsourcedCallable(executor, my_callable)
            wrapped_callable(5)

            wrapped_callable.wait()

    .. testoutput::

        Output of `my_callable`: 5

    """

    executor: Executor
    callable: Callable
    futures: List[Future] = field(init=False, default_factory=list)

    def __call__(self, *args, **kwargs):
        self.futures.append(self.executor.submit(self.callable, *args, **kwargs))

    def wait(self):
        out = []
        while self.futures:
            out.append(self.futures.pop().result())
        return out
