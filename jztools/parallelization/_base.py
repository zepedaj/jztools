import concurrent.futures as cf
from tqdm import tqdm
from typing import Callable, Optional, Union
from .utils import WorkerException, ParArgs
import abc


class _Parallelizer(abc.ABC):
    """
    Easy to use parallelization.
    """

    @property
    @abc.abstractmethod
    def PoolExecutor(self):
        """
        The pool executor class to use.
        """
        pass

    def __init__(
        self,
        do_raise: bool = False,
        verbose=False,
        tqdm_kwargs={},
        **pool_executor_kwargs
    ):
        """
        :param do_raise: If :data:`True`, Raise an exception as soon as it is received internally. Otherwise, will return the exception object.
        :param verbose: Use :mod:`tqdm` to display progress.
        :param tqdm_kwargs: Dictionary of keyword arguments passed to tqdm.
        :param pool_executor_kwargs: Variable keyword args passed to the pool executor initializer.

        """
        self.do_raise = do_raise
        self.pool_executor_kwargs = pool_executor_kwargs

        self.tqdm_bldr = lambda *_args, disable=(
            not verbose
        ), tqdm_kwargs=tqdm_kwargs, **_kwargs: tqdm(
            *_args, disable=disable, **tqdm_kwargs, **_kwargs
        )
        self.tqdm = None

    def run(self, fxn: Union[Callable, Union[Callable], ParArgs], *args, **kwargs):
        """
        Parallelizes a given function over a set of args and kwargs.

        :param fxn: The function to parallelize. Alternatively, can have one function per set of parameters by passing a :class:`ParArgs` of callables. If a list is received, it will be assumed to be a list of callables and converted to a :class:`ParArgs` automatically.
        :param args: can be any combination of standard params passed directly to the callable, or parallelized arguments wrapped in :meth:`ParArgs` to denote parallelization. The iterable over all sets of args/kwargs is produced using :meth:`ParArgs.expand`. Note that only the last entry of ``*args`` can contain ParArgs with keywords, and that :class:`ParArgs` objects cannot be passed as keywords.
        :param kwargs: (See ``args``.)

        .. Rubric:: Example:

        .. code-block::

            def build(what, who, where, size, material, **options):
               \"""Constructs a building\"""
               ...

            # Build two houses in parallel, of the same size, with no pool
            for (args, kwargs),result in Parallelizer().run(
                build,
                'house',  # what
                ParArgs(['contractor', 'myself'], ['LA', 'NY']),  # who, where
                1673,  # size
                ParArgs(['wood', 'brick'], color=['red', 'green']), # material and color option
                with_pool=False):  # pool option
                pass

        """

        # Convert list of callables to ParArgs of callables.
        if isinstance(fxn, list):
            fxn = ParArgs(fxn)

        # Return exception if worker raised an exception
        def result_or_exception(_future):
            try:
                return _future.result()
            except Exception as err:
                if self.do_raise:
                    raise
                else:
                    return WorkerException(err)

        # Send call to different threads.
        with self.PoolExecutor(**self.pool_executor_kwargs) as executor:
            #
            future_to_args = {
                executor.submit(_fxn_and_args[0], *_fxn_and_args[1:], **_kwargs): (
                    _fxn_and_args[1:],
                    _kwargs,
                )
                for _fxn_and_args, _kwargs in ParArgs.expand(fxn, *args, **kwargs)
            }

            # Yield results as they become available.
            self.tqdm = self.tqdm_bldr(
                cf.as_completed(future_to_args), total=len(future_to_args)
            )
            for _future in self.tqdm:
                yield future_to_args[_future], result_or_exception(_future)
