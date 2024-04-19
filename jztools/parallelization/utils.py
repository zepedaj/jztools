from ..py import strict_zip
from typing import Iterable
import itertools as it


class WorkerException:
    """
    Wraps an exception raised within a worker thread. Wrapping the exception enables differentiating between exceptions raised by a funciton and exceptions returned by a function as part of normal operation.
    """

    def __init__(self, error):
        self.error = error

    def __str__(self):
        return str(self.error)


class ParArgs:
    """
        Defines parameters that need to be parallelized over. All arguments and keyword arguments are sequences of the same length. The n-th entry of all sequences contains the n-th  set of args / kwargs. Lengths will be checked at initialization for sequences having :attr:`__len__` or at run-time otherwise.

    To comibne fixed and parallelized arguments, see :meth:`expand`

        Example:

        .. code-block::

            # To denote arguments for the following calls:
            # fxn(0, 'a', alpha=10, beta=50)
            # fxn(1, 'b', alpha=20, beta=60)
            # fxn(2, 'c', alpha=30, beta=70)
            ParArgs([0,1,2], ['a', 'b', 'c'], alpha=[10, 20, 30], beta=[50, 60, 70])
    """

    def __init__(self, *arg_seqs: Iterable, **kwarg_seqs: Iterable):
        """
        :param arg_seqs: Argument sequences for positional arguments
        :param kwarg_seqs: Argument sequences for keyword arguments.
        """
        self.arg_seqs = arg_seqs
        self.kwarg_seqs = kwarg_seqs

        N = None
        for _arg in it.chain(arg_seqs, kwarg_seqs.values()):
            if hasattr(_arg, "__len__"):
                if N is None:
                    N = len(_arg)
                elif N != len(_arg):
                    raise Exception(
                        f"Non-matching argument lengths {N} and {len(_arg)}."
                    )

        self.N = N
        self.consumed = 0
        self.stopped = False

    def __iter__(self):

        if len(self.arg_seqs) == 0:
            fillvalue = tuple()
        elif len(self.kwarg_seqs) == 0:
            fillvalue = {}
        outer_zip_fxn = [
            strict_zip,
            lambda *args, **kwargs: it.zip_longest(
                *args, **kwargs, fillvalue=fillvalue
            ),
        ][len(self.arg_seqs) == 0 or len(self.kwarg_seqs) == 0]

        keys = list(self.kwarg_seqs.keys())
        try:
            for _args, _kwargs in outer_zip_fxn(
                strict_zip(*self.arg_seqs),
                strict_zip(*[self.kwarg_seqs[_key] for _key in keys]),
            ):
                self.consumed += 1
                yield _args, dict(strict_zip(keys, _kwargs))
        except StopIteration:
            self.stopped = True
            raise

    @classmethod
    def _validate_input(cls, *args, **kwargs):
        """
        Checks that args / kwargs consists of a valid sequence of standard and ParArgs arguments. In particular, on ly the last member of *args can have a ParArgs that has keyword sequences, and none of the **kwargs can be ParArgs.
        """
        # Check all ParArgs have the same length if available.
        allNs = [
            _pa.N
            for _pa in it.chain(args, kwargs.values())
            if isinstance(_pa, ParArgs) and _pa.N is not None
        ]
        if len(allNs) > 0 and not all((_n == allNs[0] for _n in allNs[1:])):
            raise ParArgsMisMatchError()

        # Check only the last ParArgs object has keyword args.
        for _k, _arg in enumerate(args):
            if isinstance(_arg, ParArgs):
                if _k != len((args)) - 1 and _arg.kwarg_seqs:
                    raise ParArgsExpandError(
                        "Only the last positional argument can be a ParArgs object with keyword arguments."
                    )

        # Check the kwarg ParArgs don't have any positional arguments.
        if len([_kwarg for _kwarg in kwargs if isinstance(_kwarg, ParArgs)]):
            raise ParArgsExpandError(
                "ParArgs objects cannot be passed as kwargs. Use ParArgs(name=<iterable>) syntax instead."
            )

    @classmethod
    def expand(cls, *args, **kwargs):
        """
        Iterator producing arg/kwarg pairs given args or kwargs possibly containing :class:`ParArgs` objects. Note that only the last :class:`ParArgs` object can contain keyword arguments, and that :class:`ParArgs` objects cannot be passed as keywords.

        .. Rubric:: Example:

        .. code-block::

            # To denote arguments for the following calls:
            # fxn(0, 5, 'a', alpha=10, beta=50, gamma=100)
            # fxn(1, 5, 'b', alpha=20, beta=60, gamma=100)
            # fxn(2, 5, 'c', alpha=30, beta=70, gamma=100)
            ParArgs.expand(
                ParArgs([0, 1, 2]),
                5,
                ParArgs(['a', 'b', 'c'], alpha=[10, 20, 30], beta=[50, 60, 70]),
                gamma=100)

        """

        # Ensures proper format (e.g., no ParArgs in kwargs).
        cls._validate_input(*args, **kwargs)

        if not (any((isinstance(_arg, cls) for _arg in args))):
            # No ParArgs objects.
            if len(args) or len(kwargs):
                yield args, kwargs
            return

        #
        param_iters = [
            iter(_arg) if isinstance(_arg, ParArgs) else iter(it.repeat(([_arg], {})))
            for _arg in args
        ]

        while True:
            args_out = []
            kwargs_out = dict(kwargs)
            for k, _iter in enumerate(param_iters):
                try:
                    args0, kwargs0 = next(_iter)
                except StopIteration:
                    # Check all iterables are exhausted.
                    for _iter2, _obj in zip(param_iters, args):
                        if not isinstance(_obj, ParArgs):
                            continue
                        try:
                            next(_iter2)
                            raise ParArgsMisMatchError()
                        except StopIteration:
                            pass
                    return
                args_out.extend(args0)
                kwargs_out.update(kwargs0)

            yield tuple(args_out), kwargs_out


class ParArgsMisMatchError(Exception):
    """
    Signals that ParArgs objects have different lengths.
    """

    def __init__(self, msg="ParArgs objects have different argument set sizes."):
        super().__init__(msg)


class ParArgsExpandError(Exception):
    """
    Raised when validation fails during expand.
    """

    pass
