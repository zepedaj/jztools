from contextlib import ExitStack, AbstractContextManager, contextmanager
from torch.cuda import Event
from jztools.profiling import Count
from enum import Enum


class CUDATime(AbstractContextManager):
    """
    Time measurement context manager that users `torch.cuda.Event`. Unlike `jztools.profiling.Time`, this timer cannot be reused to accumulate time across multiple context managers.
    .. code-block::

        t = Time()

        # Time operation
        tensor = torch.ones(1000) #optionally, specify device, e.g., ``device='cuda'``.
        with t:
            out = tensor+tensor

        # Elapsed time in seconds:
        t.elapsed()
    """

    _states = Enum("CUDATime_states", ["ready", "entered", "exited"])

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)
        self._state = self._states.ready

    def __enter__(self):
        if self._state is not self._states.ready:
            raise Exception("Can not re-use a CUDATime object.")
        self.start = Event(enable_timing=True)
        self.end = Event(enable_timing=True)
        self.start.record()
        self._state = self._states.entered
        return self

    @property
    def elapsed(self):
        """
        Returns the time elapsed during execution of the CUDA operations in the context.
        """
        if self._state is not self._states.exited:
            raise Exception(
                "CUDATime.elapsed() cannot be called before its context has exited."
            )
        else:
            return self.start.elapsed_time(self.end) / 1000

    def __exit__(self, *args):
        self.end.record()
        self._state = self._states.exited
