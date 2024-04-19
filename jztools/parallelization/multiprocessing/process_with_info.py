from multiprocessing import Process as _Process
from functools import partial


class ProcessWithInfo(_Process):
    """
    A subclass of ``multiprocessing.Process`` that allows for process-level information to be transmitted as an attribute of the class. This information can be used to store global configurations for a process.
    """

    process_info = None
    """
    Class attribute that will be set in the child process to the value of the ``process_info`` initialization argument.
    """

    def __init__(self, *args, process_info=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._args = (process_info,) + self._args

    def run(self):
        if self._target:
            type(self).process_info = self._args[0]
            self._target(*self._args[1:], **self._kwargs)

    @classmethod
    def partial(cls, info):
        """
        Returns a partial of :class:`ProcessWithInfo` that has ``process_info=info`` and can be used in lieu of ``multiprocessing.Process``.
        """
        return partial(cls, process_info=info)
