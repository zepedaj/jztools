from jztools.parallelization._outsourced_iterable_base import (
    OutsourcedIterable as _OutsourcedIterable,
)
import multiprocessing


class ProcessOutsourcedIterable(_OutsourcedIterable):
    """
    Takes an iterable and runs it in another process, transferring data between processes using a queue.

    :param source: The iterable object to run in the remotely process. Depending on the multiprocessing process start method (see `this <https://stackoverflow.com/a/26026069>`_ discussion), the object might need to support pickling.
    :param initializer: Called at the start of the worker process. See :ref:`proc init fin`.
    :param finalizer: Called in the worker process at the end of normal or abnormal (because of an exception) execution. See :ref:`proc init fin`.

    .. _proc init fin:

    .. rubric:: Process initializer / finalizer

    Both ``initializer`` and ``finalizer`` run in the remote worker process and take the process's copy of ``source`` as their only input parameter. Both callables must be module-level functions (including static and class methods of non-dynamically defined classes).
    """

    Queue = multiprocessing.Queue
    Event = multiprocessing.Event
    Spawner = multiprocessing.Process
