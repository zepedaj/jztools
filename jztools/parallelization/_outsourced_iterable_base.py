from typing import Iterable, Optional, Callable, Any
import abc
from jztools.parallelization.threading.queue import (
    put_loop,
    get_loop,
    LoopExitRequested,
)


class WorkerFinished(Exception):
    pass


class WorkerException(Exception):
    def __init__(self, error):
        self.error = error


def _worker(
    source,
    q,
    exit_events,
    finished_event,
    initializer,
    finalizer,
    filter,
    mapper,
    post_filter,
):
    try:
        mapper = mapper or (lambda x: x)
        filter = filter or (lambda x: True)
        post_filter = post_filter or (lambda x: True)

        if initializer:
            initializer(source)
        for item in source:
            if filter(item):
                if post_filter(mapped_item := mapper(item)):
                    put_loop(q, mapped_item, exit_events, pre_check=True)

    except LoopExitRequested:
        pass
    except BaseException as error:
        put_loop(q, WorkerException(error), exit_events)
    else:
        put_loop(q, WorkerFinished(), exit_events)
    finally:
        try:
            if finalizer:
                finalizer(source)
        finally:
            finished_event.set()


class OutsourcedIterable(abc.ABC):
    @property
    @abc.abstractmethod
    def Queue(self):
        """
        The queue class used to communicate with the outsourced worker.
        """

    @property
    @abc.abstractmethod
    def Event(self):
        """
        The event class used to communicate with the outsourcer worker.
        """

    @property
    @abc.abstractmethod
    def Spawner(self):
        """
        The callable that creates the object that outsources execution.
        """

    def __init__(
        self,
        source: Iterable,
        initializer: Optional[Callable[[Iterable], None]] = None,
        finalizer: Optional[Callable[[Iterable], None]] = None,
        filter: Callable[[Any], bool] = None,
        mapper: Callable[[Any], Any] = None,
        post_filter: Callable[[Any], bool] = None,
        queue_size: int = 2,
        name=None,
    ):
        """
        :param source: The iterable object to outsource.
        :param initializer: Called at the start of the worker process, with ``source`` as an argument.
        :param finalizer: Called in the worker process at the end of normal or abnormal (because of an exception) execution, with ``source`` as an argument.
        :param filter: Items will be placed in the queue only if this returns ``True`` for that item.
        :param mapper: Applied to each item before placing it in the queue.
        :param post_filter: Items will be placed in the queue only if this returns ``True`` for the mapped item.
        :param queue_size: The size of the queue to pre-compute.
        :param name: The name given to the spawned work unit (e.g., thread or process).
        """

        self.source = source
        self.initializer = initializer
        self.finalizer = finalizer
        self.filter = filter
        self.mapper = mapper
        self.post_filter = post_filter
        self.queue_size = queue_size
        self.name = name or type(self).__name__
        #
        self.exit_events = {}
        self.worker_finished_event = None
        self.worker = None
        self.queue = None

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.stop()

    def __iter__(self):
        if self.worker is not None:
            raise Exception("Attempting to re-enter the iterator!")
        self.exit_events = {"stop": self.Event()}
        self.worker_finished_event = self.Event()
        self.queue = self.Queue(self.queue_size)
        self.worker = self.Spawner(
            target=_worker,
            args=(
                self.source,
                self.queue,
                self.exit_events,
                self.worker_finished_event,
                self.initializer,
                self.finalizer,
                self.filter,
                self.mapper,
                self.post_filter,
            ),
            name=self.name,
        )
        self.worker.start()
        try:
            while True:
                item = get_loop(self.queue, self.exit_events)
                if isinstance(item, WorkerException):
                    raise item.error from item
                elif isinstance(item, WorkerFinished):
                    break
                else:
                    yield item
        finally:
            self.stop()

    def stop(self):
        self.exit_events["stop"].set()
        while self.worker.is_alive():
            # Flushing the queue to unblock the worker process queue cache and
            # hence finalize the worker's queue thread.
            try:
                get_loop(self.queue, {"worker_finished": self.worker_finished_event})
            except LoopExitRequested:
                pass

            self.worker.join()
