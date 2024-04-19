from .._outsourced_iterable_base import OutsourcedIterable as _OutsourcedIterable
import threading
import queue


class ThreadOutsourcedIterable(_OutsourcedIterable):
    Queue = queue.Queue
    Event = threading.Event
    Spawner = threading.Thread
