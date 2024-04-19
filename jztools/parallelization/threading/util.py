import threading


class LoopExitRequested(Exception):
    """
    This exception is raised by ``*_loop`` functiones (e.g., :func:`~jztools.parallelization.threading.queue.get_loop`, :func:`~jztools.parallelization.threading.queue.put_loop`, :func:`~jztools.parallelization.threading.lock.wait_for_loop`) to signal that the main thread has set the exit event.
    """

    def __init__(self, sources=None):
        self.sources = sources or {}
        super().__init__("<" + ", ".join(sources) + ">")


class ThreadWorkerError(Exception):
    """
    Can be used by the main thread to signal that a thread error occurred.
    """

    def __init__(self, msg="Error detected in worker thread!"):
        super().__init__(msg)


class RepeatTimer(threading.Thread):
    """
    Runs an action every interval seconds, and stops after finishing the currently executing action if :meth:`stop` is called
    """

    def __init__(self, action, interval=1):
        super().__init__()
        self.action = action
        self.interval = interval
        self._stopped = threading.Event()

    def run(self):
        while not self._stopped.wait(self.interval):
            self.action()

    def stop(self):
        self._stopped.set()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()
