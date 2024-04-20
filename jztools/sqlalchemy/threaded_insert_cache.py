"""
Caches data record inserts and carries them out in batches in a separate thread. The thread is started only when necessary.
"""

from jztools.profiling import time_and_print
import re
import time
from sqlalchemy import insert
from jztools.parallelization.threading.queue import put_loop
from jztools.parallelization.threading.util import ThreadWorkerError, LoopExitRequested
import threading
import queue
from sqlalchemy.exc import OperationalError
from sqlalchemy import Table
from sqlalchemy.engine import Engine
import logging
from typing import Optional, Union
from enum import Enum

# sqlalchemy.exc.OperationalError: (sqlite3.OperationalError) database is locked
LOGGER = logging.getLogger(__name__)


class _State(Enum):
    THREAD_ACTIVE = 0
    INSERT_IN_PROGRESS = 1
    FLUSH = 2
    # Worker errors are addressed by ThreadedInsertCache._worker_error_event
    MAIN_ERROR = 3


class ThreadedInsertCache:
    """
    Caches data record inserts and carries them out in batches in a separate thread. The worker thread is started only when necessary and stopped if no inserts are pending. The batch size will adapt to the available records, with the thread inserting all available records (up to max_batch_size) as soon as possible.

    To ensure the queue is empty and join the worker thread, call :meth:`flush`.
    """

    recoverable_operational_errors_regexp = ".*" + re.escape(
        "(sqlite3.OperationalError) database is locked"
    )

    def __init__(
        self,
        data_table: Table,
        engine: Engine,
        max_queue_size: int = 1000,
        max_batch_size: Optional[int] = None,
        attempts=float("inf"),
    ):
        """
        :param data_table: The table where inserts are done.
        :param engine: The sqlalchemy engine (e.g., the output of :func:`sqlalchemy.create_engine`) used to generate connections.
        :param max_queue_size: Method :meth:`insert` will block after this many queued items are pending.
        :param max_batch_size: Max number of elements to insert at once. Equal to :attr:`max_queue_size` by default.
        :param attempts: Number of times to attempt a re-write upon database operational errors (e.g., file locked).
        """
        self.data_table = data_table
        self.engine = engine
        self.queue = queue.Queue(max_queue_size)
        self.max_batch_size = max_batch_size or max_queue_size
        self.attempts = attempts

        self.thread = None

        # Possible states:
        # 'THREAD_ACTIVE', 'INSERT_IN_PROGRESS', 'FLUSH', 'MAIN_ERROR'
        self._state = set()
        self._worker_error = None
        self._worker_error_event = threading.Event()
        self._state_lock = threading.Condition()
        self._flush_lock = threading.Lock()

    def join_or_fail(self):
        if self.thread is not None:
            self.thread.join()
            if self._worker_error_event.is_set():
                raise self._worker_error

    def insert(self, record: Union[dict, list]):
        try:

            # Check input
            if isinstance(record, dict):
                record = [record]
            elif not isinstance(record, list):
                raise TypeError(
                    f"Expected types {dict} or {list} but received {type(record)}."
                )

            # Apply flush lock to avoid inserts while flushing.
            with self._flush_lock:

                # Check if the worker thread is active, and activate it if it isn't.
                with self._state_lock:
                    self._state.add(_State.INSERT_IN_PROGRESS)
                    if _State.THREAD_ACTIVE not in self._state:
                        self.join_or_fail()
                        self._state.add(_State.THREAD_ACTIVE)
                        self.thread = threading.Thread(target=self._worker)
                        self.thread.start()

                try:
                    # Append records, checking if a thread error occurred every 0.5 seconds when the queue is full.
                    for _record in record:
                        try:
                            put_loop(
                                self.queue,
                                _record,
                                {"thread_error": self._worker_error_event},
                                timeout=0.5,
                            )
                        except LoopExitRequested:
                            self.join_or_fail()
                            # join_or_fail should raise an error
                            raise Exception("Unexpected code line.")
                        with self._state_lock:
                            self._state_lock.notify()

                finally:
                    # Signal end of insert, thus allowing thread worker exit.
                    with self._state_lock:
                        self._state.remove(_State.INSERT_IN_PROGRESS)
                        self._state_lock.notify()

        except Exception:
            # Signal exit to worker thread.
            with self._state_lock:
                self._state.add(_State.MAIN_ERROR)
                self._state_lock.notify()
            raise

    def flush(self):
        """
        Guarantees that at least all items inserted before flush was called are written.
        Blocks any insert operations until the flush finishes.
        """
        with self._flush_lock:
            self.join_or_fail()

    def _log_failed_inserts(self, err, batch_size=0):
        if (remaining := (self.queue.qsize() + batch_size)) > 0:
            LOGGER.error(
                f"The following error prevented writing {remaining} data records to table {self.data_table}:\n\t{err}."
            )

    def _worker(self):
        try:
            while True:

                batch = []

                # Check if the thread should exit
                while True:
                    with self._state_lock:
                        self._state_lock.wait_for(
                            lambda: (
                                (self.queue.qsize() > 0)
                                or (_State.INSERT_IN_PROGRESS not in self._state)
                                or (_State.MAIN_ERROR in self._state)
                            )
                        )
                        try:

                            # Attempt to get at least one element.
                            # If main thread errored out, will attempt to
                            # write all records before quitting.
                            new_record = self.queue.get(block=False)
                        except queue.Empty:
                            if _State.INSERT_IN_PROGRESS not in self._state:
                                # No elements remain, exit the thread.
                                self._state.remove(_State.THREAD_ACTIVE)
                                return
                            elif _State.MAIN_ERROR in self._state:
                                # Main thread error, exit the thread.
                                raise Exception("Main thread error.")
                        else:
                            batch.append(new_record)
                            break

                # Get as many elements as available or allowed.
                while len(batch) < self.max_batch_size:
                    try:
                        batch.append(self.queue.get(block=False))
                    except queue.Empty:
                        break

                # Insert the batch into the data store.
                attempt_num = 0
                while True:
                    attempt_num += 1
                    try:
                        # print(f'*************** {len(batch)}')
                        with self.engine.begin() as connection:
                            connection.execute(insert(self.data_table), batch)
                    except OperationalError as err:
                        if re.match(
                            self.recoverable_operational_errors_regexp, str(err)
                        ) and (attempt_num < self.attempts):
                            LOGGER.error(
                                f"Database operational error in batch-insert attempt {attempt_num} "
                                f"(will re-attempt {self.attempts - attempt_num} times):\n{err}."
                            )
                        else:
                            raise err
                    else:
                        break

        except KeyboardInterrupt as err:
            self._log_failed_inserts(err, len(batch))
            return

        except Exception as err:
            self._worker_error = err
            self._worker_error_event.set()
            self._log_failed_inserts(err, len(batch))
            return
