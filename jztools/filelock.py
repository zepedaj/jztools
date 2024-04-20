"""
Fcntl / flock Unix **advisory** file locking with support for exclusive and shared locks.
"""

from typing import Union, Iterable
from contextlib import contextmanager
import threading
from jztools.parallelization.threading.lock import lock_or_fail, AcquireTimedOut
import time
from jztools import validation as pgval
from functools import total_ordering

import fcntl


@total_ordering
class Mode:
    MODE_MAP = {"read": "shared", "write": "exclusive"}

    def __init__(self, val):
        user_val = val
        if (val := self.MODE_MAP.get(val, val)) not in self.MODE_MAP.values():
            raise ValueError(
                f"Mode value needs to be one of {list(self.MODE_MAP.values())} or {list(self.MODE_MAP.keys())}."
            )
        self.val = val
        self._user_val = user_val

    def __str__(self):
        return self._user_val

    def __repr__(self):
        type_ = type(self)
        module = type_.__module__
        qualname = type_.__qualname__
        return f"<{module}.{qualname} (mode={str(self)}) object at {hex(id(self))}>"

    @property
    def flock_arg(self):
        return (
            fcntl.LOCK_EX if self.val == "exclusive" else fcntl.LOCK_SH
        ) | fcntl.LOCK_NB

    @classmethod
    def produce(cls, val):
        if isinstance(val, cls):
            return val
        else:
            return cls(val)

    def __gt__(self, obj):
        if not isinstance(obj, (str, type(self))):
            return NotImplemented
        obj = self.produce(obj)
        return self.val == "exclusive" and obj.val == "shared"

    def __eq__(self, obj):
        if not isinstance(obj, (str, type(self))):
            return NotImplemented
        obj = self.produce(obj)
        return self.val == obj.val


class FileLock:
    """
    Fcntl/flock file locking. Shared locks can be re-acquired from any :class:`FileLock` object or process. Exclusive locks can only be re-acquired from the same :class:`FileLock` object and process. Note in particular that this object does not enforce exclusive locks between threads for the same lock object (use :class:`CompoundFileLock` for this purpose).

    If a file is moved while a lock is active, that lock will continue to be valid on the moved file. If it is deleted, however, that lock will continue to behave like the file existed, but it will not lock against a newly created file of the same name.

    Lock acquisition for exclusive locks can implicitly upgrade or downgrade the lock:

    Mode 'exclusive' can operate in 'shared' and 'exclusive' mode, with 'exclusive' being sticky:
    None -> acquire(mode='shared') -> 'shared'; acquire(mode='exclusive') -> 'exclusive'; acquire(mode='shared') -> 'exclusive'; release() -> 'exclusive'; release() -> 'shared'; release() -> None

    Mode 'shared' will result in an error if 'exclusive' is requested.:
    acquire(mode='shared') -> 'shared', acquire(mode='exclusive') -> *Exception*


    """

    def __init__(self, filename, mode="exclusive", poll_interval=2e-2):
        """
        :param filename: Name of file to apply advisory lock to.
        :param mode: The maximum supported mode - one of ``'shared'`` (or its alias ``'read'``), or ``'exclusive'`` (or its alias ``'write'``).
        """

        self.filename = filename
        self.max_mode = Mode.produce(mode)
        self.poll_interval = poll_interval
        self._internal_lock = threading.Lock()
        self._acquires = []
        self.fd = None

    def __repr__(self):
        type_ = type(self)
        module = type_.__module__
        qualname = type_.__qualname__
        return f"<{module}.{qualname} ({self.filename}, [{''.join([str(m)[0] for m in self._acquires])}]<={str(self.max_mode)[0]}) object at {hex(id(self))}>"

    @property
    def count(self):
        return len(self._acquires)

    @property
    def mode(self):
        return self._acquires[-1] if self._acquires else None

    def downgrade_max_mode(self):
        """
        Ensures that the max mode is shared mode.
        """
        with self._internal_lock:
            if self.max_mode == Mode("exclusive"):
                if any((_mode > Mode("shared") for _mode in self._acquires)):
                    raise Exception(
                        "The lock is being held in exclusive mode, cannot downgrade it."
                    )
                self.max_mode = Mode("shared")

    @contextmanager
    def with_acquire(self, **acquire_kwargs):
        """
        Context manager that fails if the lock type is not of the specified type, or if the acquire call times out.
        """
        if not self.acquire(**acquire_kwargs):
            raise AcquireTimedOut("Timed out trying to acquire lock.")
        try:
            yield
        finally:
            self.release()

    def acquire(self, timeout=-1, create=False, mode=None):
        """
        :param create: Will create the lock file if mode is 'write'/'exclusive'.
        """

        start_time = time.time()

        #
        if create and mode == Mode("read"):
            raise Exception(
                f"Cannot request lock file creation in {str(Mode(mode))} mode."
            )

        # Get mode, check within bounds.
        mode = Mode.produce(self.max_mode if mode is None else mode)
        if mode > self.max_mode:
            raise ValueError(f"Requested mode={mode} exceeds max_mode={self.max_mode}.")

        with lock_or_fail(self._internal_lock, timeout):

            if self.fd is None:
                # Open the file - will be created if in write mode and it does not exist.
                do_close_fd = True

                # Only create the file if create=True and mode == 'write'/'exclusive'
                _fd = open(
                    self.filename,
                    fd_mode := ("rb" if (not create or mode < Mode("write")) else "ab"),
                )
                if fd_mode == "rb" and mode == Mode("write"):
                    # Need write mode to support exclusive lock. See "NFS details" in flock(2) man pages.
                    self.fd = open(self.filename, "ab")
                    _fd.close()
                else:
                    self.fd = _fd

            else:
                do_close_fd = False

            while True:
                # Check if flock is already acquired
                if self._acquires and self._acquires[-1] >= mode:
                    self._acquires.append(self._acquires[-1])
                    return True

                else:
                    # Acquire a new flock.
                    try:
                        fcntl.flock(self.fd, mode.flock_arg)
                    except BlockingIOError:
                        if timeout > 0 and (time.time() - start_time > timeout):
                            if do_close_fd:
                                self.fd.close()
                                self.fd = None
                            return False
                        else:
                            time.sleep(self.poll_interval)
                    else:
                        self._acquires.append(mode)
                        return True

        #
        raise Exception("Unexpected code line.")

    def release(self):
        with self._internal_lock:
            if self._acquires:
                self._acquires.pop(-1)
                if self._acquires:
                    fcntl.flock(self.fd, self._acquires[-1].flock_arg)
                else:
                    fcntl.flock(self.fd, fcntl.LOCK_UN)
                    self.fd.close()
                    self.fd = None

    def __enter__(self):
        self.acquire()

    def __exit__(self, *args, **kwargs):
        self.release()


class CompoundFileLock(FileLock):
    """
    Behaves like a :class:`FileLock` object but acquires/releases one or more locks (assumed to have the :class:`threading.Lock` interface) simultaneously.
    """

    def __init__(self, file_lock, *locks):
        self.file_lock = file_lock
        self.locks = locks

    def __repr__(self):
        type_ = type(self)
        module = type_.__module__
        qualname = type_.__qualname__
        return (
            object.__repr__(self)
            + "("
            + ", ".join([repr(self.file_lock)] + [repr(_lock) for _lock in self.locks])
            + ")"
        )

    @property
    def count(self):
        return self.file_lock.count

    @property
    def lock_type(self):
        return self.file_lock.lock_type

    def _acquire_others(self, timeout=-1):
        acquired = []
        success = False
        try:
            for _lock in self.locks:
                if _lock.acquire(timeout=timeout):
                    acquired.append(_lock)
                else:
                    return False
            success = True
            return True
        finally:
            if not success:
                [_lock.release() for _lock in acquired]

    def _release_others(self):
        [_lock.release() for _lock in self.locks]

    def acquire(self, timeout=-1, **kwargs):

        if not self._acquire_others(timeout=timeout):
            return False
        try:
            out = self.file_lock.acquire(timeout=timeout, **kwargs)
        except Exception:
            self._release_others()
            raise
        else:
            if not out:
                self._release_others()
            return out

        return True

    def release(self):
        self.file_lock.release()
        self._release_others()

    def disable(self):
        self.file_lock.disable()

    @contextmanager
    def with_acquire(self, timeout=-1, **kwargs):
        with self.file_lock.with_acquire(timeout=timeout, **kwargs):

            if self._acquire_others(timeout=timeout):
                try:
                    yield
                finally:
                    self._release_others()
            else:
                raise AcquireTimedOut()
