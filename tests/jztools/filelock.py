from unittest import TestCase
import re
from tempfile import NamedTemporaryFile
import jztools.filelock as mdl
from jztools.parallelization import PoolExecutor
from jztools.parallelization.multiprocessing import ProcessPoolExecutor
import threading
import os
import shutil


class TestMode(TestCase):
    def test_total_ordering(self):
        #
        for mode1 in map(mdl.Mode, ["read", "shared"]):
            for _mode2 in ["read", "shared"]:
                for mode2 in [_mode2, mdl.Mode(_mode2)]:
                    self.assertTrue(mode1 == mode2)
                    self.assertTrue(mode2 == mode1)
                    self.assertTrue(mode1 <= mode2)
                    self.assertTrue(mode1 >= mode2)
                    self.assertTrue(mode2 <= mode1)
                    self.assertTrue(mode2 >= mode1)

                    self.assertFalse(mode2 > mode1)
                    self.assertFalse(mode2 < mode1)

        #
        for mode1 in map(mdl.Mode, ["write", "exclusive"]):
            for _mode2 in ["read", "shared"]:
                for mode2 in [_mode2, mdl.Mode(_mode2)]:
                    self.assertTrue(mode1 > mode2)
                    self.assertTrue(mode2 < mode1)
                    self.assertTrue(mode1 >= mode2)
                    self.assertTrue(mode2 <= mode1)

                    self.assertFalse(mode1 < mode2)
                    self.assertFalse(mode2 > mode1)
                    self.assertFalse(mode2 == mode1)
                    self.assertFalse(mode1 == mode2)

        #
        with self.assertRaisesRegex(
            TypeError, "'>' not supported between instances of 'Mode' and 'int'"
        ):
            mode1 > 1

        with self.assertRaisesRegex(ValueError, "Mode value needs to be one of"):
            mode1 > "invalid_mode"


class TestFileLock(TestCase):
    @staticmethod
    def acquire_lock(*args, return_object=False, **kwargs):
        if "timeout" in kwargs:
            timeout_kw = {"timeout": kwargs.pop("timeout")}
        else:
            timeout_kw = {}

        lock_obj = mdl.FileLock(*args, **kwargs)
        response = lock_obj.acquire(**timeout_kw)
        if return_object:
            return lock_obj, response
        else:
            return response

    @staticmethod
    def spawn_and_lock(*args, **kwargs):
        with PoolExecutor("PROCESS") as pool:
            out = pool.submit(TestFileLock.acquire_lock, *args, **kwargs)
            return out.result()

    def test_exclusive(self):
        with NamedTemporaryFile() as tempf, ProcessPoolExecutor(
            max_workers=1
        ) as process_pool:

            #
            lock = mdl.FileLock(tempf.name)
            lock.acquire()

            # Fails to acquire in another process
            self.assertFalse(
                process_pool.submit(
                    self.acquire_lock,
                    *(args := (tempf.name,)),
                    **(kwargs := {"timeout": 0.1}),
                ).result()
            )

            # Fails with another file descriptors for the same file in the same process.
            self.assertFalse(self.acquire_lock(*args, **kwargs))

    def test_shared(self):
        with NamedTemporaryFile() as tempf, ProcessPoolExecutor(
            max_workers=1
        ) as process_pool:

            #
            lock = mdl.FileLock(tempf.name, mode="shared")
            lock.acquire()

            # Succeeds to acquire in another process
            self.assertTrue(
                process_pool.submit(
                    self.acquire_lock,
                    *(args := (tempf.name,)),
                    **(kwargs := {"mode": "shared"}),
                ).result()
            )

            # Fails to acquire exclusive lock in another process
            self.assertFalse(
                process_pool.submit(
                    self.acquire_lock, *args, mode="exclusive", timeout=0.5
                ).result()
            )

            # ... or in this process.
            self.assertFalse(self.acquire_lock(*args, mode="exclusive", timeout=0.5))

            # Succeeds with another file descriptors for the same file in the same process.
            self.assertTrue(self.acquire_lock(*args, **kwargs))

    def test_moved(self):
        with NamedTemporaryFile(delete=False) as tempf1, NamedTemporaryFile(
            delete=False
        ) as tempf2:
            try:
                obj, _ = self.acquire_lock(tempf1.name, return_object=True)
                shutil.move(tempf1.name, tempf2.name)

                with self.assertRaises(FileNotFoundError):
                    self.acquire_lock(tempf1.name)
                self.assertFalse(self.acquire_lock(tempf2.name, timeout=0.5))

            finally:
                for fn in [tempf1.name, tempf2.name]:
                    try:
                        os.remove(fn)
                    except FileNotFoundError:
                        pass

    def test_multi_object(self):
        with NamedTemporaryFile(delete=False) as tempf:

            # Shared
            obj1, acq1 = self.acquire_lock(
                tempf.name, mode="shared", return_object=True
            )
            self.assertTrue(acq1)
            obj2, acq2 = self.acquire_lock(
                tempf.name, mode="shared", return_object=True
            )
            self.assertTrue(acq2)

            obj1.release()
            obj2.release()

            # Exclusive
            obj1, acq1 = self.acquire_lock(
                tempf.name, mode="exclusive", return_object=True
            )
            self.assertTrue(acq1)
            obj2, acq2 = self.acquire_lock(
                tempf.name, mode="exclusive", timeout=0.5, return_object=True
            )
            self.assertFalse(acq2)

    def test_max_mode(self):
        with NamedTemporaryFile() as tempf:
            file_lock = mdl.FileLock(tempf.name, mode="shared")
            with self.assertRaises(ValueError):
                with file_lock.with_acquire(mode="exclusive", timeout=0.1):
                    pass

    def test_mode_switch(self):

        with NamedTemporaryFile() as tempf:
            file_lock = mdl.FileLock(tempf.name)

            # Mode is less than max mode, then equal
            self.assertEqual(file_lock.mode, None)
            with file_lock.with_acquire(mode="shared", timeout=0.1):
                self.assertEqual(file_lock.mode, "shared")
                self.assertFalse(
                    TestFileLock.spawn_and_lock(
                        tempf.name, timeout=0.1, mode="exclusive"
                    )
                )
                self.assertTrue(
                    TestFileLock.spawn_and_lock(tempf.name, timeout=0.1, mode="shared")
                )

                with file_lock.with_acquire(mode="exclusive", timeout=0.1):
                    self.assertEqual(file_lock.mode, "exclusive")
                    self.assertFalse(
                        TestFileLock.spawn_and_lock(
                            tempf.name, timeout=0.1, mode="exclusive"
                        )
                    )
                    self.assertFalse(
                        TestFileLock.spawn_and_lock(
                            tempf.name, timeout=0.1, mode="shared"
                        )
                    )

                self.assertEqual(file_lock.mode, "shared")
                self.assertFalse(
                    TestFileLock.spawn_and_lock(
                        tempf.name, timeout=0.1, mode="exclusive"
                    )
                )
                self.assertTrue(
                    TestFileLock.spawn_and_lock(tempf.name, timeout=0.1, mode="shared")
                )

            self.assertEqual(file_lock.mode, None)
            self.assertTrue(
                TestFileLock.spawn_and_lock(tempf.name, timeout=0.1, mode="exclusive")
            )
            self.assertTrue(
                TestFileLock.spawn_and_lock(tempf.name, timeout=0.1, mode="shared")
            )

    def test_with_acquire__exception(self):
        with NamedTemporaryFile() as tempf:
            file_lock = mdl.FileLock(tempf.name)
            try:
                with file_lock.with_acquire():
                    self.assertEqual(file_lock.count, 1)
                    raise Exception()
            except Exception:
                self.assertEqual(file_lock.count, 0)


class TestCompoundFileLock(TestCase):
    def test_all(self):
        with NamedTemporaryFile() as tempf:
            lock = threading.Lock()
            file_lock = mdl.FileLock(tempf.name)

            compound_lock = mdl.CompoundFileLock(file_lock, lock)

            # Acquire suceeds.
            success = False
            with compound_lock:
                success = True
            self.assertTrue(success)

            # Threading lock was locked.
            with lock:
                self.assertFalse(lock.acquire(timeout=0.1))
                with self.assertRaises(mdl.AcquireTimedOut):
                    with compound_lock.with_acquire(timeout=0.1, mode="exclusive"):
                        pass

            # FileLock was locked.
            competing_file_lock = mdl.FileLock(tempf.name)
            with competing_file_lock:
                with self.assertRaises(mdl.AcquireTimedOut):
                    with compound_lock.with_acquire(mode="exclusive", timeout=0.1):
                        pass

            # Acquire suceeds again.
            success = False
            with compound_lock:
                success = True
            self.assertTrue(success)

    def test_max_mode(self):

        with NamedTemporaryFile() as tempf:
            lock = threading.RLock()
            file_lock = mdl.FileLock(tempf.name)
            compound_lock = mdl.CompoundFileLock(file_lock, lock)

            # Mode is less than max mode, then equal
            self.assertEqual(compound_lock.file_lock.mode, None)
            with compound_lock.with_acquire(mode="shared", timeout=0.1):
                self.assertEqual(compound_lock.file_lock.mode, "shared")
                self.assertFalse(
                    TestFileLock.spawn_and_lock(
                        tempf.name, timeout=0.1, mode="exclusive"
                    )
                )
                self.assertTrue(
                    TestFileLock.spawn_and_lock(tempf.name, timeout=0.1, mode="shared")
                )

                with compound_lock.with_acquire(mode="exclusive", timeout=0.1):
                    self.assertEqual(compound_lock.file_lock.mode, "exclusive")
                    self.assertFalse(
                        TestFileLock.spawn_and_lock(
                            tempf.name, timeout=0.1, mode="exclusive"
                        )
                    )
                    self.assertFalse(
                        TestFileLock.spawn_and_lock(
                            tempf.name, timeout=0.1, mode="shared"
                        )
                    )

                self.assertEqual(compound_lock.file_lock.mode, "shared")
                self.assertFalse(
                    TestFileLock.spawn_and_lock(
                        tempf.name, timeout=0.1, mode="exclusive"
                    )
                )
                self.assertTrue(
                    TestFileLock.spawn_and_lock(tempf.name, timeout=0.1, mode="shared")
                )

            self.assertEqual(compound_lock.file_lock.mode, None)
            self.assertTrue(
                TestFileLock.spawn_and_lock(tempf.name, timeout=0.1, mode="exclusive")
            )
            self.assertTrue(
                TestFileLock.spawn_and_lock(tempf.name, timeout=0.1, mode="shared")
            )

    def test_with_acquire__exception(self):

        # ********* Helpers ***********
        class DmyException(Exception):
            pass

        def thread_lock_count(thread_lock):
            return int(re.match(".+count=(\\d+).+", str(thread_lock)).groups()[0])

        # *****************************

        with NamedTemporaryFile() as tempf:
            compound_lock = mdl.CompoundFileLock(
                mdl.FileLock(tempf.name), threading.RLock()
            )
            try:
                with compound_lock.with_acquire():
                    self.assertEqual(thread_lock_count(compound_lock.locks[0]), 1)
                    self.assertEqual(compound_lock.file_lock.count, 1)
                    raise DmyException()
            except DmyException:
                self.assertEqual(thread_lock_count(compound_lock.locks[0]), 0)
                self.assertEqual(compound_lock.file_lock.count, 0)
