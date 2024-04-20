import jztools.shared_memory as mdl
from jztools import numpy as pgnp
import numpy.testing as npt
import numpy as np
from concurrent.futures import ProcessPoolExecutor, wait
from contextlib import contextmanager
from unittest import TestCase
from multiprocessing.shared_memory import SharedMemory
import pickle


def names(*args):
    return {x.shared_memory.name for x in args}


@contextmanager
def get_futures(worker, arr=np.arange(int(1e6)), N=10000):
    """
    Creates a large xndarray in the main process.
    Applies a worker in different processes to slices of the array.

    Returns the futures of each worker.
    """

    with mdl.XSharedMemoryManager() as smm:

        xarr = mdl.xndarray.from_ndarray(smm, np.arange(int(1e6)))

        futures = []
        with ProcessPoolExecutor(max_workers=10) as pool:
            for start in range(0, len(xarr), N):
                futures.append(pool.submit(worker, xarr, start, start + N))

            wait(futures)

        yield arr, xarr, N, futures


class TestXndarray(TestCase):
    @staticmethod
    def worker_ro(xarr, start, end):
        val = xarr[start:end].max()
        return val.item()

    def test_send_ro(self):
        with get_futures(self.worker_ro) as (arr, xarr, N, futures):
            out = [_future.result() for _future in futures]
            out.sort()
            npt.assert_equal(out, arr[N - 1 :: N])

    @staticmethod
    def worker_rw(xarr, start, end):
        xarr[start:end] += 1

    def test_send_rw(self):
        with get_futures(self.worker_rw) as (arr, xarr, N, futures):
            [_future.result() for _future in futures]

        npt.assert_equal(xarr, arr + 1)

    @staticmethod
    def worker_remote_create(N, smm):
        xarr = mdl.xndarray.from_ndarray(smm, np.arange(N), scope="remote")
        return xarr

    def test_remote_create(self):
        with mdl.XSharedMemoryManager() as smm:
            futures = []
            with ProcessPoolExecutor(max_workers=10) as pool:
                for N in range(Nmax := 50):
                    futures.append(pool.submit(self.worker_remote_create, N, smm))

            wait(futures)

            for N in range(Nmax):
                npt.assert_equal(
                    [np.arange(N) for N in range(Nmax)],
                    shared_arrs := [f.result() for f in futures],
                )

            [
                self.assertTrue(
                    type(xarr) == mdl.xndarray
                )  # and (xarr.shared_memory or xarr.base)
                for xarr in shared_arrs
            ]

    def test_from_ndarray(self):
        with mdl.XSharedMemoryManager() as smm:
            arr = mdl.xndarray.from_ndarray(smm, np.arange(1))
            assert arr.shared_memory is not None

    def test_reduction(self):
        with mdl.XSharedMemoryManager() as smm:
            xarr = mdl.xndarray.from_ndarray(smm, arr := np.arange(int(1e6)))
            for _slc in [[0, 1, 2], slice(5, 100, 3), None, slice(None, -10, -3)]:
                npt.assert_array_equal(arr[_slc], sliced_xarr := xarr[_slc])
                if isinstance(_slc, list):
                    self.assertFalse(sliced_xarr.is_shared())
                else:
                    self.assertTrue(sliced_xarr.is_shared())
                    reduction = sliced_xarr.__reduce__()
                    rec_sliced_xarr = reduction[0](*reduction[1])
                    npt.assert_array_equal(rec_sliced_xarr, sliced_xarr)

    def test_getitem(self):
        # Also does `def test_shared_memory` and `def test_shared_memory_manager`, `def test_is_shared`.
        with mdl.XSharedMemoryManager() as smm:
            xarr = mdl.xndarray.from_ndarray(smm, arr := np.arange(int(1e6)))
            for _slc in [slice(5, 100, 3), None, [0, 1, 2], slice(None, -10, -3)]:
                npt.assert_array_equal(arr[_slc], sliced_xarr := xarr[_slc])
                #
                self.assertIs(
                    sliced_xarr.shared_memory,
                    None if isinstance(_slc, list) else xarr.shared_memory,
                )
                #
                self.assertIs(
                    sliced_xarr.shared_memory_manager,
                    None if isinstance(_slc, list) else xarr.shared_memory_manager,
                )
                #
                self.assertIs(
                    sliced_xarr.is_shared(), False if isinstance(_slc, list) else True
                )

    def test_reduction__structured_arrays(self):
        arr = pgnp.random_array(
            (int(1e4), 100),
            dtype=[("f1", "i"), ("f2", "f"), ("f3", "datetime64[m]"), ("f4", "u1")],
        )
        with mdl.XSharedMemoryManager() as smm:
            xarr = mdl.xndarray.from_ndarray(smm, arr)
            for _slc in [[0, 1, 2], slice(5, 100, 3), None, slice(None, -10, -3)]:
                npt.assert_array_equal(arr[_slc], sliced_xarr := xarr[_slc])
                if isinstance(_slc, list):
                    self.assertFalse(sliced_xarr.is_shared())
                else:
                    self.assertTrue(sliced_xarr.is_shared())
                    reduction = sliced_xarr.__reduce__()
                    rec_sliced_xarr = reduction[0](*reduction[1])
                    npt.assert_array_equal(rec_sliced_xarr, sliced_xarr)

    def test_stack_as_ndarray(self):
        with mdl.XSharedMemoryManager() as smm:
            arrs = [smm.empty(10) for _ in range(2)]
            stacked = np.stack(arrs)
            self.assertEqual(type(stacked), np.ndarray)

    def test_init(self):
        with mdl.XSharedMemoryManager() as smm:

            # Creates a shared memory xndarray
            arr = smm.empty(10)
            self.assertEqual(type(arr), mdl.xndarray)

    @staticmethod
    def worker_release_on_retrieval(N, smm):
        xarr = mdl.xndarray.from_ndarray(smm, np.arange(N), scope="remote")
        return xarr

    def test_release_on_retrieval(self):
        # TODO: Check mem is released
        with mdl.XSharedMemoryManager() as smm:
            futures = []
            with ProcessPoolExecutor(max_workers=10) as pool:
                for N in range(Nmax := 50):
                    futures.append(
                        pool.submit(self.worker_release_on_retrieval, N, smm)
                    )

            wait(futures)

            for N in range(Nmax):
                npt.assert_equal(
                    [np.arange(N) for N in range(Nmax)],
                    shared_arrs := [f.result() for f in futures],
                )

    @staticmethod
    def create_arr(smm, shape, val, scope="remote"):
        arr = smm.empty(shape, scope=scope)
        if val is not None:
            arr[:] = val
        return arr

    @staticmethod
    def stack_arrs(smm, arrs):
        # Assumes all arrs are the same shape.
        out = smm.empty(
            dtype=arrs[0].dtype, shape=(len(arrs),) + arrs[0].shape, scope="remote"
        )
        out[:] = arrs
        return arrs

    def test_dataloader_like(self):
        shape = (int(1e3), int(1e3))
        num_arrays = 5
        with mdl.XSharedMemoryManager() as smm:

            # Create a shared array in the master process
            arr0 = self.create_arr(smm, shape, 0)

            # Create shared arrays in child processes
            with ProcessPoolExecutor(max_workers=10) as pool:
                futures = []
                for posn in range(1, num_arrays):
                    futures.append(pool.submit(self.create_arr, smm, shape, val=posn))

                arrays = [arr0] + [f_.result() for f_ in futures]

            # Concatenate all shared arrays into a shared array in a child process
            with ProcessPoolExecutor(max_workers=10) as pool:
                future = pool.submit(self.stack_arrs, smm, arrays)

            # Verify the results
            stacked = future.result()
            assert (
                stacked == np.arange(num_arrays).reshape((-1,) + (1,) * len(shape))
            ).all()

    @staticmethod
    def get_array_specs(arr):
        return (
            arr.shared_memory.name,
            arr.shape,
            hash(arr.shared_memory.buf.hex()),
            arr.sum().item(),
        )

    def test_sh_mem_name(self):
        with mdl.XSharedMemoryManager() as smm:

            # Create a shared array in the master process
            arr = self.create_arr(smm, num := 100, 0)
            arr[:] = np.random.randn(*arr.shape)
            sh_mem_name = arr.shared_memory.name
            hex_hash = hash(arr.shared_memory.buf.hex())

            # Get array specs in child processes.
            with ProcessPoolExecutor(max_workers=10) as pool:
                futures = []
                for last in (shapes := np.linspace(0, num, 5).astype("i").tolist()):
                    futures.append(pool.submit(self.get_array_specs, arr[:last]))

                specs = [f_.result() for f_ in futures]

            self.assertEqual(
                list(
                    zip(
                        [sh_mem_name] * len(shapes),
                        ((x_,) for x_ in shapes),
                        [hex_hash] * len(shapes),
                        (arr[:n_].sum().item() for n_ in shapes),
                    )
                ),
                specs,
            )

    def test_del(self):

        with mdl.XSharedMemoryManager() as smm:

            x = smm.empty(10)
            y = smm.empty(10)
            z = smm.empty(10)
            # Unassigned xndarray, discarded right away with scope='local'
            smm.empty(10)

            self.assertEqual(set(smm._list_tracked_segments()), names(x, y, z))

            # Manually delete xndarray
            del x
            self.assertEqual(set(smm._list_tracked_segments()), names(y, z))

            # Released xndarray
            z.release()
            self.assertEqual(set(smm._list_tracked_segments()), names(y))

    def test_scope_loop(self):
        with mdl.XSharedMemoryManager() as smm:
            for k in range(10):
                x = smm.empty(10)
                self.assertEqual(set(smm._list_tracked_segments()), names(x))

    def test_scope_remote(self):
        with mdl.XSharedMemoryManager() as smm:
            #
            with ProcessPoolExecutor(max_workers=10) as pool:
                future = pool.submit(self.create_arr, smm, 10, 1)
                x = future.result()
            self.assertEqual(set(smm._list_tracked_segments()), names(x))

    def test_as_out(self):
        with mdl.XSharedMemoryManager() as smm:

            # Fill an xndarry with random data
            N = int(1e6)
            rng = np.random.default_rng(0)
            xarr = smm.empty(N)
            rng.random(int(1e6), out=xarr)

            x2 = xarr[: N // 2]
            x = np.mean(x2)

            # Take the mean of both halves
            futures = []
            with ProcessPoolExecutor() as pool:
                futures.append(pool.submit(np.mean, xarr[: N // 2]))
                futures.append(pool.submit(np.mean, xarr[N // 2 :]))

            results = [x.result() for x in futures]

    def test_ufunc(self):

        with mdl.XSharedMemoryManager() as smm:

            # ufunc to ndarray
            xarr = smm.from_ndarray(np.arange(100))
            arr2 = np.arange(100)
            #
            for x, y, out in [
                (arr2, xarr, arr2 * 2),
                (xarr, 2 * arr2, arr2 * 3),
                (xarr, 1, arr2 + 1),
                (2, xarr, arr2 + 2),
            ]:
                self.assertIsInstance(actual_out := np.add(x, y), np.ndarray)
                self.assertTrue((actual_out == out).all())

            # ufunc to out=xndarray
            xarr_out = smm.empty(100)
            for x, y, out in [
                (arr2, xarr, arr2 * 2),
                (xarr, 2 * arr2, arr2 * 3),
                (xarr, 1, arr2 + 1),
                (2, xarr, arr2 + 2),
            ]:
                np.add(x, y, out=xarr_out)
                self.assertIsInstance(xarr_out, mdl.xndarray)
                self.assertTrue((xarr_out == out).all())

    def test_squeeze(self):

        with mdl.XSharedMemoryManager() as smm:
            arr = smm.empty((100, 1, 200, 1, 3))

            sqzd_arr = np.squeeze(arr)
            self.assertEqual(sqzd_arr.shape, (100, 200, 3))
            self.assertIsInstance(sqzd_arr, mdl.xndarray)

            arr[0, 0, 0, 0, 0] = 100
            self.assertEqual(arr[0, 0, 0, 0, 0], sqzd_arr[0, 0, 0])

    def test_pickling(self):
        with mdl.XSharedMemoryManager() as smm:

            # Multiple sizes, same pickled footprint
            np_arrs = [np.random.rand(N) for N in [10, int(1e3), int(1e4), int(1e6)]]
            x_arrs = [smm.from_ndarray(x) for x in np_arrs]

            # Add a slice
            k = -10000
            np_arrs.append(np_arrs[-1][k:])
            x_arrs.append(x_arrs[-1][k:])

            # Pickle and verify equality
            pickled_arrs = [pickle.dumps(x) for x in x_arrs]
            self.assertTrue(all(len(x) <= 300 for x in pickled_arrs))
            unpickled_arrs = [pickle.loads(x) for x in pickled_arrs]
            npt.assert_equal(np_arrs, unpickled_arrs)

            # Modifying the original array modifies the upickled array.
            for x in x_arrs:
                x[-1] = 0
            for x, y in zip(x_arrs, unpickled_arrs):
                npt.assert_array_equal(x, y)

    def test_pickling__structured(self):

        _np_arr = np.empty(
            int(1e6),
            dtype=[("scalar", int), ("matrix", "f", (3, 2)), ("date", "datetime64[m]")],
        )
        _np_arr["scalar"] = np.arange(len(_np_arr))
        _np_arr["matrix"] = np.arange(len(_np_arr))[:, None, None] + len(_np_arr)
        _np_arr["date"] = np.arange(len(_np_arr)) + 2 * len(_np_arr)

        np_arrs = {
            "full": _np_arr,
            **{fld: _np_arr[fld] for fld in _np_arr.dtype.names},
        }

        with mdl.XSharedMemoryManager() as smm:

            _x_arr = smm.from_ndarray(_np_arr)
            x_arrs = {
                "full": _x_arr,
                **{fld: _x_arr[fld] for fld in _np_arr.dtype.names},
            }

            # Pickle, check size
            pickled_arrs = pickle.dumps(x_arrs)
            self.assertTrue(len(pickled_arrs) <= 300 * len(x_arrs))
            unpickled_arrs = pickle.loads(pickled_arrs)

            # Unpickled array equals orig array.
            [
                npt.assert_array_equal(unpickled_arrs[fld], np_arrs[fld])
                for fld in np_arrs
            ]

            # Modifying the original x_arr modifies the unpickled arr
            x_arrs["full"]["date"][0] = x_arrs["full"]["date"][1]
            np_arrs["date"][0] = np_arrs["date"][1]
            [
                npt.assert_array_equal(unpickled_arrs[fld], np_arrs[fld])
                for fld in np_arrs
            ]

    @staticmethod
    def dummy_worker(self, *args, **kwargs):
        pass

    def test_max_number_of_segments(self):

        size = 1
        N = 1000

        arrs = []
        try:
            for _ in range(N):
                arrs.append(SharedMemory(size=size * 8, create=True))
        finally:
            for x in arrs:
                try:
                    x.unlink()
                    x.close()
                except Exception:
                    pass

    # THE TEST BELOW PASSES/FAILS RANDOMLY!!!
    # NO GOOD SOLUTION FOUND!!!
    # def test_error_zombies(self):
    #     with mdl.XSharedMemoryManager() as smm:
    #         #
    #         with ProcessPoolExecutor(max_workers=10) as pool:
    #             # Remotely created arrays with local scope should not exist in the parent process.
    #             future = pool.submit(self.create_arr, smm, 10, 1, scope='local')
    #         try:
    #             x = future.result()
    #         except BrokenProcessPool as err:
    #             self.assertIsNotNone(re.match('(?ms).*FileNotFoundError.*', str(err.__cause__)))
    #         else:
    #             # If they do, a zombie is at fault.
    #             # THIS SHOULD NEVER HAPPEN, BUT
    #             raise Exception('The expected exception did not occur!')
