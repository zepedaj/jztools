"""

The following operations produce ``xndarray`` objects:

 * Copying an ``xndarray`` -- the copy will have new shared memory.
 * Slicing an ``xndarray`` -- the slice will reference the original shared memory.

The following operations do not:

 * Operators that are not in-place, e.g., ``new_arr = 1 + x_arr``.

"""

import numpy as np
from jztools.py import exception_string
from mmap import mmap
from multiprocessing.shared_memory import SharedMemory
from multiprocessing.managers import SharedMemoryManager, dispatch
from typing import Union, Optional


class InvalidAsXndarray(Exception):
    def __init__(self, obj):
        super().__init__(f"Cannot view type `{type(obj).__name__}` as an `xndarray`.")


class XSharedMemoryManager(SharedMemoryManager):
    """
    SharedMemoryManager that can be used as an argument when creating multiprocessing processes.

    Allocated shared memory can be viewed using ``ls /dev/shm -lth``
    """

    @classmethod
    def _create_in_child_process(cls, address):
        obj = XSharedMemoryManager(address=address)
        obj.connect()
        return obj

    def __reduce__(self):
        # out = self._create_in_child_process, (str(self._address), bytes(self._authkey))
        out = self._create_in_child_process, (self.address,)
        return out

    def from_ndarray(self, arr, **kwargs):
        """
        Cretes an xndarray from the input np.ndarray.
        """
        return xndarray.from_ndarray(self, arr, **kwargs)

    def empty(self, *args, **kwargs):
        """
        Creates an empty array in shared memory.
        """
        return xndarray.empty(self, *args, **kwargs)

    def _list_tracked_segments(self):
        with self._Client(self.address, authkey=self._authkey) as conn:
            out = dispatch(conn, None, "list_segments", ())

        return out


class xndarray(np.ndarray):
    """
    Class derived from ``numpy.ndarray`` that uses shared memory. Implements a pickling interface that pickles a reference to the shared memory instead of the actual buffer contents and is compatible with multiprocessing. This inclues support for array slicing - sliced arrays will be sent as shared memory arrays. When indexing produces a new copy of memory (e.g., when using non-uniform indexing such as ``arr[list([0,10])])``, :meth:`__getitem__` returns a numpy.ndarray
    """

    scope: Optional[str] = None
    """
    Can be one of ``'local'``, ``'remote'`` or ``None``. See :ref:`memory management`.
    """

    _SCOPE_PICKLE_MAPPING = {"local": None, "remote": "local", None: None}
    """
    The :attr:`scope` attribute's value will be mapped using this mapping before pickling.
    """

    def __new__(
        cls,
        sh_mem: Optional[Union[str, SharedMemory]],
        manager: XSharedMemoryManager,
        shape,
        dtype,
        offset,
        strides,
        scope: Optional[str] = "local",
        **kwargs,
    ):
        """
        :param sh_mem: The shared memory with the contents of this array.
        :param scope: Specifies when the shared memory will be released. Valid values are ``'local'``, ``'remote'``, or ``None``. See :ref:`memory management`.

        .. _memory management:

        .. rubric:: Memory management

        Shared memory needs to be released in order to avoid memory leaks that can persist after the end of the program.

        The approached used herein relies on :class:`XSharedMemoryManager` -- a context manager derived from :mod:`multiprocessing`'s :class:`SharedMemoryManager` that further supports being pickled and initializing :class:`xndarray` instances. All shared memory created within a given :class:`XSharedMemoryManager` context will be released upon exiting the context.

        Within the context, :class:`xndarrays` will be released based on their :attr:`scope` attribute, which can take one of the following values:

        ``'local'``
           * The shared memory will be released when the ``xndarray`` goes out of scope in the process where it was declared.
           * When pickling, this will be converted to ``None`` so that child processes do not release the memory.

           .. warning:: An array created in child process should not use ``scope='local'``, as the created array will be garbage-collected in the child process. Attempting to access it in the parent process can result in undefined behavior, including raising a ``FileNotFoundError`` exception, or accessing the memory before it is garbage-collected.

        ``'remote'``
            * When the ``xndarray`` is pickled, this value will be pickled as ``'local'`` .
            * The shared memory will hence be released when the unpickled array goes out of scope in the process that unpickled it, unless that process changes the value of the :attr:`xndarray.scope` attribute.

        ``None``
            The shared memory will not be automatically released -- it is the user's responsibility to release the shared memory using method :meth:`release`. Otherwise, the shared memory will be released by the memory manager, but will result in a memory leak in the meantime.


        .. rubric:: Examples

        .. testcode::

          from jztools.shared_memory import XSharedMemoryManager, xndarray
          from concurrent.futures import ProcessPoolExecutor
          import numpy as np

          with XSharedMemoryManager() as smm:

            # Fill an xndarry with random data
            N = int(1e6)
            rng = np.random.default_rng(0)
            xarr = smm.empty(N)
            rng.random(N, out=xarr)

            # Take the mean of both halves in different processes
            futures = []
            with ProcessPoolExecutor() as pool:
              futures.append(pool.submit(np.mean, xarr[:N//2]))
              futures.append(pool.submit(np.mean, xarr[N//2:]))

            results = [x.result().item() for x in futures]
            print(results)

        .. testoutput::

          [0.5001123274748334, 0.5002061854525354]



        """

        if scope not in ("local", "remote", None):
            raise Exception(
                "Argument `scope` must be one of 'local', 'remote', or `None`."
            )

        if isinstance(sh_mem, str):
            # Load shared memory buffer from the manager.
            sh_mem = SharedMemory(name=sh_mem, create=False)
        elif not (
            # The shared memory buffer was previously loaded.
            isinstance(sh_mem, SharedMemory)
            or sh_mem is None
        ):  # The array should is of size 0.
            raise Exception("Invalid input arguments.")

        out = super().__new__(
            cls,
            shape,
            dtype=dtype,
            buffer=(None if sh_mem is None else sh_mem.buf),
            offset=offset,
            strides=strides,
        )

        # Verify that the array is of size 0 for ``sh_mem=None``.
        if sh_mem is None and out.size != 0:
            raise Exception(
                f"Array of size 0 expected for ``sh_mem=None``, but got `{out.size}`."
            )

        # Set special attributes
        out.scope = scope
        out._shared_memory_manager = manager
        out._shared_memory = sh_mem

        return out

    @property
    def shared_memory_manager(self):
        """
        Inherited from the source xndarray.
        """
        obj = self
        while not (
            _shared_memory_manager := getattr(obj, "_shared_memory_manager", None)
        ):
            if not isinstance((obj := obj.base), xndarray):
                return None
        return _shared_memory_manager

    def is_shared(self):
        """
        Returns ``True`` if the xndarray points to data in shared memory. If ``False``, the xndarray is no different than a regular ``numpy.ndarray``
        """

        obj = self
        while not isinstance(obj := obj.base, (mmap, type(None))):
            pass
        return obj is not None

    @property
    def shared_memory(self):
        """
        Inherited from the source xndarray.
        """
        obj = self
        while not (_shared_memory := getattr(obj, "_shared_memory", None)):
            if not isinstance((obj := obj.base), xndarray):
                return None
        return _shared_memory

    def release(self):
        """
        Unlinks the underlying shared memory block. The array should not be accessed after it is released. In most situations, calling this method should not be necessary, as the ``scope`` based mechanism deals with releasing the underlying shared memory.

        For example, if the array was initialized with the ``scope='local'`` keyword arg (the default), this method is called as part of the array's ``__del__``.
        """

        # A less verbose alternative to unlinking is
        #
        # >>>   self.shared_memory.unlink()
        #
        # But this does not do the required bookkeeping in the shared
        # memory manager server process.
        try:
            client = self.shared_memory_manager._Client(
                self.shared_memory_manager.address,
                authkey=self.shared_memory_manager._authkey,
            )
        except FileNotFoundError:
            # The shared memory manager exited and hopefully unlinked all memory.
            pass
        else:
            with client as conn:
                dispatch(conn, None, "release_segment", (self.shared_memory.name,))
        #
        self.shared_memory.close()

    @classmethod
    def from_ndarray(
        cls, manager: XSharedMemoryManager, arr: np.ndarray, **kwargs
    ) -> "xndarray":
        """
        Creates a new array in newly-allocated shared memory and copies the data in arr to it.

        Keyword arguments are passed :meth:`empty`.
        """
        order = kwargs.pop(
            "order", "C" if arr.flags.carray else ("F" if arr.flags.farray else None)
        )
        out = cls.empty(manager, arr.shape, arr.dtype, order=order, **kwargs)
        out[:] = arr[:]

        return out

    def as_ndarray(self) -> np.ndarray:
        """
        Returns a copy of the array in non-shared memory.
        """
        return np.copy(self.view(np.ndarray))

    @classmethod
    def empty(cls, manager, shape, dtype=float, order="C", scope="local") -> "xndarray":
        """
        Creates an uninitialized array in newly-allocated shared memory.
        """
        # Create the share memory.
        if (size := np.prod(shape)) > 0:
            num_bytes = size * np.dtype(dtype).itemsize
            sh_mem = manager.SharedMemory(num_bytes)
        else:
            sh_mem = None

        # Initialize the class
        sh_arr = cls(sh_mem, manager, shape, dtype, offset=0, strides=None, scope=scope)

        return sh_arr

    def _get_offset(self):
        if isinstance(self.base, mmap):
            return 0
        else:
            return (
                # np.byte_bounds(self)[0] - np.byte_bounds(self.base)[0]
                self.__array_interface__["data"][0]
                - self.base.__array_interface__["data"][0]
                # 0
            )

    def __reduce__(self):
        """
        Used by pickle.
        """

        try:
            # TODO: What happens when you create a remote array with remote scope,
            # but send only a slice of that array to the local process. That slice
            # will have scope=None, and the array will hence leak.
            if not self.is_shared():
                return super().__reduce__()
            else:
                offset = self._get_offset()
                return xndarray, (
                    self.shared_memory.name if self.shared_memory else None,
                    self.shared_memory_manager,
                    self.shape,
                    self.dtype,
                    offset,
                    self.strides,
                    self._SCOPE_PICKLE_MAPPING[self.scope],
                )
        except Exception as err:
            # Exceptions in reduce can cause pickle to hang,
            # print this exception.
            print(exception_string(err), flush=True)
            raise

    def __del__(self):
        if self.scope == "local":
            self.release()
