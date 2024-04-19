import numpy as np
from typing import Union, Optional, Tuple
from jztools.serializer import Serializer as _Serializer
from numpy.lib import recfunctions as recfns
from typing import TypeVar

try:
    # Does not exist for numpy~<1.20
    from numpy.typing import ArrayLike
except Exception:
    ArrayLike = TypeVar("ArrayLike")


def randomizer(specifier: Optional[Union[bool, int, np.random.Generator]]):
    """
    Outputs a numpy.random.Generator based on the input.
    :param specifier: An integer used as the seed, or a passed-through :class:`numpy.random.Generator` object or :attr:`False` to return :attr:`None`.
    """
    if isinstance(specifier, bool):
        return np.random.default_rng() if specifier else False
    elif isinstance(specifier, int):
        return np.random.default_rng(specifier)
    elif isinstance(specifier, np.random.Generator):
        return specifier
    else:
        raise TypeError(
            f"Type {bool}, {int} (seed) or {np.random.Generator} required, but got {type(specifier)}."
        )


def random_array(shape, dtype, rng=None):
    """
    Returns an array from random bytes of the specified dtype (can be structured).
    """
    #
    if np.dtype(dtype).itemsize == 0:
        return np.empty(shape, dtype=dtype)
    else:
        rng = np.random.default_rng(rng)
        #
        size = np.prod(shape)
        dtype = np.dtype(dtype)
        arr = rng.integers(8, size=size * dtype.itemsize).astype("u1")
        #
        arr.dtype = dtype
        arr = arr.reshape(shape)
    #
    return arr


def structured_to_unstructured(arr, dtype=None):
    """
    Alternative to numpy.lib.recfunctions import structured_to_unstructured, which has a memory leak in
    """
    # Choose dtype
    if dtype is None:
        dtype = arr.dtype
        if not all([dtype[0] == dtype[_k] for _k in range(len(dtype))]):
            raise Exception("Cannot deduce dtype!")
        dtype = dtype[0]

    # Convert
    out = np.empty(arr.shape + (len(arr.dtype),), dtype=dtype)
    for _kfld, _fld in enumerate(arr.dtype.names):
        out[:, _kfld] = arr[_fld]
    return out


def argmax_accumulate(a):
    """
    Returns the accumulated max and argmax.
    https://stackoverflow.com/questions/37855059/why-does-accumulate-work-for-numpy-maximum-but-not-numpy-argmax
    """
    am = np.maximum.accumulate(a)
    a1 = np.zeros_like(a, dtype="i")
    ind = np.nonzero(a == am)[0]
    a1[ind] = ind
    np.maximum.accumulate(a1, out=a1)

    return am, a1


def raw_encode_ndarray(arr: np.ndarray) -> Tuple[bytes, np.dtype, Tuple[int]]:
    """
    Produces a bytes string containing the input array, which can be re-constructed from the bytes string using decode_ndarray.

    :return: 3-tuple of bytes representation of content, dtype and shape.

    (See also :func:`encode_ndarray`.)
    """

    # Enforce packed fields.
    dtype = recfns.repack_fields(np.empty(0, dtype=arr.dtype)).dtype
    shape = arr.shape

    # Re-shaping to size required to support 0-dim arrays like np.array(1) .
    packed_arr = np.reshape(np.require(arr, dtype=dtype, requirements="C"), arr.size)
    u1_arr = packed_arr.view("u1")
    u1_arr.shape = u1_arr.size
    u1_arr = u1_arr.data

    return u1_arr, dtype, shape


def raw_decode_ndarray(
    arr_bytes: bytes, dtype: np.dtype, shape: Tuple[int]
) -> np.ndarray:
    """
    Inverts the operation of :func:`raw_encode_ndarray`.

    (See also :func:`encode_ndarray`.)
    """
    ndarray_spec = {"dtype": dtype, "shape": shape}
    out_arr = np.empty(shape=ndarray_spec["shape"], dtype=ndarray_spec["dtype"])

    # Re-shaping to size required to support 0-dim arrays like np.array(1).
    out_arr.shape = out_arr.size
    out_buffer = out_arr.view(dtype="u1")
    out_buffer.shape = np.prod(out_buffer.shape)
    out_arr.shape = ndarray_spec["shape"]

    out_buffer[:] = bytearray(arr_bytes)

    return out_arr


def encode_ndarray(arr: np.ndarray, serializer=None) -> bytes:
    """
    Produces a bytes string containing the input array, including shape and dtype information. The original array can be re-constructed from the output bytes string using decode_ndarray.

    (See also :func:`raw_encode_ndarray`.)
    """
    serializer = serializer or _Serializer()

    u1_arr, dtype, shape = raw_encode_ndarray(arr)
    ndarray_spec = {"shape": shape, "dtype": dtype}

    # Add header as bytes
    ndarray_specs_as_bytes = serializer.serialize(ndarray_spec).encode("utf-8")
    ndarray_specs_len_as_bytes = (len(ndarray_specs_as_bytes)).to_bytes(8, "big")
    return ndarray_specs_len_as_bytes + ndarray_specs_as_bytes + u1_arr


def decode_ndarray(arr_bytes: bytes, serializer=None) -> np.ndarray:
    """
    Inverts the operation of :func:`encode_ndarray`.

    (See also :func:`raw_decode_ndarray`.')
    """
    serializer = serializer or _Serializer()
    ndarray_specs_len_as_bytes = int.from_bytes(arr_bytes[:8], "big")
    ndarray_spec = serializer.deserialize(
        arr_bytes[8 : (data_start := (8 + ndarray_specs_len_as_bytes))].decode("utf-8")
    )
    out_arr = raw_decode_ndarray(
        arr_bytes[data_start:], ndarray_spec["dtype"], ndarray_spec["shape"]
    )

    return out_arr


class CircularArray:
    def __init__(self, shape, dtype=None, values=None):
        """
        :param shape: The shape of the circular array. Circular inserts occur over the first dimension.
        """
        self._buffer = np.empty(shape, dtype=dtype)
        self._curr_size = 0
        self.posn = 0

        if values is not None:
            self.batch_insert(values)

    def copy(self):
        out = CircularArray(self._buffer.shape, dtype=self._buffer.dtype)
        out._buffer[:] = self._buffer
        out._curr_size = self._curr_size
        out.posn = self.posn
        return out

    @property
    def capacity(self):
        return len(self._buffer)

    def insert(self, entry):
        self.batch_insert([entry])
        # self._buffer[self.posn] = entry
        # self._curr_size = min(self._curr_size + 1, len(self._buffer))
        # self.posn = (self.posn + 1) % self.capacity

    def batch_insert(self, entries):
        entries = entries[(-self.capacity or len(entries)) :]  # Fails if capacity is 0
        first_part = entries[: self.capacity - self.posn]  # Fails if capacity is 0
        second_part = entries[self.capacity - self.posn :]
        self._buffer[self.posn : self.posn + len(first_part)] = first_part
        self._buffer[
            self.posn + len(first_part) : self.posn + len(first_part) + len(second_part)
        ] = second_part
        self._curr_size = min(self._curr_size + len(entries), len(self._buffer))
        self.posn = (self.posn + len(entries)) % self.capacity

    def copy_to(self, target: np.ndarray):
        np.concatenate(
            (self._buffer[self.posn : self._curr_size], self._buffer[: self.posn]),
            out=target,
        )

    def __len__(self):
        return self._curr_size

    def get(self):
        target = np.empty_like(self._buffer)
        target = target[: self._curr_size]
        self.copy_to(target)
        return target
