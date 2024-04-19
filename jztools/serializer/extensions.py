from .abstract_type_serializer import AbstractTypeSerializer
from numpy import dtype, ndarray, datetime64
from numpy.lib.format import dtype_to_descr, descr_to_dtype
import base64


class SliceSerializer(AbstractTypeSerializer):
    @classmethod
    def check_type(cls, obj):
        return isinstance(obj, slice)

    @classmethod
    def _as_serializable(cls, obj):
        return (obj.start, obj.stop, obj.step)

    @classmethod
    def _from_serializable(cls, val):
        return slice(*val)


class DtypeSerializer(AbstractTypeSerializer):
    @classmethod
    def check_type(cls, obj):
        return isinstance(obj, dtype)

    @classmethod
    def _as_serializable(cls, obj):
        return dtype_to_descr(obj)

    @classmethod
    def _from_serializable(cls, val):
        return descr_to_dtype(val)


class NDArraySerializer(AbstractTypeSerializer):
    @classmethod
    def check_type(cls, obj):
        return isinstance(obj, (ndarray, datetime64))

    @classmethod
    def _as_serializable(cls, arr):
        from jztools.numpy import encode_ndarray

        return base64.b64encode(encode_ndarray(arr)).decode("ascii")

    @classmethod
    def _from_serializable(cls, base64_arr):
        from jztools.numpy import decode_ndarray

        return decode_ndarray(base64.b64decode(base64_arr.encode("ascii")))
