from numbers import Number
from jztools.py import filelike_open
import json
from jztools.py import class_name as _class_name
from typing import Iterable
from .extensions import (
    DtypeSerializer as _DtypeSerializer,
    SliceSerializer as _SliceSerializer,
    NDArraySerializer as _NDArraySerializer,
)
from .abstract_type_serializer import AbstractTypeSerializer

import warnings

warnings.warn(
    DeprecationWarning(
        "Module jztools.serializer is deprecated in favor of xerializer."
    )
)


def class_name(x):
    return _class_name(x, stripped_modules=[])


class ExtensionMissing(TypeError):
    def __init__(self, in_type):
        super().__init__(
            f"Serializable type {in_type} is not recognized by the installed extensions."
        )


class UnserializableType(TypeError):
    def __init__(self, in_type):
        super().__init__(
            f"Type {in_type} cannot be serialized by the installed extensions."
        )


class Serializer:
    """
    Extension of JSON serializer that also supports objects implementing or being supported by a :class:`TypeSerializable` interface as well lists, tuples, sets and dictionaries (with string keys) of such objects. Note that, unlike the default json behavior, :class:`Serializer` preserves types such as tuple and list.

    Default extensions include :class:`slice` objects and :class:`numpy.dtype` objects.
    """

    default_extension_types = [_SliceSerializer, _DtypeSerializer, _NDArraySerializer]
    """
    Contains list of external supported types. External modules register their types by appending to this class-level list.
    """

    def __init__(self, extension_types: Iterable[AbstractTypeSerializer] = tuple()):
        """
        :param extension_types: List of types that implement :class:`Serializable` that the serializer will take into account.
        """
        self.user_extension_types = tuple(extension_types)
        self.builtin_iterable_types = {
            class_name(_type): _type for _type in (tuple, set)
        }
        self.constant_types = (Number, str, type(None))

    @property
    def extension_types(self):
        return {
            class_name(_t): _t
            for _t in (*self.default_extension_types, *self.user_extension_types)
        }

    def as_serializable(self, obj):
        if type(obj) == dict:
            if "__type__" not in obj:
                # 1st, more readable way to encode a dictionary
                # (when it does not have a '__type__' field).
                return {key: self.as_serializable(val) for key, val in obj.items()}
            else:
                # 2nd, more verbose way to encode a dictionary
                # (when it has a '__type__' field).
                return {
                    "__type__": class_name(dict),
                    "__value__": {
                        key: self.as_serializable(val) for key, val in obj.items()
                    },
                }

        elif type(obj) == list:
            return [self.as_serializable(val) for val in obj]

        elif type(obj) in self.builtin_iterable_types.values():
            return {
                "__type__": class_name(type(obj)),
                "__value__": [self.as_serializable(val) for val in obj],
            }

        elif isinstance(obj, self.constant_types):
            return obj

        elif _ext_type := [
            _x for _x in self.extension_types.values() if _x.check_type(obj)
        ]:
            _ext_type = _ext_type[0]
            return {
                "__type__": class_name(_ext_type),
                "__value__": self.as_serializable(_ext_type._as_serializable(obj)),
            }
        else:
            raise UnserializableType(type(obj))

    def from_serializable(self, obj):
        if type(obj) == dict:
            if "__type__" not in obj:
                # 1st, more readable way to encode a dictionary
                # (when it does not have a '__type__' field).
                return {key: self.from_serializable(val) for key, val in obj.items()}
            elif obj["__type__"] == class_name(dict):
                # 2nd, more verbose way to encode a dictionary
                # (when it has a '__type__' field).
                return {
                    key: self.from_serializable(val)
                    for key, val in obj["__value__"].items()
                }
            elif obj["__type__"] in self.builtin_iterable_types.keys():
                return self.builtin_iterable_types[obj["__type__"]](
                    [self.from_serializable(val) for val in obj["__value__"]]
                )
            elif _ext_type := self.extension_types.get(obj["__type__"], None):
                return _ext_type._from_serializable(
                    self.from_serializable(obj["__value__"])
                )
            else:
                raise ExtensionMissing(obj["__type__"])

        elif type(obj) == list:
            return [self.from_serializable(val) for val in obj]

        elif isinstance(obj, self.constant_types):
            return obj

        else:
            raise TypeError(f"Invalid input type {type(obj)}.")

    def serialize(self, obj, *args, **kwargs):
        return json.dumps(self.as_serializable(obj), *args, **kwargs)

    def deserialize(self, obj, *args, **kwargs):
        return self.from_serializable(json.loads(obj, *args, **kwargs))

    # JSON-like interface
    loads = deserialize
    dumps = serialize

    def load(self, filelike, *args, **kwargs):
        with filelike_open(filelike, "r") as fo:
            return self.from_serializable(json.load(fo, *args, **kwargs))

    def load_safe(self, filelike, *args, **kwargs):
        """
        Similar to load, but with no errors on empty files. Returns (obj, 'success') on success,  (None, 'empty') if the file is empty, or (None, 'missing') if the file does not exist.
        """

        fo_cm = filelike_open(filelike, "r")
        try:
            fo = fo_cm.__enter__()
        except FileNotFoundError:
            return (None, "missing")
        else:
            with filelike_open(filelike, "r") as fo:
                try:
                    obj = json.load(fo, *args, **kwargs)
                except json.JSONDecodeError as err:
                    if str(err) == r"Expecting value: line 1 column 1 (char 0)":
                        return (None, "empty")
                    else:
                        raise
                else:
                    return (self.from_serializable(obj), "success")
        finally:
            fo_cm.__exit__(None, None, None)

    def dump(self, obj, filelike, *args, **kwargs):
        with filelike_open(filelike, "w") as fo:
            json.dump(self.as_serializable(obj), fo, *args, **kwargs)
