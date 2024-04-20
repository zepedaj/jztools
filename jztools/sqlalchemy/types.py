from sqlalchemy.types import TypeDecorator, VARCHAR, LargeBinary
import json
import abc
from xerializer import Serializer as _Serializer
import gzip
import numpy
import io
import numpy as np
import plotly.graph_objects as go
from numpy.lib.format import dtype_to_descr, descr_to_dtype
from jztools.py import class_name, class_from_name
from sqlalchemy.ext import serializer as sqla_serializer


class JSONEncodedType(TypeDecorator):
    "Represents an immutable structure as a json-encoded string."

    impl = VARCHAR
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return json.dumps(value)

    def process_result_value(self, value, dialect):
        return None if value is None else json.loads(value)


def create_serializable_type(serializer=None):
    """
    Creates a serializable type that uses the specified serializer.
    """
    serializer = serializer or _Serializer()
    return type("SerializableType", (_SerializableType,), {"serializer": serializer})


class _SerializableType(
    TypeDecorator,
    # metaclass=type("SerializableTypeMeta", (abc.ABCMeta,), {}),
):
    impl = VARCHAR
    cache_ok = True

    @property
    @abc.abstractmethod
    def serializer(self):
        """
        The :meth:`xerializer.Serializer` instance to use for serialization.
        """

    def process_bind_param(self, value, dialect):
        return self.serializer.serialize(value)

    def process_result_value(self, value, dialect):
        return self.serializer.deserialize(value)


class NumpyDtypeType(TypeDecorator):
    "Represents a numpy.dtype object."

    impl = VARCHAR
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return json.dumps(dtype_to_descr(value))

    def process_result_value(self, value, dialect):
        return descr_to_dtype(json.loads(value))


class NumpyType(TypeDecorator):
    "Represents an immutable structure as a json-encoded string."

    impl = LargeBinary
    cache_ok = True

    def process_bind_param(self, value, dialect):
        # assert isinstance(value, np.ndarray)
        value = np.require(value)
        memfile = io.BytesIO()
        np.save(memfile, value)
        memfile.seek(0)
        #
        uncompressed_bytes = memfile.read()
        compressed_bytes = gzip.compress(uncompressed_bytes)
        #
        return compressed_bytes

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        decompressed_bytes = gzip.decompress(value)
        memfile = io.BytesIO(decompressed_bytes)
        memfile.seek(0)
        return np.load(memfile)


class PlotlyFigureType(TypeDecorator):
    "Represents an immutable structure as a json-encoded string."

    impl = VARCHAR
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return value.to_json()

    def process_result_value(self, value, dialect):
        return go.Figure(json.loads(value))


class ClassType(TypeDecorator):
    """
    Represents a class, stored as the module path and class name.
    """

    impl = VARCHAR
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return class_name(value)

    def process_result_value(self, value, dialect):
        return class_from_name(value)


def sql_query_type_builder(session_class, bound_metadata):
    """
    .. todo:: Needs testing and mapping to sqlalchemy core from ORM.
    """

    class SQLQueryType(TypeDecorator):
        impl = VARCHAR
        cache_ok = True
        Session = session_class
        metadata = bound_metadata

        def process_bind_param(self, value, dialect):
            if isinstance(value, tuple):
                if len(value) != 1:
                    raise Exception("Invalid input")
                value = value[0]
            return sqla_serializer.dumps(value)

        def process_result_value(self, value, dialect):
            return sqla_serializer.loads(value, self.metadata, self.Session)

    return SQLQueryType
