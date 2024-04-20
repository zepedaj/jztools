from logging import getLogger
import numpy as np
from jztools.datetime64 import UTCDateTime64, UTCFlexDateTime, as_naive_utc
from typing import Dict, Any, Tuple, Type, Union
from jztools.object_recorder.global_time import GLOBAL_TIME
import pytz
from xerializer.abstract_type_serializer import Serializable
from numbers import Number

from .utils import base_get

NativelyHandledTypes = Union[
    Number,
    str,
    bytes,
    type,
    type(None),
    np.dtype,
    np.ndarray,
    np.datetime64,
    np.timedelta64,
]
"""
The base types that are handled directly. Also tuples, lists, sets and dictionaries
of these types are handled directly. All other types are handled by
wrapping them in an :class:`ObjectRecorder` instance at recording time and a
:class:`ObjecPlayer` instance at playback time.
"""

LOGGER = getLogger(__name__)


class NonMatchingCallArgs(Exception):
    pass


def get_nested_call(rec_attr: "RecordedAttribute"):
    """
    When a method of an object wrapped in a :class:`~jztools.object_recorder.RecordedObject` instance is called, by default
    a nested structure is returned consisting of 1) a :class:`RecordedAttribute` named after the called method and containing
    2) a :class:`~jztools.object_recorder.RecordedObject` containing
    3) a :class:`RecordedCall` object named `'__call__'`.

    This nested structure will support situations where method attributes besides `'__call__'` are accessed.
    But since in the vast majority of situations, only the `'__call__'` attribute is accessed, this triple-nested structure can be
    simplified to a single :class:`RecordedCall` object named after the method. This simplification is implemented by
    :meth:`RecordedAttribute.as_serializable`.

    :param rec_attr: The attribute that might be a superfluously nested recorded call.
    :return: This function detects when the above-described simplification is possible, returning the innermost :class:`RecordedCall` object if so and `None` otherwise.

    """

    from .object_recorder import ObjectRecorder  # TODO: Any way to avoid this?

    if (
        isinstance(rec_attr.value, ObjectRecorder)
        and len(recordings := base_get(rec_attr.value, "recordings")) == 1
        and isinstance(rec := recordings[0], RecordedCall)
    ):
        # Simplify the serialized attribute by converting it to a single RecordedCall
        # instead of a RecordedCall nested in a RecordedObject nested in a RecordedAttribute.
        return rec
    else:
        return None


class RecordedAttribute(Serializable):
    signature = "rs:attr"
    polymorphic = True
    managed_types: Tuple[Type] = NativelyHandledTypes.__args__
    name: str
    value: Union[NativelyHandledTypes, "ObjectRecorder"]
    access_time: UTCDateTime64

    def __init__(self, name, value, access_time: UTCDateTime64):
        from .object_recorder import ObjectRecorder

        self.name = name
        self.value = value if self.can_handle_natively(value) else ObjectRecorder(value)
        self.access_time = access_time

    @classmethod
    def can_handle_natively(cls, value: Any):
        """
        Determines if the specified attribute is handled by this class.
        """
        return (
            isinstance(value, cls.managed_types)
            or (
                isinstance(value, (tuple, list, set))
                and all(cls.can_handle_natively(_x) for _x in value)
            )
            or (
                isinstance(value, dict)
                and all(cls.can_handle_natively(_x) for _x in value.keys())
                and all(cls.can_handle_natively(_x) for _x in value.values())
            )
        )

    def as_serializable(self) -> Dict[str, Any]:
        from .object_recorder import ObjectRecorder

        if rec := get_nested_call(self):
            # Simplify the serialized attribute by converting it to a single RecordedCall
            # instead of a RecordedCall nested in a RecordedObject nested in a RecordedAttribute.
            # See the documentation in :func:`get_nested_call`
            return {
                "__type__": type(rec).signature,
                **rec.as_serializable(),
                "name": self.name,
            }

        else:
            # Return the full attribute with a nested object.
            return {
                "name": self.name,
                "value": self.value,
                "access_time": str(as_naive_utc(self.access_time, pytz.UTC)),
            }

    @classmethod
    def from_serializable(cls, name, value, access_time) -> "PlayedBackAttribute":
        return PlayedBackAttribute(name, value, np.datetime64(access_time))


class PlayedBackAttribute:
    name: str
    _value: Union[NativelyHandledTypes, "ObjectPlayer"]
    access_time: UTCDateTime64

    def __init__(self, name, value, access_time: UTCFlexDateTime):
        self.name = name
        self._value = value
        self.access_time = as_naive_utc(access_time, pytz.UTC)

    @property
    def value(self):
        if GLOBAL_TIME:
            GLOBAL_TIME.move_to(self.access_time)
        return self._value


class _Call:
    name: str
    args: Tuple
    kwargs: Dict[str, Any]

    _default_name = "__call__"

    def __init__(self, call_args, call_kwargs, **extra):
        extra.setdefault(
            "name", self._default_name
        )  # `RecordedAttribute.as_serializable` might set this name to implement polymorphic serialization for more readable files.
        super().__init__(**extra)
        self.args = call_args
        self.kwargs = call_kwargs

    def _str(self, value):
        return f"<{type(self).__name__} `{self.name}`>[{self.args}, {self.kwargs}: {value}]"


class RecordedCall(_Call, RecordedAttribute, Serializable):
    # Inherited: value:Any; access_time:UTCFlexDateTime
    signature = "rs:call"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log()

    def __str__(self):
        return self._str(self.value)

    def log(self):
        LOGGER.debug(str(self))

    def as_serializable(self):
        out = {
            **super().as_serializable(),
            "args": list(self.args),
            "kwargs": self.kwargs,
        }
        [out.pop(_fld) for _fld in ["args", "kwargs"] if not out[_fld]]
        if out["name"] == self._default_name:
            out.pop("name")

        return out

    @classmethod
    def from_serializable(cls, value, access_time, args=None, kwargs=None, name=None):
        return PlayedBackCall(
            tuple(args or []),
            kwargs or {},
            value=value,
            access_time=as_naive_utc(access_time, pytz.UTC),
            name=name,
        )


class PlayedBackCall(_Call, PlayedBackAttribute):
    def __str__(self):
        return self._str(self._value)

    def match(self, args, kwargs) -> bool:
        return self.args == args and self.kwargs == kwargs

    def __call__(self, *args, **kwargs):
        LOGGER.debug(str(self))
        if not self.match(args, kwargs):
            raise NonMatchingCallArgs()
        else:
            return self.value
