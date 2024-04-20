from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from logging import getLogger
from threading import RLock
from .utils import utc_now
from typing import List, Optional, Dict, Any, Set, Type, Union
from xerializer.abstract_type_serializer import Serializable
import abc
from .recorded_attributes import (
    NonMatchingCallArgs,
    RecordedAttribute,
    PlayedBackAttribute,
    RecordedCall,
    PlayedBackCall,
)
from .utils import base_get, get_obj

LOGGER = getLogger(__name__)


_DO_PATCH_GETATTRIBUTE = True
# Whether to use the overloaded __getattribute__ in objects that implement overloads.


@contextmanager
def with_vanilla_getattribute():
    global _DO_PATCH_GETATTRIBUTE
    orig = _DO_PATCH_GETATTRIBUTE
    _DO_PATCH_GETATTRIBUTE = False
    yield
    _DO_PATCH_GETATTRIBUTE = orig


class NonMatchingRequest(Exception):
    pass


class NoCallRecordsLeft(StopIteration):
    # This inherits from StopIteration to support __next__ ending.
    pass


# Object recorder attributes are not accessible in the standard way. Use the convenience methods below.


class _WrapperContextManager:
    # The __enter__ and __exit__ methods cannot be implemented directly in the wrapper
    # object because they are required to support the base object's context management and
    # are monkey-patched into the wrapper by function `_from_derived_class_with_special_attribs`.
    def wrapper_enter(self):
        """Supports context manager functionality for the wrapper object. Implement this method instead of __enter__"""
        return self

    def wrapper_exit(self, *args, **kwargs):
        """Supports context manager functionality for the wrapper object. Implement this method instead of __exit__"""
        return None

    def as_context_manager(self):
        class AsContextManager:
            def __init__(self, wrapped_obj):
                self.obj = wrapped_obj

            def __enter__(self):
                return base_get(self.obj, "wrapper_enter")()

            def __exit__(self, *args, **kwargs):
                return base_get(self.obj, "wrapper_exit")(*args, **kwargs)

        return AsContextManager(self)


_SPECIAL_ATTRIBUTES = {
    "__getitem__",
    "__bool__",
    "__len__",
    "__enter__",
    "__exit__",
    "__call__",
    "__iter__",
    "__next__",
    "__call__",
}
""" ObjectRecorder classes will be derived from the same class to include these special attributes if the wrapped object has them too."""


class _WrapperGetAttribute(abc.ABC):
    """
    Supports the overloaded __getattribute__ mechanism, as well as a context-based temporary override of this mechanism.
    """

    recordings: List[
        Union[RecordedAttribute, RecordedCall, PlayedBackAttribute, PlayedBackCall]
    ]
    _attribute_overloads: Optional[Dict[str, Any]] = None
    _patch_getattribute = True

    def __getattribute__(self, name):
        if _DO_PATCH_GETATTRIBUTE:
            if name in (_attribute_overloads := base_get(self, "_attribute_overloads")):
                return _attribute_overloads[name]
            else:
                return base_get(self, "_getattribute")(name)
        else:
            return base_get(self, name)

    @contextmanager
    def with_vanilla_getattribute(self):
        orig_value = base_get(self, "_patch_getattribute")
        try:
            self._patch_getattribute = False
            with ExitStack() as exit_stack:
                [
                    exit_stack.enter(base_get(rec.value, "with_vanilla_getattribute")())
                    for rec in base_get(self, "recordings")
                    if hasattr(rec.value, "with_vanilla_getattribute")
                ]
                yield
        finally:
            self._patch_getattribute = orig_value

    @staticmethod
    def _get_special_attribs(in_class: Type):
        # WrapperGetAttribute-derived classes will be dynamically derived to
        # have these special attributes if the wrapped object also has them.
        return _SPECIAL_ATTRIBUTES.intersection(dir(in_class))

    @classmethod
    def _from_derived_class_with_special_attribs(
        cls,
        special_attribs: Union[Type, Set[str]],
        *args,
        overloads=None,
        **kwargs,
    ):
        # This method first derives a new class from ObjectRecorder or ObjectPlayer. The new class
        # will have those special methods that get called by python with a mechanism that bypasses __getattribute__
        # appended as attributes so that they are instead called via __getattribute__. The method then
        # instantiates a new instance of this derived class and returns the resulting object.

        updated_attributes = {}

        # Append other special attribs as ObjecPlayer objects (done implicitly by __getattribute__).
        for special_name in special_attribs:
            updated_attributes[special_name] = property(
                (lambda self: base_get(self, "_call"))
                if special_name == "__call__"
                else (
                    lambda self, _special_name=special_name: (
                        base_get(self, "__getattribute__")(_special_name)
                    )
                )
            )

        derived_type = type(
            "_" + cls.__name__,
            (cls,),
            {
                **{"__module__": cls.__module__},
                **updated_attributes,
                **(overloads or {}),
            },
        )

        derived_obj = object.__new__(derived_type)
        derived_type.__init__(derived_obj, *args, **kwargs)

        return derived_obj


def _append(object_recorder, recording):
    return base_get(object_recorder, "_append")(recording)


class ObjectRecorder(_WrapperContextManager, _WrapperGetAttribute, Serializable):
    """
    Records a sequence of calls to a given object and their responses. Playback using :class:`ObjectPlayer` verifies that the object is called with the same sequence and returns the corresponding stored response.

    Wrapping an API object accessing a remote service can be useful to do offline testing of code that relies on that API services.

    This object overloads :meth:`__getattribute__` so that any attribute reference will return the value of the :attr:`obj` being recorded. In order to instead access the attributes of this class, use :func:`base_get`.
    """

    obj: Any
    """
    The wrapped object. Use :func:`get_obj` to get this object and by-pass the :meth:`__getattribute__` overloads.
    """

    # This class supports all the classes automatically derived
    # from ObjectRecorder using _from_derived_class_with_special_attribs()
    # by redirecting all instantiations (by means of the signature below) to
    # the parent ObjectRecorder class and relying on the __new__ method of that class.
    signature = "rs:obj"
    inheritable = True

    def __init__(
        self,
        obj: Any,
        meta: Optional[Dict] = None,
        attribute_overloads: Optional[Dict[str, Any]] = None,
    ):
        """
        :param obj: The object to record to.
        :param meta: Extra data.
        :param attribute_overloads: Any attributes specified here will be returned instead of a recorded attribute when accessing object attributes.
        """
        self.obj = obj
        self.recordings = []
        self.meta = {
            "special_attribs": list(base_get(self, "_get_special_attribs")(type(self))),
            **(meta or {}),
        }
        self._attribute_overloads = attribute_overloads or {}
        self.lock = RLock()

    def __new__(cls, obj, *args, **kwargs):
        return cls._from_derived_class_with_special_attribs(
            cls._get_special_attribs(type(obj)), obj, *args, **kwargs
        )

    def as_serializable(self):
        return {
            "recordings": base_get(self, "recordings"),
            "meta": base_get(self, "meta"),
        }

    @classmethod
    def from_serializable(cls, recordings, meta) -> "ObjectPlayer":
        return ObjectPlayer(recordings=recordings, meta=meta)

    def _append(self, rec_attrib: RecordedAttribute):
        """
        Thread-safe appending of a new played-back attribute. Attributes should be serializable in order to be saved to the recordings file.

        Call :func:`_append` instead of this method to by-pass the :meth:`__getattribute__` overloads.
        """
        with base_get(self, "lock"):
            base_get(self, "recordings").append(rec_attrib)

    def _getattribute(self, name):
        """
        Returns the attributes of the wrapped object :attr:`obj`. To instead retrieve ``self`` attributes, use :meth:`base_get`, or the convenience aliases :meth:`append` and :meth:`get_obj`.
        """
        # Recorded attributes.

        LOGGER.debug(f"Recording attribute `{name}`.")

        access_time = utc_now()
        value = getattr(get_obj(self), name)
        attrib = RecordedAttribute(name, value, access_time)
        _append(self, attrib)
        return attrib.value

    def _call(self, *args, **kwargs):
        # Used to break an infinite loop when calling attribute `__call__`
        # from an object. Added as a class attribute in derived classes by
        # `_from_derived_class_with_special_attribs`.

        call_time = utc_now()
        value = base_get(self, "obj")(*args, **kwargs)
        base_get(self, "_append")(
            call_record := RecordedCall(
                args, kwargs, value=value, access_time=call_time
            )
        )

        return call_record.value


class ObjectPlayer(_WrapperContextManager, _WrapperGetAttribute):
    _special_methods: Set[str]

    def __init__(self, recordings, meta, attribute_overloads=None):
        self.lock = RLock()
        self.recordings = recordings
        self.meta = meta
        self._attribute_overloads = attribute_overloads or {}

    def __new__(cls, recordings, meta):
        return cls._from_derived_class_with_special_attribs(
            meta["special_attribs"], recordings, meta
        )

    def _getattribute(self, name):
        with base_get(self, "lock"):
            LOGGER.debug(f"Playing back attribute `{name}`.")
            recordings = base_get(self, "recordings")
            if not recordings:
                raise NoCallRecordsLeft(
                    f"No recording entries left when attempting to get `{name}`."
                )
            rec = recordings[0]
            # Recorded attributes.
            if rec.name != name:
                raise NonMatchingRequest(
                    f"The request `{name}` does not match the next recorded entry `{rec.name}`."
                )
            else:
                recordings.pop(0)

            if isinstance(rec, PlayedBackCall):
                # Return an object with a __call__ method
                # instead of the recorded response of the call.
                return rec
            else:
                # Return the recorded value of the attribute
                return rec.value

    def _call(self, *args, **kwargs):
        # Used to break an infinite loop when calling attribute `__call__`
        # from an object. Added as attribute `__call__` in derived classes by
        # `_from_derived_class_with_special_attribs`.

        with base_get(self, "lock"):
            recording = base_get(self, "_getattribute")("__call__")

            try:
                out = recording(*args, **kwargs)
            except NonMatchingCallArgs:
                # Undo change
                self._append(recording)
                raise
            return out
