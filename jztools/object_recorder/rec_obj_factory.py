import abc
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union
from jztools.validation import checked_get_single
from .object_recorder import ObjectPlayer, ObjectRecorder
from .rec_obj_factory__defs import (
    ManagedType,
    RecordedManagedType,
    PlayedBackManagedType,
)


from jztools.object_recorder.object_recorder import ObjectPlayer, ObjectRecorder
from jztools.validation import checked_get_single

RecObjFactorySpec = Union[
    "RecObjFactory", Callable, Tuple[Type, Optional[Tuple], Optional[Dict]]
]
"""
Parameters to be passed to :class:`RecObjFactory`
Object factory specifiers can be provided as a callable, or as a tuple of class/callable, args and kwargs.

When a tuple with a type is provided, any registered object factories will be used for that type.
"""


REGISTERED_OBJECT_FACTORIES = {}


def register_rec_obj_factory(
    in_type: Type, factory: Type["RecObjFactory"], overwrite=True
):
    """
    Register a new recorded object factory.
    """
    if overwrite:
        REGISTERED_OBJECT_FACTORIES[in_type] = factory
    else:
        REGISTERED_OBJECT_FACTORIES.setdefault(in_type, factory)


class RecObjFactory(abc.ABC):
    """
    When deriving this class, use class keyword argument `register=<handled type>` to register
    the factory as a handler for the specified type.
    """

    def __init__(self, in_type, *args, **kwargs):
        # Default implementation -- this method can be overridden.
        self.in_type = in_type
        self.args = args
        self.kwargs = kwargs

    def __init_subclass__(cls, *args, register=None, **kwargs):
        if register is not None:
            register_rec_obj_factory(register, cls)
        return super().__init_subclass__(*args, **kwargs)

    def build_live(self) -> ManagedType:
        """Behaves like a no-op returning the actual object without recording."""
        # Default implementation -- this method can be overriden.
        return self.in_type(*self.args, **self.kwargs)

    @abc.abstractmethod
    def build_recorded(
        self,
    ) -> Tuple[RecordedManagedType, List[ObjectRecorder]]:
        """
        Returns a 1) the managed object as an object recorder or with the components that are object recorders patched in; and
        2) a list of all recorded object components (possibly containing only the managed object as an object recorder).
        """

    @abc.abstractmethod
    def build_played_back(
        self, recordings: List[ObjectPlayer]
    ) -> Tuple[PlayedBackManagedType, List[ObjectPlayer]]:
        """
        Takes the recordings extracted from the recorders produced by :meth:`build_recorded` and returns 1) an object of the managed type as a played-back object or the  with
        played-back components monkey-patched into it and 2) the played-back components as a list.
        """


def flex_create(flex_args: RecObjFactorySpec) -> RecObjFactory:
    """
    Creates a :class:`RecObjFactory` from a variety of inputs.
    """
    if isinstance(flex_args, RecObjFactory):
        # Already a RecObjFactory
        return flex_args
    if callable(flex_args):
        target_cls = REGISTERED_OBJECT_FACTORIES.get(flex_args, DefaultRecObjFactory)
        return target_cls(flex_args)
    if isinstance(flex_args, (tuple, list)) and 1 <= len(flex_args) <= 3:
        # A callable or type optionally with args and kwargs.
        in_type, args, kwargs = list(flex_args) + [(), {}][len(flex_args) - 1 :]
        target_cls = REGISTERED_OBJECT_FACTORIES.get(in_type, DefaultRecObjFactory)
        return target_cls(in_type, *args, **kwargs)
    raise ValueError(
        "Invalid input values whan attempting to create an RecObjFactory object."
    )


class DefaultRecObjFactory(RecObjFactory):
    """
    Default factory.
    """

    def build_recorded(self) -> Tuple[RecordedManagedType, List[ObjectRecorder]]:
        """Returns the managed object with the component recorders produced by :meth:`build_component_recorders` monkey-patched into it."""
        out = ObjectRecorder(self.build_live())
        return out, [out]

    def build_played_back(
        self, recordings: List[ObjectPlayer]
    ) -> Tuple[PlayedBackManagedType, List[ObjectPlayer]]:
        """
        Takes the recordings extracted from the recorders produced by :meth:`build_component_recorders` and returns a played-back object.
        Each recording will have the form of a dictionary with fields ``'recordings'`` and ``'meta'``.
        """
        out = checked_get_single(recordings)
        return out, [out]
