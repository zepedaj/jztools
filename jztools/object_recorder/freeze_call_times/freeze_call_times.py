from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import wraps
from threading import Lock
from traceback import format_stack
from freezegun import freeze_time as _freeze_time
import numpy as np
from typing import Any, Callable, List, Optional, Tuple, Type, Union
from jztools.colorama import colored_text
from jztools.object_recorder.freeze_call_times.stack import Stack
from jztools.object_recorder.global_time import GLOBAL_TIME
from jztools.object_recorder.object_recorder import _WrapperContextManager
from jztools.py import entity_from_name
from jztools.validation import checked_get_single
import pytz
from xerializer.abstract_type_serializer import Serializable
from xerializer.decorator import serializable
from ..rec_obj_factory import RecObjFactory
from datetime import datetime


def utc_now():
    # Removes a dependency and avoids an infinite loop when
    # patching that dependency
    return np.datetime64(datetime.utcnow())


def as_list(x: Any):
    return x if isinstance(x, list) else [x]


TargetsType = Union[Tuple[Any, str], List[Tuple[Any, str]]]


@dataclass
class _CallablePatcherFactory:
    targets: TargetsType
    """The object(s) and attribute name(s) Where the time-frozen callable will be substituted"""
    fxn: Callable = None
    """The callable who's calls are to be frozen in time. If not provided, will be set to the first entry in targets."""

    def __post_init__(self):
        self.targets = [
            (entity_from_name(mdl) if isinstance(mdl, str) else mdl, attr_name)
            for mdl, attr_name in as_list(self.targets)
        ]
        if self.fxn is None:
            fxns = [getattr(mdl, attr_name) for mdl, attr_name in self.targets]
            if not all(_f is fxns[0] for _f in fxns):
                raise Exception(
                    "The callables in the specified targets are not all the same! Provide an explicit `fxn` argument if intentional."
                )
            self.fxn = fxns[0]

    def build_live(self):
        return self.fxn

    @abstractmethod
    def build_recorded(self): ...

    @abstractmethod
    def build_played_back(self, recordings: List["CallTimeRecord"]): ...


@dataclass
class freeze_call_times(_CallablePatcherFactory, RecObjFactory):
    """
    Wraps a callable so that the time of each call is frozen and recorded. At playback, the time is frozen
    to the recorded call time, and then the actual callable is called. No checks are made that
    the recording arguments match the playback arguments.

    Currently, only non-locally defined callables are supported so that the containing context
    can be inferred from the callable's name.


    .. testsetup:: freeze

        from jztools.object_recorder import factory
        from tempfile import TemporaryDirectory

        temp_dir = TemporaryDirectory()

        _, recording_switch = factory(temp_dir.__enter__())

    .. testcode:: freeze

        from jztools.object_recorder import freeze_call_times
        from jztools.object_recorder.example import MyExampleClass

        my_obj = MyExampleClass(
            3
        )  # Objects instantiated before the call will also be patched.
        orig_method = MyExampleClass.my_method
        for args in [
            ((MyExampleClass, "my_method"), MyExampleClass.my_method),
            ((MyExampleClass, "my_method"),),
        ]:
            with recording_switch(freeze_call_times(*args), rec_mode="RECORD"):
                assert MyExampleClass.my_method is not orig_method
                orig_output = MyExampleClass(2).my_method(5)
                orig_output_2 = my_obj.my_method(6)

        assert MyExampleClass.my_method is orig_method

        # The lists are equal
        with recording_switch(
            freeze_call_times((MyExampleClass, "my_method"), MyExampleClass.my_method),
            rec_mode="PLAYBACK",
        ):
            played_back_output = MyExampleClass(2).my_method(5)
            played_back_output_2 = my_obj.my_method(6)
            assert orig_output == played_back_output
            assert orig_output_2 == played_back_output_2

        assert MyExampleClass.my_method is orig_method

    .. testcleanup:: freeze

        temp_dir.__exit__(None, None, None)


    """

    compare_stacks: bool = False
    """
    Whether to enforce the same call stacks at record and play-back times --
    this increases the recording file size but enforces correctness and aids debugging
    """

    def build_recorded(self):
        out = FreezeCallTimesRecorder(
            self.targets, self.fxn, compare_stacks=self.compare_stacks
        )
        return out, [out]

    def build_played_back(self, recordings: List["CallTimeRecord"]):
        calls = checked_get_single(recordings)
        player = FreezeCallTimesPlayer(
            self.targets, self.fxn, compare_stacks=self.compare_stacks, calls=calls
        )
        return player, [player]


@dataclass
class _MonkeyPatching(_WrapperContextManager, ABC):
    targets: TargetsType
    """See ``freeze_call_times`` for descriptions of attributes."""
    fxn: Callable
    calls: List["CallTimeRecord"] = field(default_factory=list)
    lock: Lock = field(init=False, default_factory=Lock)
    _original_callables = None

    def wrapper_enter(self):
        with self.lock:
            wrapped_callable = wraps(self.fxn)(
                lambda *args, **kwargs: self(*args, **kwargs)
            )
            self._original_callables = [
                getattr(_obj, _attr_name) for _obj, _attr_name in self.targets
            ]
            [
                setattr(_obj, _attr_name, wrapped_callable)
                for _obj, _attr_name in self.targets
            ]
            return self

    def wrapper_exit(self, *args, **kwargs):
        with self.lock:
            if self._original_callables is not None:
                [
                    setattr(_obj, _attr_name, _value)
                    for ((_obj, _attr_name), _value) in zip(
                        self.targets, self._original_callables
                    )
                ]
            self._original_callables = None

    @contextmanager
    def with_vanilla_getattribute(self):
        # Not really part of _MonkeyPatching, but required to support
        # _WrapperGetattribute interface.
        yield self

    @abstractmethod
    def __call__(self, *args, **kwargs): ...


@serializable(signature="rs:call_time")
@dataclass
class CallTimeRecord:
    time: np.datetime64
    stack: Optional[List[str]] = None


@dataclass
class FreezeCallTimesRecorder(_MonkeyPatching, Serializable):
    compare_stacks: bool = False
    signature = "rs:freeze_call_times_recorder"

    def __post_init__(self):
        self.calls = []

    def __call__(self, *args, **kwargs):
        with _freeze_time(pytz.UTC.localize((frozen_time := utc_now()).item())):
            out = self.fxn(*args, **kwargs)
            self.calls.append(
                CallTimeRecord(
                    frozen_time,
                    None if not self.compare_stacks else format_stack()[:-1],
                )
            )
            return out

    def as_serializable(self):
        return {"calls": self.calls}

    @classmethod
    def from_serializable(cls, calls) -> List[CallTimeRecord]:
        return calls


@dataclass
class FreezeCallTimesPlayer(_MonkeyPatching):
    compare_stacks: bool = False

    def __call__(self, *args, **kwargs):
        next_call_record = self.calls.pop(0)
        if next_call_record.stack and (rec_stack := Stack(next_call_record.stack)) != (
            pb_stack := Stack(format_stack()[:-1])
        ):
            raise Exception(
                f"{colored_text('Recorded', 'yellow')} and {colored_text('played-back', 'red')} "
                f"stacks do not match:\n{rec_stack.comparison_string(pb_stack)}"
            )

        with self.lock:
            GLOBAL_TIME.move_to(next_call_record.time)
            out = self.fxn(*args, **kwargs)
            return out
