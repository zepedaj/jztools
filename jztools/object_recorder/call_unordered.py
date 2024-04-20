from dataclasses import dataclass, field
from itertools import groupby
from threading import RLock
from typing import Any, Callable, Dict, Hashable, List, Tuple
from jztools.object_recorder.rec_obj_factory import RecObjFactory
from jztools.validation import checked_get_single

from xerializer.abstract_type_serializer import Serializable
from .freeze_call_times.freeze_call_times import (
    _CallablePatcherFactory,
    _MonkeyPatching,
    utc_now,
)
from jztools.object_recorder.object_recorder import RecordedCall, PlayedBackCall


class NoCallEntryForArgs(Exception):
    pass


def general_arg_hasher(args, kwargs):
    """Returns a tuple of args and a tuple of sorted kwarg items."""
    return (args, tuple(sorted(kwargs.items())))


def method_arg_hasher(args, kwargs):
    """Drops the first argument, which is expected to correspond to self or cls."""
    return general_arg_hasher(args[1:], kwargs)


REGISTERED_HASHERS = {"general": general_arg_hasher, "method": method_arg_hasher}


@dataclass
class _WithArgHasher:
    arg_hasher: Callable[[Tuple, Dict[str, Any]], Hashable] = general_arg_hasher


@dataclass
class call_unordered(_WithArgHasher, _CallablePatcherFactory, RecObjFactory):
    def __post_init__(self):
        if isinstance(self.arg_hasher, str):
            self.arg_hasher = REGISTERED_HASHERS[self.arg_hasher]
        super().__post_init__()

    def build_recorded(self):
        return (
            out := UnorderedCallRecorder(
                self.targets, self.fxn, arg_hasher=self.arg_hasher
            )
        ), [out]

    def build_played_back(self, recordings: List["RecordedCall"]):
        return (
            out := UnorderedCallPlayer(
                self.targets,
                self.fxn,
                checked_get_single(recordings),
                arg_hasher=self.arg_hasher,
            )
        ), [out]


@dataclass
class UnorderedCallRecorder(_WithArgHasher, _MonkeyPatching, Serializable):
    signature = "rs:unordered_calls"
    calls: List[RecordedCall] = field(default_factory=list)

    def __call__(self, *args, **kwargs):
        call_time = utc_now()
        # No locking should happen here, as it would prevent multi-threading benefits.
        response = self.fxn(*args, **kwargs)
        self.calls.append(
            RecordedCall(args, kwargs, value=response, access_time=call_time)
        )
        return response

    def as_serializable(self):
        return {"calls": self.calls}

    @classmethod
    def from_serializable(cls, calls):
        return calls


@dataclass
class UnorderedCallPlayer(_WithArgHasher, _MonkeyPatching):
    calls: Dict[Hashable, List[PlayedBackCall]]
    lock: RLock = field(init=False, default_factory=RLock)

    def __post_init__(self):
        if not isinstance(self.calls, dict):
            calls = self.calls
            self.calls = {
                key: list(group)
                for key, group in groupby(
                    calls, lambda x: self.arg_hasher(x.args, x.kwargs)
                )
            }

    def __call__(self, *args, **kwargs):
        key = self.arg_hasher(args, kwargs)

        with self.lock:
            try:
                group = self.calls[key]
            except KeyError:
                raise NoCallEntryForArgs(
                    f"Could not find a {self.fxn} call entry for arguments args={args}, kwargs={kwargs}."
                )
            out = group.pop()
            if not group:
                self.calls.pop(key)
            return out.value
