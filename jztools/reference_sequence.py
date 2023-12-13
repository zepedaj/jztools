from dataclasses import dataclass
from itertools import chain
from typing import Any, Dict, List, Tuple
from copy import deepcopy
from contextlib import contextmanager


def isinstance_(value, types):
    """Use this instead of ``isinstance`` to check objects that might be :class:`RefSeq` for any type"""
    return issubclass(type(value), types)


def base_get(ref_seq: "RefSeq", attr):
    return object.__getattribute__(ref_seq, attr)


def base_set(ref_seq: "RefSeq", attr, value):
    return object.__setattr__(ref_seq, attr, value)


@contextmanager
def vanilla_get_set(*ref_seqs: "RefSeq"):
    vanilla_flags_0 = [base_get(x, "__ref_seq_vanilla__") for x in ref_seqs]
    try:
        [base_set(x, "__ref_seq_vanilla__", True) for x in ref_seqs]
        yield
    finally:
        [
            base_set(x, "__ref_seq_vanilla__", f)
            for x, f in zip(ref_seqs, vanilla_flags_0)
        ]


def getter(ref_seq: "RefSeq", target: Any):
    out = target
    for ref in base_get(ref_seq, "__ref_seq__"):
        out = ref.getter(out)
    return out


def setter(ref_seq: "RefSeq", obj: Any, target):
    if not (seq := base_get(ref_seq, "__ref_seq__")):
        raise Exception("Setting an object on an empty ref seq is not supported.")
    for ref in seq[:-1]:
        obj = ref.getter(obj)
    seq[-1].setter(obj, target)


@dataclass
class Attr:
    name: str

    def getter(self, target):
        return getattr(target, self.name)

    def setter(self, obj, value):
        setattr(obj, self.name, value)

    def __str__(self):
        return "." + self.name


@dataclass
class Item:
    entry: Any

    def getter(self, target):
        return target[self.entry]

    def setter(self, obj, value):
        obj[self.entry] = value

    def __str__(self):
        return f"[{self.entry}]"


@dataclass
class Call:
    args: Tuple
    kwargs: Dict[str, Any]

    def getter(self, target):
        return target(*self.args, **self.kwargs)

    def setter(self, *args):
        raise SyntaxError("Cannot assign to function call.")

    def __str__(self):
        kwarg_strs = (f"{name}={value}" for name, value in self.kwargs.items())
        return "(" + ", ".join(chain(map(str, self.args), kwarg_strs)) + ")"


class RefSeq:
    """
    Transferable reference sequence.
    """

    __ref_seq_vanilla__ = False
    """ Whether to behave normally or as a reference sequence. """

    __ref_seq__: List
    """ The sequence of referenences """

    __ref_seq_protected__ = tuple()
    """ Special methods that will be handled using the standard getter"""

    # These attributes will be called from the type to avoid clashes.
    Attr = Attr
    Item = Item
    Call = Call

    def __init__(self):
        self.__ref_seq__ = []

    def __getitem__(self, key: Any):
        if base_get(self, "__ref_seq_vanilla__"):
            return super().__getitem__(key)
        else:
            out = type(self).copy(self)
            base_get(out, "__ref_seq__").append(type(self).Item(key))
            return out

    @classmethod
    def copy(cls, ref_seq: "RefSeq", out=None):
        # Methods will be called from the type to avoid clashes.
        out = cls() if out is None else out
        with vanilla_get_set(ref_seq, out):
            out.__ref_seq__ = list(ref_seq.__ref_seq__)
            out.__ref_seq_vanilla__ = ref_seq.__ref_seq_vanilla__
        return out

    def __getattribute__(self, attr: str):
        """
        When attr is not a member of :class:`RefSeq`, behaves like an alias of :meth:`a_`. Otherwise, returns the ``self`` attribute of name ``attr``. This can cause name conflicts which can be avoided by using :meth:`a_`.
        """
        if base_get(self, "__ref_seq_vanilla__") or attr in base_get(
            self, "__ref_seq_protected__"
        ):
            return super().__getattribute__(attr)
        else:
            out = type(self).copy(self)
            base_get(out, "__ref_seq__").append(type(self).Attr(attr))
            return out

    def __call__(self, *args, **kwargs):
        if base_get(self, "__ref_seq_vanilla__"):
            return super()(*args, **kwargs)
        else:
            out = type(self).copy(self)
            base_get(out, "__ref_seq__").append(type(self).Call(args, kwargs))
            return out

    def __str__(self):
        return (
            f"<{type(self).__name__}"
            f"{''.join(str(x) for x in base_get(self, '__ref_seq__'))})>"
        )
