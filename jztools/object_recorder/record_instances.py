from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from typing import Callable, Type, Union
from jztools.object_recorder import recording_switch


@dataclass
class _NewOverload:
    rec_switch: recording_switch
    source_cls: Type
    orig_new: Callable
    orig_init: Callable

    def __call__(self, cls, *args, __object_recorder__skip_overload__=False, **kwargs):
        if __object_recorder__skip_overload__ or cls is not self.source_cls:
            # Has the same effect as the non-patched class's __new__ and
            # ensures that the __new__ overload is not inherited.
            out = (
                self.orig_new(cls)
                if self.orig_new is object.__new__
                else self.orig_new(cls, *args, **kwargs)
            )
            return out
        else:
            # Records the class
            kwargs["__object_recorder__skip_overload__"] = True
            return self.rec_switch.extend_enter((cls, args, kwargs))


@dataclass
class _InitOverload:
    source_cls: Type
    orig_init: Callable

    def __call__(self, obj, *args, **kwargs):
        if (
            # The call is from a derived class (the init overload is not inherited)
            type(obj) is not self.source_cls
            # The call follows a call to a _NewOverload (ensures a single call to __init__)
            or kwargs.pop("__object_recorder__skip_overload__", False)
        ):
            return self.orig_init(obj, *args, **kwargs)


@contextmanager
def record_instances(switch: recording_switch, *classes: Type):
    """
    Overloads the specified classes' __new__ method so that the object uses its recording factory to create an instance.

    When this context is used,  instantions of the class will be recorded,played-back or original objects, depending on the
    current recording mode.
    """
    with ExitStack() as stack:
        [stack.enter_context(_record_instances(switch, cls)) for cls in classes]
        yield


@contextmanager
def _record_instances(switch: recording_switch, cls: Type):
    #
    if isinstance(vars(cls).get("__new__", None), _NewOverload):
        # The class is already being recorded, no-op.
        yield
        return

    orig_new = cls.__new__
    orig_init = cls.__init__

    try:
        cls.__new__ = _NewOverload(switch, cls, orig_new, orig_init)
        cls.__init__ = lambda *args, _init_overload=_InitOverload(
            cls, orig_init
        ), **kwargs: _init_overload(*args, **kwargs)

        yield

    finally:
        # If __new__ is not overloaded (i.e., it is object.__new__), we need to ensure that the
        # default object.__new__ ignores other initialization arguments. By default these extra
        # arguments are removed by a Python mechanism that is removed when __new__ is assigned.
        # delattr(cls, '__new__') (cf., orig_init below) will not work as it does not reinstate
        # that mechanism, and hence we have to implement the mechanism ourselves.
        cls.__new__ = (
            (lambda cls, *args, **kwargs: orig_new(cls))
            if orig_new is object.__new__
            else orig_new
        )

        #
        if orig_init is object.__init__:
            delattr(cls, "__init__")
        else:
            cls.__init__ = orig_init
