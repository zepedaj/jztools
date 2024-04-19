from importlib import import_module
from ast import literal_eval
import re
from contextlib import contextmanager
from pathlib import PurePath
from io import IOBase
from inspect import isclass, ismodule
from typing import Dict, List, Any, Union, Callable
import traceback
import inspect
import tempfile
from importlib import import_module as _import_module

# from py.path import local  # TODO - pytest session still returns this type of paths


def _raise(ex):
    raise ex


def not_implemented_property(name):
    return property(lambda self: _raise(NotImplementedError(name)))


def get_caller(offset=1):
    fullstack = inspect.stack()
    stack = fullstack[offset]
    frame = stack.frame
    while True:
        if "self" in frame.f_locals:
            return getattr(frame.f_locals["self"], stack.function)
        else:
            # if stack.function in frame.f_locals:
            return frame.f_back.f_locals.get(
                stack.function, frame.f_back.f_globals.get(stack.function)
            )


def dict_prune(x: Dict, prune_val=None):
    """Prunes entries from a dictionary with the specified value (``None`` by default)."""
    return {key: val for key, val in x.items() if val != prune_val}


def get_caller_name(offset=1, stack_entry=None):
    """
    Returns the fully qualified name of the calling method or function.

    if ``stack_entry`` is provided, ``offset`` is ignored.
    """
    stack_entry = stack_entry or inspect.stack()[offset]
    frame = stack_entry.frame
    if "self" in frame.f_locals:
        cls = frame.f_locals["self"].__class__
        name = cls.__module__ + "." + cls.__qualname__ + "." + stack_entry.function
    else:
        module = inspect.getmodule(stack_entry.frame)
        module_name = "__main__" if module is None else module.__name__
        name = module_name + "." + stack_entry.function
    return name


def setattrs(obj, **kwargs):
    """
    Sets the attributes specified as keywords. The returned object is the same as the input object, with attributes modified.

    Example:

    .. code-block::

        obj = Rectangle()
        attr_update(obj, width=10, height=20)

    """

    for key, val in kwargs.items():
        setattr(obj, key, val)
    return obj


class LazyObject:
    def __init__(self, object_constructor):
        self._constructor = object_constructor

    @property
    def obj(self):
        if hasattr(self, "_obj"):
            return self._obj
        else:
            self._obj = self._constructor()
            return self.obj


# def strict_zip(*args, sentinel=None):
#     for items in zip_longest(*args, fillvalue=sentinel):
#         if sentinel in items:
#             raise Exception(
#                 'Strict zip requires input iterators of equal length!')
#         yield items


class StrictZipException(Exception):
    def __init__(self):
        super().__init__("Lengths of iterators do not match!")


def strict_zip(*args):
    """
    Raises an exception if the input iterators do not have the same length.
    """
    if not len(args):
        return
    arg_iters = list(map(iter, args))
    while True:
        out = []
        for k in range(len(arg_iters)):
            try:
                out.append(next(arg_iters[k]))
            except StopIteration:
                if k != 0:
                    raise StrictZipException()
                else:
                    for l in range(1, len(arg_iters)):
                        try:
                            next(arg_iters[l])
                            raise StrictZipException()
                        except StopIteration:
                            pass
                return

        yield tuple(out)


# Importing from strings
def import_from_string(path):
    """
    Example:
    ClassA = import_from_string('module.submodule.ClassA')
    """
    module = _import_module(path.rsplit(".", 1)[0])
    return vars(module)[path.rsplit(".", 1)[1]]


def obj_from_string(class_path, *args, **kwargs):
    """
    Given a module and class path of the form ``module.submodule.ClassName``, returns an instance of that object with `*args` and `**kwargs` used to instantiate it.
    """
    cls = import_from_string(class_path)
    return cls(*args, **kwargs)


# # Threads
# class StoppableThread(threading.Thread):
#     """
#     Joined timed-out threading.Thread threads continue to execute. This class adds
#     a stop method that enables stopping these threads.

#     Usage:
#     t1 = StoppableThread(target=fxn)
#     t1.start()
#     t1.timeout(timeout_in_secs)
#     """

#     def get_id(self):
#         if hasattr(self, '_thread_id'):
#             out_id = self._thread_id
#         for id, thread in threading._active.items():
#             if thread is self:
#                 out_id = id
#         #
#         return ctypes.c_ulong(out_id)

#     def timeout(self, secs):
#         self.join(secs)
#         if self.is_alive():
#             self.stop()
#             self.join()

#     def stop(self):
#         thread_id = self.get_id()
#         res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
#             thread_id, ctypes.py_object(SystemExit))
#         if res > 1:
#             ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
#         if res != 1:
#             raise Exception('Could not stop thread.')


def strict_chunks(L, N):
    """
    Returns tuples of N items from L in each iteration. If some chunks remain, raises an exception.
    """
    return strict_zip(*([iter(L)] * N))


def exception_string(exc: Exception):
    """
    Returns the full string produced when the exception is raised.
    """
    try:
        raise exc
    except:
        tb = traceback.format_exc()
    return tb


def get_nested_keys(d: Union[Dict[str, Any], List], criterion: Callable):
    """
    Traverses the input container hierarchy tree, and returns the sequence of keys with values that satisfy the specified criterion.

    Traveral is halted for a given branch if the criterion returns True.
    """

    #
    all_leafs = []

    # Get iterator.
    if isinstance(d, dict):
        key_val_iter = d.items()
    elif isinstance(d, (list, tuple)):
        key_val_iter = enumerate(d)
    else:
        raise ValueError(f"Dict, list or tuple expected, but got {type(d)}.")

    # Iterate depth-first.
    for parent_key, val in key_val_iter:
        if criterion(val):
            all_leafs.append([parent_key])
        elif isinstance(val, (dict, list, tuple)):
            all_leafs.extend(
                [
                    [parent_key] + children_keys
                    for children_keys in get_nested_keys(d[parent_key], criterion)
                ]
            )

    return all_leafs


def entity_name(in_entity, stripped_modules=None):
    """
    Works for a module or any object with a __qualname__ (e.g., a class), but inversion will fail for 1) bound instance methods, 2) object instances and local variables.

    :param in_class: The class to represent as a string.
    :param stripped_modules: A list of modules objects to strip in the string representation. If module X is in the list, X.MyClass is represented as 'MyClass' instead of 'X.MyClass'. Currently defaults to an empty list. In the future, it will contain  module 'builtins' by default.
    """

    # In [100]: for entity in [fxn, A.static_method, A.class_method, A.instance_method, A().instance_method, lambda x: x]: print(entity, '\n', {'__self__': getattr(entity, '__self__', '--NA--'), **{key:getattr(inspect, key)(entity) for key in ['isfunction', 'ismethod']}})
    # <function fxn at 0x7facc93193a0>
    #  {'__self__': '--NA--', 'isfunction': True, 'ismethod': False}
    # <function A.static_method at 0x7facc9319040>
    #  {'__self__': '--NA--', 'isfunction': True, 'ismethod': False}
    # <bound method A.class_method of <class '__main__.A'>>
    #  {'__self__': <class '__main__.A'>, 'isfunction': False, 'ismethod': True}
    # <function A.instance_method at 0x7faccaeb40d0>
    #  {'__self__': '--NA--', 'isfunction': True, 'ismethod': False}
    # <bound method A.instance_method of <__main__.A object at 0x7faba88fc370>>
    #  {'__self__': <__main__.A object at 0x7faba88fc370>, 'isfunction': False, 'ismethod': True}
    # <function <lambda> at 0x7facc9322430>
    #  {'__self__': '--NA--', 'isfunction': True, 'ismethod': False}

    if ismodule(in_entity):
        return in_entity.__name__
    elif (
        not hasattr(in_entity, "__qualname__")
        or not hasattr(in_entity, "__module__")
        or (hasattr(in_entity, "__self__") and not inspect.isclass(in_entity.__self__))
    ):
        raise Exception(
            f"Cannot extract an entity name from {in_entity}. Note that bound instance methods and objects are not supported."
        )

    #
    return f"{in_entity.__module__}:{in_entity.__qualname__}"


def entity_from_name(in_name):
    if not (
        components := re.match("^(?P<module>[\w\.]+)(:(?P<entity>[\.\w]+))?$", in_name)
    ):
        raise Exception(f"Invalid entity name {in_name}.")
    else:
        components = components.groupdict()
        module = import_module(components["module"])
        if entity := components["entity"]:
            parts = entity.split(".")
            out = module
            for part in parts:
                out = getattr(out, part)
            return out
        elif entity is None:
            return module

    raise Exception("Unexpected case.")


def parent_entity(in_entity):
    child_name = entity_name(in_entity)
    parent_name = "".join(re.split(r"([\.\:])", child_name)[:-2])
    if not parent_name:
        raise NotImplementedError(f"Cannot extract the parent entity for {child_name}.")
    return entity_from_name(parent_name)


def class_name(in_class, stripped_modules=None):
    """
    :param in_class: The class to represent as a string.
    :param stripped_modules: A list of modules objects to strip in the string representation. If module X is in the list, X.MyClass is represented as 'MyClass' instead of 'X.MyClass'. Currently defaults to an empty list. In the future, it will contain  module 'builtins' by default.
    """
    # Function change warning.
    stripped_modules = stripped_modules or []

    #
    if not isclass(in_class):
        raise Exception(f"Expected a class but received {in_class}.")

    #
    if in_class.__module__ in [_mdl.__name__ for _mdl in stripped_modules]:
        out = f"{in_class.__name__}"
    else:
        out = f"{in_class.__module__}.{in_class.__name__}"

    return out


def class_from_name(in_class_name, stripped_modules=tuple()):
    """
    :param in_class_name: The representation produced by :func:`class_name`
    :param stripped_modules: The same value passed to :func:`class_name` when building :attr:`in_class_name`.
    """
    module, class_name = ([None] + in_class_name.rsplit(".", 1))[-2:]
    if module is None:
        for _mdl in stripped_modules:
            mdl_vars = vars(_mdl)
            try:
                out = mdl_vars[class_name]
            except KeyError:
                continue
            else:
                return out
        raise Exception(
            f"Could not find class {class_name} in the specified modules {stripped_modules}."
        )
    else:
        module = import_module(module)
        return vars(module)[class_name]


@contextmanager
def filelike_open(filelike: Union[str, IOBase, PurePath], *args, **kwargs):
    """
    Takes 1) a file path specification and opens with the specified arguments, or 2) an open IO object (e.g., a file pointer) and returns that.
    """
    if isinstance(filelike, IOBase):
        yield filelike
    elif isinstance(filelike, (str, IOBase, PurePath, local)):
        with open(filelike, *args, **kwargs) as fp:
            yield fp
    elif isinstance(filelike, tempfile._TemporaryFileWrapper):
        yield filelike.file
    else:
        raise TypeError(f"Invalid type {type(filelike)} for filelike.")


def cutoff_str(in_str: Any, length: int = 2000, suffix: str = " ..."):
    """
    Converts ``in_str`` to string and cuts off + appends suffix if too long.
    """
    # Keywords: Truncate, trim, max length

    in_str = str(in_str)
    length = max(length, len(suffix))
    length -= len(suffix)
    return in_str[:length] + (in_str[length:] and suffix)


def display(val: Any):
    """
    Produces a string representation of the input. For string, in particular, the string representation includes single quotes.
    """
    return str([val])[1:-1]


from pygments import highlight
from pygments.lexers import get_lexer_by_name
from pygments.formatters import TerminalFormatter


class ReadableMultiline:
    """
    Encoder/decoder that encodes a multi-line string as a single-line string, attempting to maintain readability.
    """

    @classmethod
    def encode(cls, in_str):
        return str(in_str.encode())

    @classmethod
    def decode(cls, encoded_str):
        return literal_eval(encoded_str).decode()


class ErrorEncoder(ReadableMultiline):
    """
    Encodes/decodes exceptions as traceback strings. Decoding can optionally (and by default) colorize the string for terminal display.
    """

    _terminal_formatter = TerminalFormatter()
    _lexer = get_lexer_by_name("py3tb")

    @classmethod
    def encode(cls, in_err: Exception):
        return super().encode(exception_string(in_err))

    @classmethod
    def decode(cls, in_err_str: str, colorize=True):
        tbtext = super().decode(in_err_str)
        if colorize:
            tbtext = highlight(tbtext, cls._lexer, cls._terminal_formatter)
        return tbtext


def setdefaultattr(obj, name, value):
    try:
        return getattr(obj, name)
    except AttributeError:
        setattr(obj, name, value)
    return value
