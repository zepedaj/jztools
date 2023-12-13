from collections import Counter
import functools
import inspect
import re
from typing import Any, Callable, Union
from jztools.py import display, cutoff_str


class InvalidOptionValue(Exception):
    pass


class OptionNotConfirmed(Exception):
    pass


class NonSingle(Exception):
    pass


class NonUnique(Exception):
    pass


class NoItem:
    pass


def checked_get_single(
    container,
    *posns,
    msg: Union[
        str, Callable[[], str]
    ] = "Expected single entry but found {count} for keys {posns}.",
    raise_empty=True,
) -> Any:
    """
    Checks if a (nested) container contains a single entry with the specified index (or indices) and returns it.

    # Success
    checked_get_single((x for x in [0])) -> 0
    checked_get_single(['first']) -> 'first'
    checked_get_single(['f'], 0, 0) -> 'f'
    checked_get_single({'sentence':{'words':['My']}}, 'sentence', 'words', 0) -> 'My'

    # Error
    checked_get_single((x for x in [0, 1]))
    checked_get_single(['first', 'second'])
    checked_get_single(['fa'], 0, 0)
    checked_get_single({'sentence':{'words':['My'], 'phrases':[None]}}, 'sentence', 'words', 0) -> 'My'
    """

    def msg_as_str() -> str:
        return msg if isinstance(msg, str) else msg()

    if not posns:
        k = -1
        for k, out in enumerate(container):
            if k > 0:
                break
        if k == -1 and not raise_empty:
            return NoItem
        elif k != 0:
            raise NonSingle(
                msg_as_str().format(count="None" if k == -1 else ">1", posns=posns)
            )
        container = out
    else:
        for _posn in posns:
            if len(container) == 0 and not raise_empty:
                return NoItem
            elif len(container) > 1:
                raise NonSingle(msg_as_str().format(count=len(container), posns=posns))
            container = container[_posn]
    return container


def not_none(x) -> Any:
    if x is None:
        raise ValueError("`None` not expected")
    return x


def checked_get_unique(container):
    """
    Ensures that the container contains a unique element, possibly repeated multiple times, and returns that element.
    """
    out = set(container)
    if len(out) > 1:
        raise NonUnique(
            cutoff_str(
                f"Expected a single unique (possibly repeated) element in the container, but found {len(out)} unique elements: {out}"
            )
        )
    return list(out)[0]


def checked_filter_single(
    predicate, container, msg="Expected a single matching entry but found {count}."
):
    """
    Similar to filter, but ensures that a single item in the input container satisfies the predicate and returns that item. If a none or more than one found, raises an exception.
    """
    out = list(x for x in container if predicate(x))
    if len(out) != 1:
        raise Exception(msg.format(count=len(out)))
    return out[0]


def check_option(name, value, options, ignore_list=[]):
    """
    Checks that an option has a valid value, raising :class:`InvalidOptionValue` otherwise.

    :param name: The name of the option to check.
    :param value: The value of the option.
    :param options: List of valid option values.
    :param ignore_list: Do not raise an error if value is in this list.

    .. rubric:: Example

    .. code-block::

        def winner(medal):
            check_option('medal', medal, [1, 2, 3], ignore_list=[-1])
            ...

    """
    if value not in options and value not in ignore_list:
        raise InvalidOptionValue(
            f"Invalid option value {name}={value}. Use one of {options}."
        )
    return value


def check_expected_kwargs(expected, received, name="arguments", missing_ok=False):
    expected = set(expected)
    received = set(received)
    if expected != received:
        extra_fields = received - expected
        missing_fields = expected - received

        if extra_fields and missing_fields and not missing_ok:
            raise Exception(
                f"Missing and invalid {name}! "
                f"Missing: {missing_fields}. Invalid: {extra_fields}."
            )
        elif extra_fields:
            raise Exception(f"Invalid {name} {extra_fields}.")
        elif missing_fields and not missing_ok:
            raise Exception(f"Missing {name} {extra_fields}.")


def confirm_option(
    name,
    received,
    dangerous_values,
    message="\nPlease confirm choice {name}={received} (y/n): ",
):
    """
    If the received value is in the list of dangerous values, user console input will be requested.
    The received value is returned if confirmed by the user, and an :exc:`OptionNotConfirmed` error
    raised otherwise.
    """
    if received in dangerous_values:
        while (out := input(message.format(**locals()))) not in "yn":
            pass
        if out == "n":
            raise OptionNotConfirmed(f"Choice {name}={received} not confirmed.")

    return received


def check_unique(
    values, msg="The following entries occurred more than once {non_unique_flds}."
):
    counts = Counter(values)
    if any(x > 1 for x in counts.values()):
        non_unique_flds = ", ".join(
            (f"{_key}({_val})" for _key, _val in counts.items() if _val > 1)
        )
        raise Exception(msg.format(non_unique_flds=non_unique_flds))


class ParameterChoiceError(ValueError):
    def __init__(self, name, err_value, values):
        super().__init__(f"Parameter {name}={err_value} needs to be one of {values}.")


# Parameter validation.
def choices(name, values, multi=False, doc=True):
    """
    Will only check the value if it is provided explicitly by the user - default values are not checked.

    :param name: The parameter name.
    :param values: The valid choices as an iterable.
    :param multi: If ``True``, ``lists``, ``tuples`` or ``sets`` of the allowed values also allowed.
    :param doc: If ``True``, the choices are appended afer the ``:param <param name>:`` string (if any) in the doc string.
    """

    values = list(values)

    def wrapper(fxn):
        if doc and fxn.__doc__:
            fxn.__doc__ = re.sub(
                f"(:\\s*param\\s+){name}(\\s*:)",
                f"\\1{name}\\2 ``{values}{' (or combinations)' if multi else ''}``",
                fxn.__doc__,
            )

        @functools.wraps(fxn)
        def check_and_call(*args, **kwargs):
            signature = inspect.signature(fxn)
            params = signature.bind(*args, **kwargs)
            if name in params.arguments and not (
                (err_value := (in_value := params.arguments[name])) in values
                or (
                    multi
                    and isinstance(in_value, (tuple, list, set))
                    and not (
                        err_value := list(filter(lambda _v: _v not in values, in_value))
                    )
                )
            ):
                raise ParameterChoiceError(name, err_value, values)

            return fxn(*args, **kwargs)

        return check_and_call

    return wrapper
