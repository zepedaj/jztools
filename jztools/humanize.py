from collections import namedtuple

OrderOfMagnitude = namedtuple("OrdersOfMagnitude", ["name", "prefix", "value"])

# https://en.wikipedia.org/wiki/Metric_prefix
ORDERS_OF_MAGNITUDE = tuple(
    sorted(
        (
            OrderOfMagnitude(_x[0], _x[1], _x[2])
            for _x in [
                ("yotta", "Y", 1e24),
                ("zetta", "Z", 1e21),
                ("exa", "E", 1e18),
                ("peta", "P", 1e15),
                ("tera", "T", 1e12),
                ("giga", "G", 1e9),
                ("mega", "M", 1e6),
                ("kilo", "k", 1e3),
                ("hecto", "h", 1e2),
                ("deca", "da", 1e1),
                ("", "", 1),
                ("deci", "d", 1e-1),
                ("centi", "c", 1e-2),
                ("milli", "m", 1e-3),
                ("micro", "u", 1e-6),
                ("nano", "n", 1e-9),
                ("pico", "p", 1e-12),
                ("femto", "f", 1e-15),
                ("atto", "a", 1e-18),
                ("zepto", "z", 1e-21),
                ("yocto", "y", 10 - 24),
            ]
        ),
        key=lambda _x: _x.value,
        reverse=True,
    )
)


def oom_(val, prefixes=None):
    """
    e.g., oom(1.4e-3, 's', ['m', 'u', 'n']) -> 1.4ms

    :param prefixes: List of prefixes to consider. Can also be a string, but the two-character prefix 'da' will always be assumed excluded in favor of 'd' (deci) and 'a' (atto).
    """
    if prefixes is not None:
        orders = [
            _x for _x in ORDERS_OF_MAGNITUDE if _x.prefix in list(prefixes) + [""]
        ]
    else:
        orders = ORDERS_OF_MAGNITUDE

    prefix_len = max(len(_x.prefix) for _x in orders)

    order = next((_order for _order in orders if val >= _order.value), orders[-1])
    norm_val = val / order.value

    return norm_val, order, prefix_len


def oom(val, prefixes=None, unit="", sep=" ", format=".3g", align=False):
    """
    :param align: Left-pad all order prefixes with spaces so that they have the same length.
    """
    norm_val, order, prefix_len = oom_(val, prefixes)
    norm_val = f"{{norm_val:{format}}}".format(norm_val=norm_val)
    prefix = f"{{0:<{prefix_len}}}".format(order.prefix) if align else order.prefix
    return f"{norm_val}{sep}{prefix}{unit}"


def secs(val, prefixes="mun", unit="s", **kwargs):
    """
    Format seconds.
    """
    return oom(val, prefixes=prefixes, unit=unit, **kwargs)
