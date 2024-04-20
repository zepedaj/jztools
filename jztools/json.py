from jztools.files import ThreadSafeWriter
import warnings
import json


class ThreadSafeJsonWriter(ThreadSafeWriter):
    def write(self, obj):
        super().write(json.dumps(obj))
        super().write("\n")


def as_json_serializable(val):
    """
    Makes sure all entries are json-serializable. Entries that cannot be serialized are converted to their string representation.
    """
    base_types = (int, float, str)
    if isinstance(val, (dict, list, tuple)):
        # Make a copy of the iterable
        out = (ref_type if (ref_type := type(val)) is not tuple else list)(val)
        # Make sure entries are serializable
        for key, val in val.items() if issubclass(ref_type, dict) else enumerate(val):
            if not isinstance(val, base_types):
                out[key] = as_json_serializable(val)
    elif isinstance(val, (float, int, str)):
        out = val
    else:
        out = str(val)

    return out


def as_serializable(val):
    # TODO: DEPRECATE
    warnings.warn("Deprecated, use 'as_json_serializable' instead.", DeprecationWarning)
    return as_json_serializable(val)
