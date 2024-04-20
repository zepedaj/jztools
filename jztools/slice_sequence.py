"""

.. warning::

  This module is deprecated in favor of ``jztools.reference_sequence``!!

Serializable sequential slicing. 

Examples:

.. testcode:: group

    from jztools.slice_sequence import SliceSequence, SSQ_

Both :class:`SliceSequence` and :class:`SSQ_` are aliases of the same class and can be used interchangeably.

.. testcode:: group

    # Using slice sequences
    arr = {'field1':list(range(20))}

    orig = arr['field1'][1:10][0]
    with_syntax1 = SSQ_()['field1'][1:10][0](arr)
    with_syntax2 = SSQ_(['field1', slice(1,10), 0])(arr)

    assert with_syntax1 == orig
    assert with_syntax1 == with_syntax2

Slice sequence objects can be serialized in a human-readable format:

.. testcode:: group

    from xerializer import Serializer

    # Serialization:
    srlzr = Serializer()
    ssq = SSQ_()['field1'][1:10][0]
    json_string = srlzr.serialize(ssq)

    print(json_string)

    deserialized_ssq = srlzr.deserialize(json_string)
    assert deserialized_ssq(arr) == ssq(arr)

.. testoutput:: group

    {"__type__": "SliceSequence", "value": ["field1", {"__type__": "slice", "start": 1, "stop": 10}, 0]}


"""

import warnings
from copy import deepcopy
from xerializer.abstract_type_serializer import Serializable as _Serializable
from jztools.py import class_name, cutoff_str


class SliceSequence(_Serializable):
    signature = "SliceSequence"

    def __init__(self, value=None):
        self.slice_sequence = value or []

    def __str__(self):
        return f"{class_name(type(self))}<{self.slice_sequence}>"

    @classmethod
    def produce(cls, val):
        if val is None:
            out = object.__new__(cls)
            out.slice_sequence = []
        elif isinstance(val, (list, tuple)):
            out = object.__new__(cls)
            out.slice_sequence = list(val)
        elif isinstance(val, cls):
            out = val
        else:
            out = object.__new__(cls)
            out.slice_sequence = [val]
        return out

    def __eq__(self, ssq):
        return self.slice_sequence == ssq.slice_sequence

    def copy(self):
        out = object.__new__(type(self))
        out.slice_sequence = deepcopy(self.slice_sequence)
        return out

    def __getitem__(self, slice_args):
        out = self.copy()
        out.slice_sequence.append(slice_args)
        return out

    def __call__(self, obj):

        orig_obj = obj
        for k, slice_obj in enumerate(self.slice_sequence):
            try:
                obj = obj[slice_obj]
            except Exception:
                raise Exception(
                    f"{self} retrieval failed when applying {k}-nested entry '{slice_obj}' to {cutoff_str(str(orig_obj)+'.')}\nSee above error."
                )
        return obj

    def set(self, obj, val):
        slice_sequence = (
            self.slice_sequence if len(self.slice_sequence) > 1 else [slice(None)]
        )
        for slice_obj in slice_sequence[:-1]:
            obj = obj[slice_obj]
        obj[slice_sequence[-1]] = val

    def as_serializable(self):
        return {"value": self.slice_sequence}

    @classmethod
    def from_serializable(cls, value):
        obj = object.__new__(cls)
        obj.slice_sequence = value
        return obj

    # # DEPRECATED

    # def assign(self, *args, **kwargs):
    #     warnings.warn("Use method 'set' instead.", DeprecationWarning)
    #     return self.set(*args, **kwargs)

    # def serialize(self):
    #     return self._serializer.serialize(self)

    # @classmethod
    # def deserialize(cls, srlzd):
    #     return cls._serializer.deserialize(srlzd)


SSQ_ = SliceSequence
