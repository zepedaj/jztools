"""

Motivation
============

Getting or setting nested object members often requires a sequence of attribute and key references such as

.. code-block::

  B.contents['obj'][2].value

This access pattern can be summarized by the following reference sequence:

#. Get attribute ``contents``.
#. Get entry with key ``'obj'``
#. Get entry with key ``2``.
#. Get attribute ``value``.

This same reference sequence might need to be *transferred* (applied) to a variety of current and future objects, or it might need sent as a parameter to an algorithm. Being able to save *serialize* it in human-readable form for future use is also relevant. 


Usage
======

This module presents a natural way to define *transferable*, *serializable* reference sequences. Such sequences can be defined by applying the same pattern (e.g., everything after ``B.`` in the above exampe) to a :class:`RefSeq` object:

.. code-block::

  rs = RefSeq().contents['obj'][2].value

Setting or getting a value for a given object can then be carried out using the :meth:`~RefSeq.__call__` method:

.. code-block::

  value = rs(B)
  rs(B, new_value)


.. testcode:: group

    from jztools.reference_sequence import RefSeq


.. testcode:: group

    # Using reference sequences
    arr = {'key1':list(range(20))}

    orig = arr['key1'][1:10][0]
    with_syntax1 = RefSeq()['key1'][1:10][0](arr)
    with_syntax2 = RefSeq(['[key1]', slice(1,10), 0])(arr)

    assert with_syntax1 == orig
    assert with_syntax1 == with_syntax2

Slice sequence objects can be serialized in a human-readable format:

.. testcode:: group

    from xerializer import Serializer

    # Serialization:
    srlzr = Serializer()
    rs = RefSeq()['key1'][1:10][0]
    json_string = srlzr.serialize(rs)

    print(json_string)

    deserialized_rs = srlzr.deserialize(json_string)
    assert deserialized_rs(arr) == rs(arr)

.. testoutput:: group

    {"__type__": "RefSeq", "value": ["[key1]", {"__type__": "slice", "start": 1, "stop": 10}, 0]}


Reference sequences can also contain references to attributes that can be accessed in a natural way:

.. testcode:: group

  class A:
    value = 'target'

  class B:
    contents = {'obj': [0, 1, A]}

  # Accessing  B.contents['obj'][2].value
  rs = RefSeq().contents['obj'][2].value
  assert rs(B) == 'target'

This approach to attribute access will pose a problem when referenced attribute and object attribute names collide. To minimize this occurrence, all object attributes are pre- or suffixed with ``_``. Nonetheless, it is safer to use the :meth:`~RefSeq.a_` method:

.. testcode:: group

  rs = RefSeq().a_('contents')['obj'][2].a_('value')
  assert rs(B) == 'target'

Reference sequences with attribute references can also be serialized. String keys and attributes are differentiated by surrounding keys with square brackets in the serialized representation:

.. testcode:: group

  print(srlzr.serialize(rs))

.. testoutput:: group

  {"__type__": "RefSeq", "value": ["contents", "[obj]", 2, "value"]}

A similar syntax can be used to initialize reference sequences directly:

.. testcode:: group

  rs = RefSeq(["contents", "[obj]", 2, "value"])
  assert rs(B) == 'target'

In this syntax, all non-string and bracket-enclosed string values are treated as keys, while non-bracket-enclosed strings are treated as attributes. Alternatively, the attribute and key helper functions :func:`a_` and :func:`k_` can be used to explicitly indicate intent for some or all entries:

.. testcode:: group

  from jztools.reference_sequence import a_, k_

  rs = RefSeq([a_("contents"), k_("obj"), k_(2), a_("value")])
  assert rs(B) == 'target'


To summarize, all the following are equivalent:

.. testcode:: group

  rs1 = RefSeq().contents['obj'][2].value
  rs2 = RefSeq().a_('contents').k_('obj').k_(2).a_('value')
  rs3 = RefSeq([a_("contents"), k_("obj"), k_(2), a_("value")])

  assert rs1(B) == 'target'
  assert rs2(B) == 'target'
  assert rs3(B) == 'target'

Note that any serializable object can be used as a key, including string-key dictionaries and slices.


.. todo::

  * Re-organize, add ..rubric:: headers.
  * Add ``set_`` examples.
  * Add exampes for slice and dictionary keys, including serialization.


.. warning::

  Care needs to be taken when using :class:`RefSeq` objects since mis-spelled member names will not raise errors but rather return a new object with the misspelled member appended as an attribute reference. This is mitigated by the fact that only two members (:meth:`RefSeq.k_` and :meth:`RefSeq.a_`) should be called directly by the user. Note also that all :class:`RefSeq` members begin or end with ``_``.

.. warning::

  Reference sequences should only be initialized from trusted sources, and in particular those initialized from user input can be harmful.

"""

from jztools.py import class_name, cutoff_str
from typing import Any
from xerializer.abstract_type_serializer import TypeSerializer as _TypeSerializer
import abc
from copy import deepcopy


class RefSeq:
    """
    Serializable, transferable reference sequence.
    """

    signature = "RefSeq"

    def __init__(self, value=None):
        if value is None:
            self._ref_seq = []
        elif isinstance(value, type(self)):
            self._ref_seq = deepcopy(value._ref_seq)
        elif isinstance(value, (list, tuple)):
            self._ref_seq = [_Ref.from_escaped(_x) for _x in value]
        else:
            raise ValueError("Unexpected init value {value}.")

    def __str__(self):
        """
        Human-readable string representation of reference sequence.
        """
        return f"{class_name(type(self))}<{self._ref_seq}>"

    def __eq__(self, rs):
        """
        Equality operator.
        """
        if type(self) != type(rs):
            return NotImplemented
        else:
            return self._ref_seq == rs._ref_seq

    def copy(self):
        return type(self)(self)

    def k_(self, key: Any):
        """
        Appends a key reference - same operation as :meth:`__getitem__`. Provided for consistency with :meth:`a_`.

        Keys can be appended to the reference sequence ``rs`` using ``rs = rs[new_key]`` or ``rs = rs.k_(new_key)``.

        Keys can be any object.
        """
        out = self.copy()
        out._ref_seq.append(k_(key))
        return out

    def __getitem__(self, key: Any):
        """
        Alias to :meth:`k_`.
        """
        return self.k_(key)

    def a_(self, name: str):
        """
        Appends an attribute reference. See also :meth:`__getattribute__`.

        Attribute ``'attr'`` can be appended to the reference sequence ``rs`` using ``rs = rs.attr`` (when there is no conflict with :class:`RefSeq` members) or ``rs = rs.a_('attr')``.

        Attributes need to be valid python identifiers.
        """
        out = self.copy()
        out._ref_seq.append(a_(name))
        return out

    def __getattribute__(self, attr: str):
        """
        When attr is not a member of :class:`RefSeq`, behaves like an alias of :meth:`a_`. Otherwise, returns the ``self`` attribute of name ``attr``. This can cause name conflicts which can be avoided by using :meth:`a_`.
        """
        try:
            return object.__getattribute__(self, attr)
        except AttributeError:
            return self.a_(attr)

    def __call__(self, *args):
        """
        Sets or gets the ``obj`` at the reference sequence:

        * ``__call__(obj)`` Gets the value of ``obj`` at the reference sequence.
        * ``__call__(obj, val)`` Sets the value of ``obj`` at the reference sequence to ``val``.

        """

        # Get
        if len(args) == 1:
            orig_obj = args[0]
            obj = orig_obj
            for k, ref in enumerate(self._ref_seq):
                try:
                    obj = ref.get(obj)
                except Exception:
                    raise Exception(
                        f"{self} retrieval failed when applying {k}-nested {ref.ref_type} reference '{ref}' to {cutoff_str(str(orig_obj)+'.')}\nSee above error."
                    )
            return obj

        # Set
        elif len(args) == 2:
            obj, val = args

            ref_seq = self._ref_seq if len(self._ref_seq) > 1 else [slice(None)]
            for ref in ref_seq[:-1]:
                obj = ref.get(obj)
            ref_seq[-1].set(obj, val)

        else:
            raise Exception(
                f"Invalid number of input argments. Received {len(args)}, expected 1 or 2."
            )


class _RefSeqSerializer(_TypeSerializer):
    signature = "RefSeq"
    handled_type = RefSeq

    def as_serializable(self, obj):
        return {"value": [_x.escape() for _x in obj._ref_seq]}

    def from_serializable(self, value):
        return self.handled_type([_Ref.from_escaped(_x) for _x in value])


class _Ref(abc.ABC):
    def __init__(self, value):
        if isinstance(value, _Ref):
            self.value = deepcopy(value.value)
        else:
            self.value = value

    @property
    @abc.abstractmethod
    def ref_type(self):
        """
        Returns a human-readable string name for the class.
        """
        pass

    @classmethod
    def from_escaped(cls, value):
        """
        If value is a string object enclosed in brackets, returns a :class:`a_` reference.
        If it is already an :class:`a_` or :class:`k_` object, returns a copy.
        Otherwise, returns,  a :class:`k_` reference.
        """

        # value[0] can raise a KeyError if value == '',
        # but the empty string is an invalid input to begin with,
        # as attributes are non empty and keys are escaped with brackets.
        if isinstance(value, str) and value[0] == "[" and value[-1] == "]":
            return k_(value[1:-1])
        elif isinstance(value, str):
            return a_(value)
        elif isinstance(value, cls):
            return type(value)(value)
        else:
            return k_(value)

    def __str__(self):
        return self.escape()

    @abc.abstractmethod
    def get(self):
        pass

    @abc.abstractmethod
    def set(self, value):
        pass

    def __eq__(self, val):
        if type(self) != type(val):
            return NotImplemented
        else:
            return self.value == val.value


class k_(_Ref):
    """
    Specifies that the provided value is a key retrieved / set with ``__getitem__`` / ``__setitem__`` (``obj[key]`` syntax). When serializing this key, strings will be wrapped in square brackets and other types will be passed through.
    """

    ref_type = "key"

    def escape(self):
        if isinstance(self.value, str):
            return f"[{self.value}]"
        else:
            return self.value

    def get(self, obj):
        return obj[self.value]

    def set(self, obj, val):
        obj[self.value] = val


class a_(_Ref):
    """
    Specifies that the provided value is an attribute retrieved / set with ``getattr`` / ``setattr`` (``obj.attribute`` syntax).
    """

    ref_type = "attribute"

    def escape(self):
        return self.value

    def get(self, obj):
        return getattr(obj, self.value)

    def set(self, obj, val):
        setattr(obj, self.value, val)
