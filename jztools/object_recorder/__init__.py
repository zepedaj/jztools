"""
Objection recording utilities.

Things to keep in mind to make tests reproducible:


..testcode::
    # (Not part of group)
    from jztools.object_recorder import recording_switch
    from jztools.object_recorder.object_recorder import ObjectRecorder

..testsetup:: init

    from jztools.object_recorder._testing_utils import temp_factory

    _factory = temp_factory()
    _, recording_switch, _ = _factory.__enter__()

..testcode:: init

    import random, datetime

    # Set the shuffle seed
    random.seed(0)

    # Freeze time
    import freezegun

    with freezegun.freeze_time(datetime.datetime(2023, 1, 1, 12, 0)):
        ...

..testcode:: init

    # Typing is slightly different
    with recording_switch(lambda: {"a": 0}, rec_mode="RECORD") as my_dict:
        assert isinstance(my_dict, dict)
        assert isinstance(my_dict, ObjectRecorder)  # Does not record!

..testcleanup:: init

    _factory.__exit__(None, None, None)


Example use case:

.. testcode::

    #
    from jztools.object_recorder import recording_switch
    from jztools.object_recorder import base_get  # Usually not required
    from jztools.object_recorder.object_recorder import (
        ObjectRecorder,
        ObjectPlayer,
    )  # Usually not required


.. testsetup:: object_recorder

    from jztools.contextlib import environ
    from jztools.object_recorder import factory, base_get
    from jztools.object_recorder.object_recorder import ObjectRecorder, ObjectPlayer
    import os
    from tempfile import TemporaryDirectory

    temp_dir_obj = TemporaryDirectory()
    temp_dir = temp_dir_obj.__enter__()


.. testcode:: object_recorder

    # A common use case for `recording_switch` is inside a test function such
    # as the one below that is assumed to be inside a file 'tests.py'
    def test_function():
        with (
            rec_switch := recording_switch((dict, ([("a", 0), ("b", 1)],)))
        ) as dict_like:  # {'a':0, 'b': 1}
            #

            # By default, `dict_like` will try to quack like a `dict` during
            # recording and  playback, assuming playback  happens in the same order.
            assert isinstance(dict_like, dict)

            # ... but it is in fact an `ObjectRecorder` or an `ObjectPlayer`
            assert issubclass(
                base_get(dict_like, "__class__"),
                ObjectRecorder if rec_switch.rec_mode == "RECORD" else ObjectPlayer,
            )

            # The `dict_like` will try to quack like a `dict` for all object attribute accesses,
            # including most special methods/properties, when these return serializable values.
            # This currently excludes generators (like `dict.keys`, `dict.values` and `dict.items`)
            assert bool(dict_like) is True
            assert dict_like["a"] == 0
            assert isinstance(dict_like, dict)
            assert len(dict_like) == 2
            assert dict_like.get("b") == 1

    # The first time the test should be run in 'RECORD' mode to generate the recording.
    # This will generate a recording as file './_recordings/test.test_mode.json' next to 'tests.py'`

.. code-block:: bash

    REC_MODE='RECORD' pytest tests.py::test_mode

.. testcode:: object_recorder
    :hide:

    # Hidden test code
    with environ({"REC_MODE": "RECORD"}):
        _, recording_switch = factory(temp_dir)
        test_function()


.. code-block:: bash

    # Otherwise, the test runs in playback mode.
    # As bash code:
    pytest tests.py::test_mode

.. testcode:: object_recorder
    :hide:

    # Hidden test code
    with environ({"REC_MODE": "PLAYBACK"}):
        _, recording_switch = factory(temp_dir)
        test_function()

.. testcleanup:: object_recorder
    # Hidden cleanup code
    temp_dir_obj.__exit__(None, None, None)


.. todo::

    * Most important: Refactor so that all attributes in ObjectRecorder can be set to different recording modes. In particular, include stateless, stateful, jointly stateful attributes. Examples of these are:
        * Stateless: `BarDataProvider.bar_dtype`, `BarDataProvider.get_bars` -- their value does not change for the life of the object and/or depends only on input parameters. These calls can be mutli-threaded out-of-the box.
        * Stateful:  `History.update` The order and/or absolute time of calls matters. Not multi-threaded out-of-the-box.
        * Jointly stateful: `ibapi.connection.Connection.recvMsg` and `ibapi.connection.Connection.sendMsg` -- the relative order of calls (besides absolute order and/or time) to jointly stateful methods matters.
    * Better organize :class:`recording_switch` code
    * Make it possible to pass `record_instances` as an argument to `recording_switch`.
    * Have ``ObjRecFactory`` ``build_recorded`` and ``build_playedback`` return an object instead of a list of recordings.
    * Better organize the recording files to make it easier to read and to understand what is recorded in them.

"""

from .recording_switch_utils import (
    factory,
    recording_switch,
    live_test,
    RecMode,
)
from .replicate import replicate
from .freeze_call_times import freeze_call_times
from .rec_obj_factory import RecObjFactory, register_rec_obj_factory
from .object_recorder import base_get, with_vanilla_getattribute
from .call_unordered import call_unordered
from .record_instances import record_instances

__all__ = [
    "factory",
    "recording_switch",
    "replicate",
    "freeze_call_times",
    "call_unordered",
    "RecObjFactory",
    "register_rec_obj_factory",
    "live_test",
    "RecMode",
    "base_get",
    "with_vanilla_getattribute",
    "record_instances",
]
