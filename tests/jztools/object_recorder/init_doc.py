from jztools.object_recorder.recording_switch_utils import RecMode


def test_doc__keep_in_mind():
    # .. testcode::
    # (Not part of group)
    from jztools.object_recorder import recording_switch
    from jztools.object_recorder.object_recorder import ObjectRecorder

    # .. testsetup:: init

    from jztools.object_recorder._testing_utils import temp_factory

    _factory = temp_factory()
    _, recording_switch, _ = _factory.__enter__()

    # .. testcode:: init

    import random, datetime

    # Set the shuffle seed
    random.seed(0)

    # Freeze time
    import freezegun

    with freezegun.freeze_time(datetime.datetime(2023, 1, 1, 12, 0)):
        ...

    # .. testcode:: init

    # Typing is slightly different
    with recording_switch(lambda: {"a": 0}, rec_mode="RECORD") as my_dict:
        assert isinstance(my_dict, dict)
        assert isinstance(my_dict, ObjectRecorder)  # Does not record!

    # .. testcleanup:: init

    _factory.__exit__(None, None, None)


def test_doc__example_use_case():
    # .. testcode::

    #
    from jztools.object_recorder import recording_switch
    from jztools.object_recorder import base_get  # Usually not required
    from jztools.object_recorder.object_recorder import (
        ObjectRecorder,
        ObjectPlayer,
    )  # Usually not required

    # .. testsetup:: object_recorder

    from jztools.contextlib import environ
    from jztools.object_recorder import base_get
    from jztools.object_recorder._testing_utils import temp_factory
    from jztools.object_recorder.object_recorder import ObjectRecorder, ObjectPlayer
    import os

    # .. testcode:: object_recorder

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
                (
                    ObjectRecorder
                    if rec_switch.rec_mode == RecMode.RECORD
                    else ObjectPlayer
                ),
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

    # .. code-block:: bash

    # REC_MODE='RECORD' pytest tests.py::test_mode

    # .. testcode:: object_recorder
    #    :hide:

    _factory = temp_factory()
    _, recording_switch, _ = _factory.__enter__()

    # Hidden test code
    with environ({"REC_MODE": "RECORD"}):
        test_function()

    # .. code-block:: bash

    # Otherwise, the test runs in playback mode.
    # As bash code:
    #    pytest tests.py::test_mode

    # .. testcode:: object_recorder
    #    :hide:

    # Hidden test code
    with environ({"REC_MODE": "PLAYBACK"}):
        test_function()

    # .. testcleanup:: object_recorder
    # Hidden cleanup code
    _factory.__exit__(None, None, None)
