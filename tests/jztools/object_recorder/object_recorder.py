from unittest import TestCase
from tempfile import NamedTemporaryFile, TemporaryDirectory
import jztools.object_recorder.object_recorder as mdl
from jztools.object_recorder.recorded_attributes import (
    PlayedBackAttribute,
    PlayedBackCall,
    RecordedAttribute,
)
from jztools.object_recorder.recording_switch_utils import RecMode
from jztools.object_recorder.utils import base_get
import pytest


class MyObject:
    def __init__(self, a=1, b=2):
        self.a = a
        self.b = b

    def method(self, a, b):
        self.a = a
        self.b = b
        return a + b

    def __getitem__(self, n):
        return n


class NonSubsObj:
    pass


def as_player(rec_obj: mdl.ObjectRecorder):
    from xerializer import Serializer

    with mdl.with_vanilla_getattribute():
        srlzr = Serializer()
        return srlzr.deserialize(srlzr.serialize(rec_obj))


class TestRecording(TestCase):
    def test_all(self):
        obj = MyObject()
        #
        rec_obj = mdl.ObjectRecorder(obj)
        self.assertEqual(rec_obj.a, 1)
        self.assertEqual(rec_obj.b, 2)
        self.assertEqual(rec_obj.method(3, 4), 7)
        self.assertEqual(rec_obj.a, 3)
        self.assertEqual(rec_obj.b, 4)
        self.assertEqual(rec_obj[-1], -1)
        #
        pb_obj = as_player(rec_obj)
        recordings = base_get(pb_obj, "recordings")
        assert [_r.name for _r in recordings] == [
            "a",
            "b",
            "method",
            "a",
            "b",
            "__getitem__",
        ]
        assert [type(_r) for _r in recordings] == [
            PlayedBackAttribute,
            PlayedBackAttribute,
            PlayedBackCall,
            PlayedBackAttribute,
            PlayedBackAttribute,
            PlayedBackCall,
        ]
        self.assertEqual(pb_obj.a, 1)
        with self.assertRaises(mdl.NonMatchingRequest):
            pb_obj.a
        self.assertEqual(pb_obj.b, 2)
        self.assertEqual(pb_obj.method(3, 4), 7)
        self.assertEqual(pb_obj.a, 3)
        self.assertEqual(pb_obj.b, 4)
        self.assertEqual(pb_obj[-1], -1)

    def test_getitem(self):
        obj = MyObject()
        rec_obj = mdl.ObjectRecorder(obj)
        assert rec_obj[0] == 0
        assert rec_obj[1] == 1

        pb_obj = as_player(rec_obj)
        assert pb_obj[0] == 0
        assert pb_obj[1] == 1

    def test_getitem_dict(self):
        obj = {"a": 0, "b": 1}
        rec_obj = mdl.ObjectRecorder(obj)
        assert rec_obj["a"] == 0
        assert rec_obj["b"] == 1

        pb_obj = as_player(rec_obj)
        assert pb_obj["a"] == 0
        assert pb_obj["b"] == 1

    def test_non_subscriptable(self):
        with NamedTemporaryFile() as tmpf:
            rec_obj = mdl.ObjectRecorder(nso := NonSubsObj())

            with pytest.raises(
                TypeError, match="'NonSubsObj' object is not subscriptable"
            ):
                nso[0]

            # Rec obj fails
            with pytest.raises(
                TypeError, match="'_ObjectRecorder' object is not subscriptable"
            ):
                rec_obj[0]

    def test_class_attribute(self):
        rec_obj = mdl.ObjectRecorder(obj := MyObject())
        isinstance(rec_obj, int)

        pb_obj = as_player(rec_obj)
        recordings = mdl.base_get(rec_obj, "recordings")
        assert len(recordings) == 1
        assert recordings[0].name == "__class__"

        assert isinstance(pb_obj, MyObject)

    def test_len(self):
        rec_obj = mdl.ObjectRecorder({"a": 0, "b": 1})
        assert len(rec_obj) == 2

        pb_obj = as_player(rec_obj)
        assert len(pb_obj) == 2

    def test_docs(self):
        # Visible Code outside group
        from jztools.object_recorder import recording_switch
        from jztools.object_recorder import base_get  # Usually not required
        from jztools.object_recorder.object_recorder import (
            ObjectRecorder,
            ObjectPlayer,
        )  # Usually not required

        # Hidden setup code
        from jztools.contextlib import environ
        from jztools.object_recorder import factory, base_get
        from jztools.object_recorder.object_recorder import ObjectRecorder, ObjectPlayer
        import os

        temp_dir_obj = TemporaryDirectory()

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
        # As bash code
        # >> REC_MODE='RECORD' pytest tests.py::test_mode
        # Hidden test code
        with environ({"REC_MODE": "RECORD"}):
            _, recording_switch = factory(temp_dir_obj.__enter__())
            test_function()

        # By default, otherwise, the test runs in playback mode.
        # As bash code:
        # >> pytest tests.py::test_mode
        # Hidden test code
        with environ({"REC_MODE": "PLAYBACK"}):
            _, recording_switch = factory(temp_dir_obj.__enter__())
            test_function()

        # Hidden cleanup code
        temp_dir_obj.__exit__(None, None, None)
