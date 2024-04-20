from contextlib import contextmanager
import json
from jztools.freezegun import freeze_time_tz
from jztools.object_recorder import recording_switch_utils as mdl
from jztools.object_recorder import replicate
from jztools.object_recorder.object_recorder import ObjectPlayer, ObjectRecorder
from pathlib import Path
from .object_recorder import MyObject
from jztools.object_recorder._testing_utils import temp_factory as factory

from ..recording_switch_utils__helper import TestFunctionsParent

from unittest import TestCase


class MyContext:
    a = None

    def __enter__(self):
        self.a = 1

    def __exit__(self, *args, **kwargs):
        self.a = None


class ComplexObj:
    prop = 10

    def get_obj(self):
        return ComplexObj()

    def __call__(self, k):
        return k * 2

    def get_half(self, k):
        return k / 2


class TestFunctions(TestCase, TestFunctionsParent):
    def test_all(self):
        with factory() as (_, rec_switch, temp_dir):
            obj = MyObject()

            rs = rec_switch(lambda: obj, rec_mode="RECORD")
            with rs as rec_obj:
                _ = rec_obj.a
                _ = rec_obj.b

            self.assertTrue(
                (
                    expected := Path(temp_dir)
                    / "tests.jztools.object_recorder.recording_switch_utils.TestFunctions.test_all.json"
                ).is_file()
            )
            assert expected == rs.filename

            with rec_switch(lambda: obj) as pb_obj:
                assert issubclass(type(pb_obj), ObjectPlayer)
                self.assertEqual(obj.a, pb_obj.a)
                self.assertEqual(obj.b, pb_obj.b)

    def test_default_root(self):
        # Remove the expected file.
        expected = (
            Path(__file__).parent
            / "_recordings/tests.jztools.object_recorder.recording_switch_utils.TestFunctions.test_default_root.json"
        )
        expected.unlink(missing_ok=True)

        try:
            rec_switch = mdl.recording_switch(lambda: MyObject(), rec_mode="RECORD")
            with rec_switch:
                pass

            # Check that it was created during recording
            self.assertTrue(expected.is_file())
        finally:
            expected.unlink(missing_ok=True)

    def test_default_root__derived_class(self):
        # Remove the expected file.
        expected = (
            Path(__file__).parent
            / "_recordings/tests.jztools.object_recorder.recording_switch_utils.TestFunctions._test_default_root__parent_method.json"
        )

        actual = self._test_default_root__parent_method()
        assert expected == actual

    def test_context(self):
        with factory() as (_, rec_switch, _):
            for rec_mode in ["RECORD", "PLAYBACK"]:
                with rec_switch(lambda: MyContext(), rec_mode=rec_mode) as rec_cntx:
                    self.assertIsNone(x := rec_cntx.a)
                    with rec_cntx:
                        self.assertEqual(rec_cntx.a, 1)
                    self.assertIsNone(rec_cntx.a)

    def test_no_context(self):
        with factory() as (_, rec_switch, _):
            with self.assertRaisesRegex(AttributeError, "__enter__"):
                with rec_switch(lambda: MyObject(), rec_mode="RECORD") as rec_obj:
                    with rec_obj:
                        pass

    def test_multi_obj(self):
        with factory() as (_, rec_switch, _):
            for rec_mode in ["RECORD", "PLAYBACK"]:
                with (
                    rs := rec_switch(
                        lambda: MyObject(1, 2),
                        lambda: MyObject(3, 4),
                        rec_mode=rec_mode,
                    )
                ) as (
                    rec_obj_1,
                    rec_obj_2,
                ):
                    assert issubclass(
                        type(rec_obj_1),
                        ObjectRecorder if rec_mode == "RECORD" else ObjectPlayer,
                    )
                    assert issubclass(
                        type(rec_obj_2),
                        ObjectRecorder if rec_mode == "RECORD" else ObjectPlayer,
                    )
                    self.assertEqual(rec_obj_1.a, 1)
                    self.assertEqual(rec_obj_2.a, 3)

    def test_extend_enter(self):
        with factory() as (_, rec_switch, _):
            for rec_mode in ["RECORD", "PLAYBACK"]:
                with (
                    rs := rec_switch(
                        lambda: MyObject(1, 2),
                        rec_mode=rec_mode,
                    )
                ) as (rec_obj_1):
                    assert issubclass(
                        type(rec_obj_1),
                        ObjectRecorder if rec_mode == "RECORD" else ObjectPlayer,
                    )
                    self.assertEqual(rec_obj_1.a, 1)

                    rec_obj_2 = rs.extend_enter(lambda: MyObject(3, 4))
                    assert issubclass(
                        type(rec_obj_2),
                        ObjectRecorder if rec_mode == "RECORD" else ObjectPlayer,
                    )
                    self.assertEqual(rec_obj_2.a, 3)

    def test_replicate(self):
        from jztools.object_recorder import factory, replicate
        from tempfile import TemporaryDirectory

        temp_dir = TemporaryDirectory()

        # .. testsetup:: replicate
        _, recording_switch = factory(temp_dir.__enter__())

        # .. testcode::

        # from jztools.object_recorder import recording_switch, replicate

        # .. testcode:: replicate

        def my_function(N):
            return list(range(N))

        orig_list = my_function(3)
        with recording_switch(replicate(orig_list), rec_mode="RECORD") as saved_list:
            assert orig_list == saved_list

        # The lists are equal
        with recording_switch(replicate(orig_list), rec_mode="PLAYBACK") as saved_list:
            assert orig_list == saved_list

        # The lists are not equal
        wrong_list = my_function(4)
        with recording_switch(replicate(orig_list), rec_mode="PLAYBACK") as saved_list:
            assert wrong_list != saved_list

        # .. testcleanup:: replicate

        temp_dir.__exit__(None, None, None)

    @contextmanager
    def _helper_context(self, rec_mode):
        #
        with factory() as (live_test, recording_switch, temp_dir):
            # Adds two levels: _helper_context and contextmanager.__enter__
            rec_switch_0 = recording_switch(
                replicate(1), rec_mode=rec_mode, _caller_skip_levels=5
            )

            with (
                rec_switch := recording_switch(
                    replicate(1), rec_mode=rec_mode, _caller_skip_levels=5
                )
            ) as val:
                assert rec_switch_0.filename == rec_switch.filename
                yield rec_switch, val, temp_dir

    def test_filename__nested_context(self):
        with self._helper_context("RECORD") as (rec_switch, val, temp_dir):
            expected = (
                Path(temp_dir)
                / "tests.jztools.object_recorder.recording_switch_utils.TestFunctions.test_filename__nested_context.json"
            )
            assert rec_switch.filename == expected

    @freeze_time_tz("2022-01-04T09:20", "US/Eastern")
    def test_filename__freezetime(self):
        with factory() as (live_test, recording_switch, temp_dir):
            rs = recording_switch(replicate(1))
            expected = (
                Path(temp_dir)
                / "tests.jztools.object_recorder.recording_switch_utils.TestFunctions.test_filename__freezetime.json"
            )
            assert rs.filename == expected

    def test_get_default_filename(self):
        assert mdl.get_default_filename(caller_skip_levels=2, mk_root=False) == str(
            Path(__file__).parent
            / "_recordings/tests.jztools.object_recorder.recording_switch_utils.TestFunctions.test_get_default_filename.json"
        )

        def dmy_fxn():
            assert mdl.get_default_filename(caller_skip_levels=2, mk_root=False) == str(
                Path(__file__).parent
                / "_recordings/tests.jztools.object_recorder.recording_switch_utils.dmy_fxn.json"
            )

            assert mdl.get_default_filename(caller_skip_levels=3, mk_root=False) == str(
                Path(__file__).parent
                / "_recordings/tests.jztools.object_recorder.recording_switch_utils.TestFunctions.test_get_default_filename.json"
            )

        dmy_fxn()

    def test_isinstance(self):
        with factory() as (
            live_test,
            recording_switch,
            temp_dir,
        ):
            for rec_mode in ["RECORD", "PLAYBACK"]:
                with recording_switch(
                    lambda: {0: 0, 1: 1}, lambda: [0, 1, 2, 3], rec_mode=rec_mode
                ) as (my_dict, my_list):
                    wrapper_type = (
                        ObjectRecorder if rec_mode == "RECORD" else ObjectPlayer
                    )

                    # Records __class__ access
                    isinstance(my_dict, dict)
                    isinstance(my_list, list)

                    # Calls to type() are not recorded
                    issubclass(type(my_dict), wrapper_type)
                    issubclass(type(my_list), wrapper_type)

    def test_compound(self):
        with factory() as (
            live_test,
            recording_switch,
            temp_dir,
        ):
            for rec_mode in ["RECORD", "PLAYBACK"]:
                with (rs := recording_switch(ComplexObj, rec_mode=rec_mode)) as obj:
                    assert obj.prop == 10
                    assert (x := obj.get_half(10)) == 5
                    new_obj = obj.get_obj()
                    assert isinstance(new_obj, ComplexObj)
                    assert isinstance(
                        new_obj,
                        ObjectRecorder if rec_mode == "RECORD" else ObjectPlayer,
                    )
                    assert new_obj.__class__ is ComplexObj
                    assert obj.__class__ is ComplexObj

    def test_iter(self):
        class GenIter:
            def __init__(self, N):
                self.N = N

            def __iter__(self):
                yield from range(self.N)

        class SelfIter:
            def __init__(self, N):
                self.N = N

            def __iter__(self):
                self.k = 0
                return self

            def __next__(self):
                if self.k < self.N:
                    out = self.k
                    self.k += 1
                    return out
                raise StopIteration()

        for iter_cls in [GenIter, SelfIter]:
            with factory() as (
                live_test,
                recording_switch,
                temp_dir,
            ):
                for rec_mode in ["RECORD", "PLAYBACK"]:
                    with (
                        rs := recording_switch((iter_cls, (10,)), rec_mode=rec_mode)
                    ) as obj:
                        assert list(obj) == list(range(10))

    def test_factory_doc(self):
        # .. testcode::

        # (Not part of group)
        custom_dir = "./my_custom_recordings_dir"

        # .. testsetup:: factory

        from tempfile import TemporaryDirectory

        _temp_dir = TemporaryDirectory()
        custom_dir = _temp_dir.__enter__()

        # .. testcode:: factory

        from jztools.object_recorder import factory, replicate
        from jztools.contextlib import environ

        # You can create a custom live_test decorator and recording_switch
        # using `factory`
        live_test, recording_switch = factory(custom_dir, "MY_REC_MODE")

        # .. testcode:: factory

        for rec_mode in ["RECORD", "PLAYBACK"]:
            with environ({"MY_REC_MODE": rec_mode}):
                rec_switch = recording_switch(replicate({"a": 0}))
                with rec_switch as my_dir:
                    assert len(my_dir) == 1
                    assert rec_switch.rec_mode.name == rec_mode

        # .. testcleanup:: factory

        _temp_dir.__exit__(None, None, None)
