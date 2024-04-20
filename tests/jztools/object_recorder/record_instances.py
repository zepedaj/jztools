from jztools.object_recorder._testing_utils import temp_factory as factory
from jztools.object_recorder import record_instances
from jztools.object_recorder.object_recorder import ObjectPlayer, ObjectRecorder


class MyObj:
    prop_1 = 1
    prop_2 = "c"
    init_calls = 0

    def __init__(self, prop_1=1):
        self.prop_1 = prop_1
        self.init_calls += 1

    def meth_1(self, a):
        return a * 2


class MyObjWithNew:
    init_calls = 0

    def __init__(self, a, b):
        self.prop_1 = a
        self.prop_2 = b
        self.init_calls += 1

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    def meth_1(self, a):
        return a * 2


REC_TYPES = {
    "RECORD": ObjectRecorder,
    "OVERWRITE": ObjectRecorder,
    "PLAYBACK": ObjectPlayer,
}


class TestRecordInstances:
    def test_all(self):
        with factory() as (_, recording_switch, temp_dir):
            for rec_mode in ["RECORD", "PLAYBACK", "FORCE_LIVE"]:
                with (
                    rec_switch := recording_switch(rec_mode=rec_mode)
                ), record_instances(rec_switch, MyObj), record_instances(
                    rec_switch, MyObjWithNew
                ):
                    for obj in [MyObj(), MyObjWithNew(1, "c")]:
                        if rec_mode == "FORCE_LIVE":
                            assert not any(
                                isinstance(obj, _t) for _t in REC_TYPES.values()
                            )
                        else:
                            assert isinstance(
                                obj,
                                REC_TYPES[rec_mode],
                            )

                        assert obj.meth_1(2) == 4
                        assert obj.prop_1 == 1
                        assert obj.prop_2 == "c"
                        assert obj.init_calls == 1

    def test_context_manager(self):
        with factory() as (_, recording_switch, temp_dir):
            for rec_mode in ["RECORD", "FORCE_LIVE"]:
                with (
                    rec_switch := recording_switch(rec_mode=rec_mode)
                ), record_instances(rec_switch, MyObj):
                    my_obj = MyObj(2)
                    assert my_obj.prop_1 == 2
                my_obj = MyObj(2)
                assert my_obj.prop_1 == 2
                assert my_obj.init_calls == 1

    def test_inheritance(self):
        class MyDerived(MyObjWithNew):
            pass

        with factory() as (_, recording_switch, temp_dir):
            for rec_mode in ["OVERWRITE", "PLAYBACK"]:
                #
                with (
                    rec_switch := recording_switch(rec_mode=rec_mode)
                ), record_instances(rec_switch, MyObjWithNew):
                    # Object recording is not inherited
                    obj = MyDerived(1, 2)
                    assert not isinstance(obj, REC_TYPES[rec_mode])
                    assert isinstance(obj, MyDerived)
                    assert obj.init_calls == 1

                    # The parent class is still recorded
                    obj = MyObjWithNew(1, 2)
                    assert isinstance(obj, REC_TYPES[rec_mode])
                    assert isinstance(obj, MyObjWithNew)
                    assert obj.init_calls == 1
