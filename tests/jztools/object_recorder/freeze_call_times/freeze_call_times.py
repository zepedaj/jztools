from jztools.object_recorder import freeze_call_times
from jztools.object_recorder.example import MyExampleClass
from ..recording_switch_utils import factory


def test_all():
    # .. testsetup:: freeze

    from jztools.object_recorder._testing_utils import temp_factory

    _factory = temp_factory()
    _, recording_switch, _ = _factory.__enter__()

    # .. testcode:: freeze

    from jztools.object_recorder import freeze_call_times
    from jztools.object_recorder.example import MyExampleClass

    my_obj = MyExampleClass(
        3
    )  # Objects instantiated before the call will also be patched.
    orig_method = MyExampleClass.my_method
    for args in [
        ((MyExampleClass, "my_method"), MyExampleClass.my_method),
        ((MyExampleClass, "my_method"),),
    ]:
        with recording_switch(freeze_call_times(*args), rec_mode="RECORD"):
            assert MyExampleClass.my_method is not orig_method
            orig_output = MyExampleClass(2).my_method(5)
            orig_output_2 = my_obj.my_method(6)

    assert MyExampleClass.my_method is orig_method

    # The lists are equal
    with recording_switch(
        freeze_call_times((MyExampleClass, "my_method"), MyExampleClass.my_method),
        rec_mode="PLAYBACK",
    ):
        played_back_output = MyExampleClass(2).my_method(5)
        played_back_output_2 = my_obj.my_method(6)
        assert orig_output == played_back_output
        assert orig_output_2 == played_back_output_2

    assert MyExampleClass.my_method is orig_method

    # .. testcleanup:: freeze

    _factory.__exit__(None, None, None)


def my_function():
    from time import time

    return time()


def test_module_function():
    y = None
    import tests.jztools.object_recorder.freeze_call_times.freeze_call_times as fct_module

    orig_fct = my_function

    for rec_mode in ["RECORD", "OVERWRITE"]:
        with factory() as (_, recording_switch, _), recording_switch(
            freeze_call_times((fct_module, "my_function"), my_function),
            rec_mode=rec_mode,
        ):
            x = my_function()
            if x is None:
                assert y == x
            else:
                y = x
            pass
            assert fct_module.my_function is not orig_fct
        assert fct_module.my_function is orig_fct
