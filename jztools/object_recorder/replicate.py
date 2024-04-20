from typing import List
from jztools.object_recorder.object_recorder import ObjectPlayer, ObjectRecorder
from jztools.validation import checked_get_single
from .rec_obj_factory import register_rec_obj_factory, RecObjFactory


class replicate(RecObjFactory):
    """
    Can be used to verify that the output of a function does not change.

    .. testsetup:: replicate

        from jztools.object_recorder import factory, replicate
        from tempfile import TemporaryDirectory

        temp_dir = TemporaryDirectory()

        _, recording_switch = factory(temp_dir.__enter__())

    .. testcode::

        from jztools.object_recorder import recording_switch, replicate

    .. testcode:: replicate

        def my_function(N):
            return list(range(N))

        orig_list = my_function(3)
        with recording_switch(replicate(orig_list), rec_mode='RECORD') as saved_list:
            assert orig_list == saved_list

        # The lists are equal
        with recording_switch(replicate(orig_list), rec_mode='PLAYBACK') as saved_list:
            assert orig_list == saved_list

        # The lists are not equal
        wrong_list = my_function(4)
        with recording_switch(replicate(orig_list), rec_mode='PLAYBACK') as saved_list:
            assert wrong_list != saved_list

    .. testcleanup:: replicate

        temp_dir.__exit__(None, None, None)


    """

    def __init__(self, value):
        self.value = value

    def build_live(self):
        return self.value

    def build_recorded(self):
        out = ObjectRecorder(_ID(self.value))
        return out.get(), [out]

    def build_played_back(self, recordings):
        op = checked_get_single(recordings)
        return op.get(), [op]


class _ID:
    """
    Replicates the initializer argument.
    """

    def __init__(self, x):
        self._x = x

    def get(self):
        return self._x
