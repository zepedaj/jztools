from jztools.object_recorder import recording_switch_utils as mdl
from jztools.object_recorder import base_get


class TestFunctionsParent:
    ## Used by `jazs-git/jztools/jztools/object_recorder/recording_switch_utils.py`
    def _test_default_root__parent_method(self):
        rec_switch = mdl.recording_switch(
            lambda: dict(), rec_mode="RECORD", _mk_root=False
        )
        return rec_switch.filename
