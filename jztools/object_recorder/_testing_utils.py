from contextlib import contextmanager
from tempfile import TemporaryDirectory
from jztools.object_recorder.recording_switch_utils import factory as _factory


@contextmanager
def temp_factory():
    # Returns a recording switch in a temporary directory in order to support tests that do not litter
    # the test source code directory.
    with TemporaryDirectory() as temp_dir:
        live_test, recording_switch = _factory(root=temp_dir)
        yield live_test, recording_switch, temp_dir
