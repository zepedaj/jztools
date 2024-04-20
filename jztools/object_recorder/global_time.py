from datetime import datetime
from freezegun import freeze_time
from freezegun.api import FrozenDateTimeFactory
from jztools.datetime64 import FlexDateTime, as_naive_utc
import pytz


class _GlobalTime:
    freeze_time_context = None
    frozen_time: FrozenDateTimeFactory = None

    def _format_time(self, where: FlexDateTime):
        return pytz.UTC.localize(as_naive_utc(where, pytz.UTC)).item()

    def __bool__(self):
        """Checks whether global time is being used."""
        return self.frozen_time is not None

    def __enter__(self):
        self.freeze_time_context = freeze_time()
        self.frozen_time = self.freeze_time_context.__enter__()

    def move_to(self, where: FlexDateTime, monotonic=True):
        where = pytz.UTC.localize(as_naive_utc(where, pytz.UTC).item())
        if not monotonic or where > datetime.now(pytz.UTC):
            self.frozen_time.move_to(where)

    def __exit__(self, *args):
        self.freeze_time_context.__exit__(*args)
        self.frozen_time = None


GLOBAL_TIME = _GlobalTime()
"""
Context containing the global time that is adjusted by :attr:`~pblig.object_recorder.recorded_attributes.PlayedBackAttribute.value<PlayedBackAttribute.value>` accesses.
If time is not warpped, will be set to ``None``. Use :func:`monotonically_move_global_time` to modify.
"""
