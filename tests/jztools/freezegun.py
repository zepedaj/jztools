import pytz
from datetime import datetime

from datetime import datetime
import pytz

from jztools import freezegun as mdl
import freezegun as orig_fg


class TestFunctions:
    # @freeze_time(datetime(2020, 10, 10, 10, 00))
    # @freeze_time("2020-10-10T10:00")
    def test_freeze_time_tz(self):

        with mdl.freeze_time_tz("2020-10-10T10:00", "US/Eastern"):

            ret_now = datetime.now(pytz.timezone("US/Eastern"))
            exp_now = pytz.timezone("US/Eastern").localize(datetime(2020, 10, 10, 10))

            assert ret_now == exp_now
