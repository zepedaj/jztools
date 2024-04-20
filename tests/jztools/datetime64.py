import jztools.datetime64 as mdl
import re
import pytest
import pytz
import numpy as np
from unittest import TestCase
import numpy.testing as npt
import datetime

US_EASTERN = pytz.timezone("US/Eastern")


class TestFunctions(TestCase):
    def test_weekday(self):
        offsets = np.arange(-7, 7)
        orig_dates = np.array(["2012-01-02"], dtype="datetime64[D]") + np.array(
            offsets, dtype="timedelta64[D]"
        )

        for timeres in ["ns", "us", "ms", "s", "m", "h", "D"]:
            dates = orig_dates.astype(f"datetime64[{timeres}]")

            expected = offsets % 7
            npt.assert_array_equal(mdl.weekday(dates), expected)

    def test_monthday(self):
        offsets = np.arange(5)
        orig_dates = np.array(["2020-12-30"], dtype="datetime64") + offsets.astype(
            "timedelta64[D]"
        )
        expected = (30 + offsets - 1) % 31

        for timeres in ["ns", "us", "ms", "s", "m", "h", "D"]:
            dates = orig_dates.astype(f"datetime64[{timeres}]")
            npt.assert_array_equal(expected, mdl.monthday(dates))

    def test_month(self):
        offsets = np.arange(5)
        orig_dates = np.array(["2020-12-30"], dtype="datetime64") + offsets.astype(
            "timedelta64[D]"
        )
        expected = [11, 11, 0, 0, 0]

        for timeres in ["ns", "us", "ms", "s", "m", "h", "D"]:
            dates = orig_dates.astype(f"datetime64[{timeres}]")
            npt.assert_array_equal(expected, mdl.month(dates))

    def test_as_naive_utc(self):
        # From string
        assert mdl.as_naive_utc(sdt0 := "2020-10-10T10:00", in_tzinfo=pytz.UTC) == (
            ndt0 := np.datetime64(sdt0)
        )

        # From aware datetime
        dt0_aware = pytz.UTC.localize(mdl.as_datetime(np.datetime64(sdt0))).astimezone(
            pytz.timezone("US/Eastern")
        )
        assert mdl.as_naive_utc(dt0_aware) == ndt0

        # From string in different timezone
        assert mdl.as_naive_utc(
            pytz.timezone("US/Eastern").localize(
                np.datetime64(sdt0 := "2020-10-10T10:00").item()
            ),
            in_tzinfo=pytz.UTC,
        ) == np.datetime64(sdt0) + np.timedelta64(4, "h")

        # From date
        for expected, actual in [
            (np.datetime64("2020-10-10"), mdl.as_naive_utc("2020-10-10")),
            (np.datetime64("2020-10"), mdl.as_naive_utc("2020-10")),
            (np.datetime64("2020"), mdl.as_naive_utc("2020")),
        ]:
            assert expected == actual
            assert expected.dtype == actual.dtype

    def test_as_naive(self):
        # OK
        for expected, actual in [
            (
                np.datetime64("2020-10-10T06"),
                mdl.as_naive("2020-10-10T10", pytz.UTC, US_EASTERN),
            ),
        ]:
            assert expected == actual
            assert actual.dtype == np.dtype("datetime64[us]")

        # In tzinfo is required for naive inputs
        for in_datetime in [
            "2020-10-10T10",
            datetime.datetime(2020, 10, 10, 10),
            np.datetime64("2020-10-10T10"),
        ]:
            with pytest.raises(pytz.UnknownTimeZoneError):
                mdl.as_naive(in_datetime, out_tzinfo=pytz.UTC)

        # Dates returned
        for expected, actual in [
            (np.datetime64("2020-10-10"), mdl.as_naive("2020-10-10")),
            (np.datetime64("2020-10"), mdl.as_naive("2020-10")),
            (np.datetime64("2020"), mdl.as_naive("2020")),
        ]:
            assert expected == actual
            assert expected.dtype == actual.dtype

    def test_as_datetime(self):
        for expected, actual in [
            # Dates return dates
            (datetime.date(2020, 10, 10), mdl.as_datetime(np.datetime64("2020-10-10"))),
            # Aware datetime returned
            (
                pytz.UTC.localize(datetime.datetime(2020, 10, 10, 10)),
                mdl.as_datetime(np.datetime64("2020-10-10T10"), pytz.UTC),
            ),
            # Naive datetime returned
            (
                datetime.datetime(2020, 10, 10, 10),
                mdl.as_datetime(np.datetime64("2020-10-10T10")),
            ),
        ]:
            assert expected == actual
            assert type(expected) is type(actual)
            if isinstance(expected, datetime.datetime):
                assert expected.tzinfo == actual.tzinfo

        # Check warning
        dt64 = np.datetime64("2020-10-10T10:10:10.123456789")
        with pytest.warns(
            UserWarning,
            match=re.escape(
                f"Loss of precision when converting datetime64 of dtype {dt64.dtype} to datetime.datetime."
            ),
        ):
            assert (
                mdl.as_datetime(dt64)
                == np.datetime64("2020-10-10T10:10:10.123457").item()
            )

    def test_datetime64_dtype_to_timedelta64_dtype(self):
        for x in ["10m", "1D", "s", "us", "15us"]:
            assert mdl.datetime64_dtype_to_timedelta64_dtype(
                np.dtype(f"datetime64[{x}]")
            ) == np.dtype(f"timedelta64[{x}]")
