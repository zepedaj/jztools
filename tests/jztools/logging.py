from unittest import TestCase
import pytz
import pytest
import logging
from jztools import logging as mdl
import tempfile


class TestBlockingFilter(TestCase):
    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        # https://stackoverflow.com/questions/50373916/pytest-to-insert-caplog-fixture-in-test-method
        self._caplog = caplog

    def test_all(self):
        with tempfile.TemporaryDirectory() as tmpd, self._caplog.at_level(logging.INFO):
            #
            # filename = osp.join(tmpd, 'log.log')
            #
            for filename in [None, "log.log"]:
                for timezone in [None, pytz.timezone("US/Eastern")]:
                    filename = mdl.configure_logging_handler(
                        filename=tmpd,
                        level="INFO",
                        timezone=timezone,
                        log_filter=mdl.AndFilter(
                            mdl.BlockingFilter("blocked", "INFO"),
                            mdl.BlockingFilter(
                                "enabled", "INFO", msg="Blocked message"
                            ),
                        ),
                    )
                    filename = (
                        filename.stream.name
                        if isinstance(filename, logging.FileHandler)
                        else filename
                    )
                    #
                    getattr(self, "assertRegex" if timezone else "assertNotRegex")(
                        str(filename), r".* (EST|EDT)\.log"
                    )

                    #
                    blocked = logging.getLogger("blocked")
                    enabled = logging.getLogger("enabled")
                    #
                    enabled.error("msg")
                    enabled.info("msg")
                    #
                    enabled.info(
                        "msg sldkfj \t\n alskdf \n Blocked message \n laksdf \n"
                    )
                    #
                    blocked.error("msg")
                    blocked.info("msg")
                    #
                    with open(filename, "rt") as log_fo:
                        text = log_fo.read()

                    self.assertRegex(text, "(?ms).*ERROR[^\n]*blocked.*")
                    self.assertNotRegex(text, "(?ms).*INFO[^\n]*blocked.*")
                    #
                    self.assertRegex(text, "(?ms).*ERROR[^\n]*enabled.*")
                    self.assertRegex(text, "(?ms).*INFO[^\n]*enabled.*")
                    self.assertNotRegex(text, "(?ms).*Blocked message.*")
                    #

                    for x in [y for y in text.split("\n") if y]:
                        getattr(self, "assertRegex" if timezone else "assertNotRegex")(
                            x, ".* (EST|EDT) .*"
                        )

    def test_log_exception(self):
        try:
            raise Exception("My exception.")
        except Exception as err:
            logger = logging.getLogger()

            mdl.log_exception(logger, err)
            for record in self._caplog.records:
                assert record.levelname == "ERROR"
                assert "My exception." in record.message
