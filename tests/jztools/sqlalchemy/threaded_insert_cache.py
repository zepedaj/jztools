from unittest import TestCase
import warnings
from jztools.parallelization.threading.util import ThreadWorkerError
from jztools.sqlalchemy import threaded_insert_cache as mdl
from sqlalchemy import select, exc
from ._helper import get_database
import random
from pytest import PytestUnhandledThreadExceptionWarning


def random_records(num_recs, num_chars=5):
    a = "abcdefghijklmnopqrstuvwxyz"
    return [
        {"string": "".join([random.choice(a) for _2 in range(num_chars)])}
        for _1 in range(num_recs)
    ]


class TestThreadedInsertCache(TestCase):
    def _get_records(self, engine, table):
        # Check empty table.
        with engine.connect() as connection:
            return connection.execute(select(table)).fetchall()

    def _compare(self, x, y):
        self.assertEqual(len(x), len(y))
        self.assertEqual(
            (list((_x["string"] for _x in x))), (list((_y["string"] for _y in x)))
        )

    def test_all(self):
        with get_database() as (engine, metadata, tables):
            table = tables[0]
            #
            tic = mdl.ThreadedInsertCache(table, engine, queue_size := 1000)

            # Empty flush.
            tic.flush()

            # Check insert single
            self.assertEqual(len(self._get_records(engine, table)), 0)
            rec = random_records(1)
            tic.insert(rec)
            tic.flush()
            all_recs = rec
            self._compare(all_recs, self._get_records(engine, table))

            # Check insert >queue_size.
            recs = random_records(queue_size * 2)
            all_recs.extend(recs)
            tic.insert(recs)
            tic.flush()
            self._compare(all_recs, self._get_records(engine, table))

            # Check insert with error.
            recs = {"string": object}
            all_recs.extend(recs)
            with self.assertRaisesRegex(
                exc.InterfaceError,
                "Error binding parameter 0 \\- probably unsupported type\\.",
            ):
                # TODO: Check the logger error message indicates 1 record was not written.
                tic.insert(recs)
                tic.flush()
