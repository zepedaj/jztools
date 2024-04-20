from concurrent.futures import ThreadPoolExecutor
import random
from time import sleep

from jztools.contextlib import environ
from .recording_switch_utils import factory
from jztools.object_recorder.call_unordered import call_unordered


def worker(k):
    sleep(random.random())
    return k * 2


class TestUnordered:
    def test_all(self):
        N = 10
        with factory() as (_, recording_switch, temp_dir):
            for rec_mode in ["RECORD", "PLAYBACK"]:
                with recording_switch(
                    call_unordered(
                        ("tests.jztools.object_recorder.call_unordered", "worker")
                    ),
                    rec_mode=rec_mode,
                ) as rec_switch:
                    with ThreadPoolExecutor(max_workers=N) as pool:
                        futures = [pool.submit(worker, k) for k in range(N)]
                    assert (actual := [f.result() for f in futures]) == list(
                        range(0, 2 * N, 2)
                    )
