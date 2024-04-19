from jztools.parallelization.multiprocessing import process_with_info as mdl
from multiprocessing import Queue
from unittest import TestCase


def get_proc_info(q):
    q.put(mdl.ProcessWithInfo.process_info)


class TestProcessWithInfo(TestCase):
    def test_all(self):

        q = Queue()
        pwi = mdl.ProcessWithInfo(
            target=get_proc_info,
            args=(q,),
            process_info=(proc_info := {"str": "My str"}),
        )
        pwi.start()
        self.assertEqual(q.get(), proc_info)
        pwi.join()
