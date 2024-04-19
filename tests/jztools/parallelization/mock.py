from jztools.parallelization import mock as mdl
from ._parallelizer import _TestParallelizer
from unittest import TestCase


class TestParallelizer(_TestParallelizer, TestCase):
    Parallelizer = mdl.MockParallelizer
