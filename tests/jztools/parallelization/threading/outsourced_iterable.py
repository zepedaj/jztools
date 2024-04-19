#
from tests.jztools.parallelization import _outsourced_iterable_base as _mdl
from jztools.parallelization.threading import outsourced_iterable as mdl
from unittest import TestCase


class TestOutsourcedIterable(_mdl._TestOutsourcedIterable, TestCase):
    OutsourcedIterable = mdl.ThreadOutsourcedIterable
