from unittest import TestCase
from numpy import testing as npt
import numpy as np
from jztools import reference_sequence_v0 as mdl
from xerializer import Serializer
from dataclasses import dataclass


class TestRefSeq(TestCase):
    def test_slice_scalar(self):
        # Test apply
        arr = np.array(
            [[(0, 1), (2, 3)], [(4, 5), (6, 7)]], dtype=[("fld1", "f"), ("fld2", "f")]
        )
        ssq = mdl.RefSeq()[1, [1]]["fld1"][0]
        self.assertEqual(ssq(arr), 6)

        # Test serialization.
        srlzr = Serializer()
        serialized = srlzr.serialize(ssq)
        self.assertIsInstance(serialized, str)

        des_ssq = srlzr.deserialize(serialized)
        self.assertEqual(des_ssq(arr), 6)

    def test_str_and_int(self):
        self.assertEqual(mdl.RefSeq()["abc"]({"abc": 11}), 11)

    def test_slice_array(self):
        # Test apply
        arr = np.array(
            [[(0, 1), (2, 3)], [(4, 5), (6, 7)]], dtype=[("fld1", "f"), ("fld2", "f")]
        )
        ssq = mdl.RefSeq()[:, 1]["fld1"]
        npt.assert_equal(ssq(arr), arr[:, 1]["fld1"])

        # Test serialization.
        srlzr = Serializer()
        serialized = srlzr.serialize(ssq)
        self.assertIsInstance(serialized, str)

        des_ssq = srlzr.deserialize(serialized)
        npt.assert_equal(des_ssq(arr), arr[:, 1]["fld1"])

    def test_no_slice(self):
        # Test apply
        arr = np.array(
            [[(0, 1), (2, 3)], [(4, 5), (6, 7)]], dtype=[("fld1", "f"), ("fld2", "f")]
        )
        ssq = mdl.RefSeq()
        npt.assert_equal(ssq(arr), arr)

        # Test serialization.
        srlzr = Serializer()
        serialized = srlzr.serialize(ssq)
        self.assertIsInstance(serialized, str)

        des_ssq = srlzr.deserialize(serialized)
        npt.assert_equal(des_ssq(arr), arr)

    def test_with_slice(self):
        # Test apply
        arr = np.array(
            [[(0, 1), (2, 3)], [(4, 5), (6, 7)]], dtype=[("fld1", "f"), ("fld2", "f")]
        )
        ssq = mdl.RefSeq()[:2, 1]["fld1"]
        npt.assert_equal(ssq(arr), arr[:2, 1]["fld1"])

        # Test serialization.
        srlzr = Serializer()
        serialized = srlzr.serialize(ssq)
        self.assertIsInstance(serialized, str)

        des_ssq = srlzr.deserialize(serialized)
        npt.assert_equal(des_ssq(arr), arr[:2, 1]["fld1"])

    def test_nested(self):
        data = {"abc": [0, {"def": "abcdef"}]}
        self.assertEqual(mdl.RefSeq()["abc"][1]["def"][3](data), "d")
        self.assertEqual(mdl.RefSeq([mdl.k_("abc"), 1, mdl.k_("def"), 3])(data), "d")
        self.assertEqual(mdl.RefSeq(["[abc]", 1, "[def]", 3])(data), "d")

    def test_set(self):
        data = {"abc": [0, {"def": "abcdef"}]}
        mdl.RefSeq()["abc"][0](data, "xyz")
        self.assertEqual(data["abc"][0], "xyz")

    def test_copy_constructor(self):
        ssq = mdl.RefSeq()["abc"][{"abc": 0, "def": 1}][0][::2][3:100][{"xyz": 2}]
        self.assertEqual(ssq, ssq2 := mdl.RefSeq(ssq))
        ssq2._ref_seq[1].value["abc"] = 1
        self.assertNotEqual(ssq, ssq2)

    def test_attribute(self):
        class A:
            str1: str = "abc"
            str2: str = "def"
            int1: int = 1
            dict1: int = {
                "key1": "xyz",
                "arr": np.array([(-1, -2), (-3, -4)], dtype=[("f0", "i"), ("f1", "i")]),
            }
            _getitem_dict: dict = {"a": 0, "b": 1, "c": 2}

            def __getitem__(self, key):
                return self._getitem_dict[key]

        a = A()
        self.assertEqual(mdl.RefSeq().str1(a), "abc")
        self.assertEqual(mdl.RefSeq().str2(a), "def")
        self.assertEqual(mdl.RefSeq().dict1["arr"][1]["f0"](a), -3)
