from unittest import TestCase
from numpy import testing as npt
import numpy as np
from jztools import slice_sequence as mdl
from xerializer import Serializer


class TestSliceSequence(TestCase):
    def test_slice_scalar(self):
        # Test apply
        arr = np.array(
            [[(0, 1), (2, 3)], [(4, 5), (6, 7)]], dtype=[("fld1", "f"), ("fld2", "f")]
        )
        ssq = mdl.SliceSequence()[1, [1]]["fld1"][0]
        self.assertEqual(ssq(arr), 6)

        # Test serialization.
        srlzr = Serializer()
        serialized = srlzr.serialize(ssq)
        self.assertIsInstance(serialized, str)

        des_ssq = srlzr.deserialize(serialized)
        self.assertEqual(des_ssq(arr), 6)

    def test_str_and_int(self):
        self.assertEqual(mdl.SliceSequence()["abc"]({"abc": 11}), 11)

    def test_slice_array(self):
        # Test apply
        arr = np.array(
            [[(0, 1), (2, 3)], [(4, 5), (6, 7)]], dtype=[("fld1", "f"), ("fld2", "f")]
        )
        ssq = mdl.SliceSequence()[:, 1]["fld1"]
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
        ssq = mdl.SliceSequence()
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
        ssq = mdl.SliceSequence()[:2, 1]["fld1"]
        npt.assert_equal(ssq(arr), arr[:2, 1]["fld1"])

        # Test serialization.
        srlzr = Serializer()
        serialized = srlzr.serialize(ssq)
        self.assertIsInstance(serialized, str)

        des_ssq = srlzr.deserialize(serialized)
        npt.assert_equal(des_ssq(arr), arr[:2, 1]["fld1"])

    def test_nested(self):
        data = {"abc": [0, {"def": "abcdef"}]}
        self.assertEqual(mdl.SliceSequence()["abc"][1]["def"][3](data), "d")
        self.assertEqual(mdl.SliceSequence.produce(["abc", 1, "def", 3])(data), "d")

    def test_set(self):
        data = {"abc": [0, {"def": "abcdef"}]}
        mdl.SliceSequence()["abc"][0].set(data, "xyz")
        self.assertEqual(data["abc"][0], "xyz")

    def test_copy(self):
        ssq = mdl.SliceSequence()["abc"][{"abc": 0, "def": 1}][0][::2][3:100][
            {"xyz": 2}
        ]
        self.assertEqual(ssq, ssq2 := ssq.copy())
        ssq2.slice_sequence[1]["abc"] = 1
        self.assertNotEqual(ssq, ssq2)

    # def test_hash(self):
    #     ssq = mdl.SliceSequence()['abc'][{'abc': 0, 'def': 1}][0][::2][3:100][{'xyz': 2}]
    #     self.assertEqual({ssq: 0, ssq: 1}, {ssq: 1})  # noqa - Same keys part of test for values 0, 1
