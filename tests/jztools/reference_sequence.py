from collections import UserDict
import pytest
from numpy import testing as npt
import numpy as np
from jztools import reference_sequence as mdl


class TestRefSeq:
    def test_slice_scalar(self):
        # Test getter
        arr = np.array(
            [[(0, 1), (2, 3)], [(4, 5), (6, 7)]], dtype=[("fld1", "f"), ("fld2", "f")]
        )
        ssq = mdl.RefSeq()[1, [1]]["fld1"][0]
        assert mdl.getter(ssq, arr) == 6

    def test_str_and_int(self):
        assert mdl.getter(mdl.RefSeq()["abc"], {"abc": 11}) == 11

    def test_slice_array(self):
        # Test getter
        arr = np.array(
            [[(0, 1), (2, 3)], [(4, 5), (6, 7)]], dtype=[("fld1", "f"), ("fld2", "f")]
        )
        npt.assert_equal(mdl.getter(mdl.RefSeq()[:, 1]["fld1"], arr), arr[:, 1]["fld1"])

    def test_no_slice(self):
        # Test getter
        arr = np.array(
            [[(0, 1), (2, 3)], [(4, 5), (6, 7)]], dtype=[("fld1", "f"), ("fld2", "f")]
        )
        ssq = mdl.RefSeq()
        npt.assert_array_equal(mdl.getter(ssq, arr), arr)

    def test_with_slice(self):
        # Test getter
        arr = np.array(
            [[(0, 1), (2, 3)], [(4, 5), (6, 7)]], dtype=[("fld1", "f"), ("fld2", "f")]
        )
        npt.assert_equal(
            mdl.getter(mdl.RefSeq()[:2, 1]["fld1"], arr), arr[:2, 1]["fld1"]
        )

    def test_nested(self):
        data = {"abc": [0, {"def": "abcdef"}]}
        assert mdl.getter(mdl.RefSeq()["abc"][1]["def"][3], data) == "d"

        assert mdl.getter(mdl.RefSeq()["abc"][1]["def"][3], data) == "d"

    def test_isinstance(self):
        class A(UserDict):
            pass

        with pytest.raises(TypeError):
            isinstance(mdl.RefSeq(), A)
        assert not mdl.isinstance_(mdl.RefSeq(), A)
        assert mdl.isinstance_(mdl.RefSeq(), mdl.RefSeq)

    def test_setter(self):
        class A:
            a = "a"
            b = ["b", "c"]

        x = {0: [1, A], 1: 2}
        mdl.setter(mdl.RefSeq()[1], x, 3)
        assert x[1] == 3

        mdl.setter(mdl.RefSeq()[0][1].b[1], x, "d")
        assert x[0][1].b[1] == "d"

    def test_call(self):
        class A:
            a = 1
            b = 2

            def __call__(self, a, b):
                return a + b

        rf = mdl.RefSeq()
        assert mdl.getter(rf(A.a, A.b), A()) == 3

    # def test_set(self):
    #     data = {"abc": [0, {"def": "abcdef"}]}
    #     mdl.RefSeq()["abc"][0](data, "xyz")
    #     self.assertEqual(data["abc"][0], "xyz")

    # def test_copy_constructor(self):
    #     ssq = mdl.RefSeq()["abc"][{"abc": 0, "def": 1}][0][::2][3:100][{"xyz": 2}]
    #     self.assertEqual(ssq, ssq2 := mdl.RefSeq(ssq))
    #     ssq2._ref_seq[1].value["abc"] = 1
    #     self.assertNotEqual(ssq, ssq2)

    # def test_attribute(self):
    #     class A:
    #         str1: str = "abc"
    #         str2: str = "def"
    #         int1: int = 1
    #         dict1: int = {
    #             "key1": "xyz",
    #             "arr": np.array([(-1, -2), (-3, -4)], dtype=[("f0", "i"), ("f1", "i")]),
    #         }
    #         _getitem_dict: dict = {"a": 0, "b": 1, "c": 2}

    #         def __getitem__(self, key):
    #             return self._getitem_dict[key]

    #     a = A()
    #     self.assertEqual(mdl.RefSeq().str1(a), "abc")
    #     self.assertEqual(mdl.RefSeq().str2(a), "def")
    #     self.assertEqual(mdl.RefSeq().dict1["arr"][1]["f0"](a), -3)
