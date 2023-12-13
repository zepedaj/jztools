from unittest import TestCase
import re
from jztools import validation as mdl


class TestParameters(TestCase):
    def test_choices(self):
        # Function
        @mdl.choices("param", ["val1", "val2"])
        def abc(param):
            """
            abc documentation

            :param param:
            """

            pass

        # Instance method.
        class MyClass:
            @mdl.choices("param", ["val1", "val2"])
            def method(self, param):
                """
                method documentation

                :param param: X documentation.
                """
                pass

        for func in [abc, MyClass().method]:
            # Check doc string.
            self.assertTrue(":param param: ``['val1', 'val2']``" in func.__doc__)

            # Check raises error
            with self.assertRaisesRegex(
                ValueError,
                re.escape("Parameter param=val3 needs to be one of ['val1', 'val2']."),
            ):
                func("val3")

    def test_choices__combinations(self):
        # Function
        @mdl.choices("param", ["val1", "val2"])
        def abc_no_combs(param):
            """
            abc documentation

            :param param:
            """

            return param

        @mdl.choices("param", ["val1", "val2"], multi=True)
        def abc_combs(param):
            """
            abc documentation

            :param param:
            """

            return param

        # Check raises error
        for func, params in [
            (abc_no_combs, ["val1", "val3"]),
            (abc_combs, ["val1", "val3"]),
            (abc_combs, "val3"),
        ]:
            with self.assertRaisesRegex(
                ValueError,
                r"Parameter param\=.*val3.* needs to be one of \['val1', 'val2'\].",
            ):
                func(params)

        assert abc_combs(["val1", "val2"]) == ["val1", "val2"]


class TestFunctions(TestCase):
    def test_checked_get_single(self):
        for args, expected in [
            (((x for x in [0]),), 0),
            ((["first"],), "first"),
            ((["f"], 0, 0), "f"),
            (({"sentence": {"words": ["My"]}}, "sentence", "words", 0), "My"),
        ]:
            self.assertEqual(mdl.checked_get_single(*args), expected)

        for args in [
            ((x for x in [0, 1]),),
            (["first", "second"],),
            (["fa"], 0, 0),
            (
                {"sentence": {"words": ["My"], "phrases": [None]}},
                "sentence",
                "words",
                0,
            ),
        ]:
            with self.assertRaises(mdl.NonSingle):
                mdl.checked_get_single(*args)

    def test_check_get_single__no_item(self):
        assert mdl.checked_get_single([], raise_empty=False) is mdl.NoItem
        assert mdl.checked_get_single((x for x in []), raise_empty=False) is mdl.NoItem
        assert (
            mdl.checked_get_single({"abc": []}, "abc", 0, raise_empty=False)
            is mdl.NoItem
        )
