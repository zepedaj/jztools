from unittest import TestCase
import jztools.json as mdl


class TestFunctions(TestCase):
    def test_as_serializable(self):
        val = {"abc": 1, "def": 2}
        self.assertEqual(mdl.as_serializable(val), val)

        self.assertEqual(mdl.as_serializable(2), 2)
        self.assertEqual(mdl.as_serializable("abc"), "abc")
        self.assertEqual(mdl.as_serializable((1, 2, 3)), [1, 2, 3])

        val.update({"val": dict(val)})
        val["val"].update({"val": dict(val)})
        self.assertEqual(mdl.as_serializable(val), val)
