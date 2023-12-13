from importlib import import_module
from jztools import py as mdl
import builtins
from unittest import TestCase
import numpy as np
from numpy import testing as npt
from .py_support import TestCallerNameTestClassParent


# HELPER FUNCTIONS for test_get_caller_name
def caller_name_test_function():
    return mdl.get_caller_name()


class ABC:
    def abc(self):
        pass


class TestCallerNameTestClass(TestCallerNameTestClassParent):
    def __call__(self):
        return mdl.get_caller_name()

    def method(self):
        return mdl.get_caller_name()

    def test_get_caller_name(self):
        assert (
            TestCallerNameTestClass().method()
            == "tests.jztools.py.TestCallerNameTestClass.method"
        )
        assert (
            TestCallerNameTestClass()()
            == "tests.jztools.py.TestCallerNameTestClass.__call__"
        )
        assert (
            caller_name_test_function() == "tests.jztools.py.caller_name_test_function"
        )

    def test_get_caller_name_derived(self):
        assert (
            self.parent_method()
            == "tests.jztools.py.TestCallerNameTestClass.parent_method"
        )


# class TestStoppableThread(TestCase):
#     def test_smoke(self):
#         #
#         def infinite():
#             while True:
#                 pass

#         t1 = mdl.StoppableThread(target=infinite)
#         t1.start()
#         t1.join(timeout=0.1)
#         #
#         self.assertTrue(t1.is_alive())
#         t1.timeout(0.1)
#         # t1.stop()
#         # t1.join()
#         self.assertFalse(t1.is_alive())
#         #


class TestFunctions(TestCase):
    def test_get_caller_name(self):
        self.assertEqual(
            TestCallerNameTestClass().method(),
            "tests.jztools.py.TestCallerNameTestClass.method",
        )
        self.assertEqual(
            TestCallerNameTestClass()(),
            "tests.jztools.py.TestCallerNameTestClass.__call__",
        )
        self.assertEqual(
            caller_name_test_function(), "tests.jztools.py.caller_name_test_function"
        )

    def test_get_caller(self):
        # Method
        class A:
            def my_meth(self):
                func = mdl.get_caller()
                return func

            def another_meth(self):
                pass

        a = A()
        assert a.my_meth() == a.my_meth
        assert a.my_meth != a.another_meth

        # Function
        def my_func():
            return mdl.get_caller()

        out = my_func()
        assert out is my_func

    def test_strict_zip(self):
        #
        list(mdl.strict_zip([1, 2, 3], [4, 5, 6]))
        with self.assertRaises(mdl.StrictZipException):
            list(mdl.strict_zip([1, 2, 3], [4, 5]))
        #
        list(mdl.strict_zip([1, 2, None], [4, 5, 6]))

        list(mdl.strict_zip(*[]))

        self.assertEqual(list(mdl.strict_zip([], [])), [])

    def test_get_nested_keys(self):
        d = {
            "d.0": {
                "d.0.0": {
                    "l.0.0.0": [0, 1, None, 3],
                    "l.0.0.1": [0, 1, 2],
                    "l.0.0.2": [None, 1, 2],
                    "l.0.0.3": {
                        "d.0.0.3.0": [0, 0, 0],
                        "d.0.0.3.1": [None, 0, 1],
                        "d.0.0.3.2": None,
                    },
                }
            },
            "d.1": [None, 5, 6, 7],
            "d.2": 1,
            "d.3": None,
        }

        keys = mdl.get_nested_keys(d, lambda x: x is None)
        self.assertEqual(
            keys,
            [
                ["d.0", "d.0.0", "l.0.0.0", 2],
                ["d.0", "d.0.0", "l.0.0.2", 0],
                ["d.0", "d.0.0", "l.0.0.3", "d.0.0.3.1", 0],
                ["d.0", "d.0.0", "l.0.0.3", "d.0.0.3.2"],
                ["d.1", 0],
                ["d.3"],
            ],
        )

    def test_class_names(self):
        for _cls in [dict, set, list, type(self), np.ndarray]:
            for kwargs in [{"stripped_modules": []}, {"stripped_modules": [builtins]}]:
                self.assertEqual(
                    mdl.class_from_name(mdl.class_name(_cls, **kwargs), **kwargs), _cls
                )

        self.assertEqual(mdl.class_name(dict), "builtins.dict")
        self.assertEqual(mdl.class_name(dict, stripped_modules=[]), "builtins.dict")
        self.assertEqual(mdl.class_name(dict, stripped_modules=[builtins]), "dict")
        self.assertEqual(mdl.class_name(list, stripped_modules=[builtins]), "list")

    def test_entity_names(self):
        class ABClocal:
            def abc(self):
                pass

        # Some class
        for _cls in [dict, set, list, type(self), np.ndarray, ABC.abc]:
            self.assertEqual(mdl.entity_from_name(mdl.entity_name(_cls)), _cls)

        # Some class methods
        for entity in [ABC(), ABC().abc]:
            with self.assertRaisesRegex(
                Exception, "^Cannot extract an entity name from.*"
            ):
                out = mdl.entity_name(entity)

        for entity in [ABClocal]:
            name = mdl.entity_name(entity)
            with self.assertRaisesRegex(Exception, "^Invalid entity name"):
                out = mdl.entity_from_name(name)

    def test_entity_parent(self):
        assert mdl.parent_entity(ABC).__name__ == __name__  # Module from class
        assert (
            mdl.parent_entity(import_module(__name__)).__name__ == "tests.jztools"
        )  # Module from module
        assert mdl.parent_entity(ABC.abc) == ABC  # Class from method


class TestReadableMultiline(TestCase):
    def test_all(self):
        text = "alkjwe!@#$\n`;lkàäê\nî"
        self.assertTrue("\n" in text)
        self.assertNotEqual(encoded := mdl.ReadableMultiline.encode(text), text)
        self.assertTrue("\n" not in encoded)
        self.assertEqual(mdl.ReadableMultiline.decode(encoded), text)
