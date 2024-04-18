from jztools import py as mdl


class TestCallerNameTestClassParent:
    def parent_method(self):
        return mdl.get_caller_name()
