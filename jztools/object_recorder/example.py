""" This is used to define a class that can be accessed with jztools.py's entity manipulation code."""

from time import time


class MyExampleClass:
    def __init__(self, n):
        self.n = n

    def my_method(self, N):
        return {"range": list(range(self.n, N)), "time": time()}
