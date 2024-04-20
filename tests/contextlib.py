from dataclasses import dataclass
from uuid import UUID, uuid1, uuid4
from jztools import contextlib as mdl


class _Context:
    def __enter__(self):
        self.uuid = uuid4()
        return self.uuid

    def __exit__(self, *args, **kwargs):
        self.uuid = None


def _fxn_context():
    uuid = uuid4()
    yield uuid


@dataclass
class Obj:
    a: int
    b: int


class TestReentrantContext:
    def _test_wrapped(self, wrapped):
        ctx = wrapped()
        with ctx as id0:
            with ctx as id1:
                with ctx as id2:
                    assert id0 == id1 == id2 and isinstance(id0, UUID)

    def test_wrap_class(self, wrapped=_Context):
        ctx = mdl.reentrant_context_manager(_Context)
        assert ctx is _Context
        self._test_wrapped(ctx)

    def test_wrap_function(self):
        self._test_wrapped(mdl.reentrant_context_manager(_fxn_context))

    def test_args(self):
        @mdl.reentrant_context_manager
        def gen(a, b):
            yield Obj(a, b)

        with gen(1, 2) as x, gen(3, 4) as y:
            assert x is y
            assert y.a == 1 and y.b == 2
