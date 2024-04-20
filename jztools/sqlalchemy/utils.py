from contextlib import contextmanager


@contextmanager
def begin_connection(engine, connection=None, commiting=True):
    """
    If a connection is provided, begins from that connection.
    If not, a new connection is created first from the engine.
    """
    if connection is None:
        with (engine.begin() if commiting else engine.connect()) as connection:
            yield connection
    else:
        # with connection.begin():
        yield connection
