import threading


class ThreadSafeWriter:
    def __init__(self, filename):
        self.filename = filename
        self.lock = threading.Lock()

    def write(self, str_val, mode="at"):
        with self.lock:
            with open(self.filename, mode) as fo:
                fo.write(str_val)

    def read(self, *arg, mode="rt"):
        assert len(arg) in [0, 1], "Invalid input args."

        with self.lock:
            try:
                with open(self.filename, mode) as fo:
                    return fo.read()
            except FileNotFoundError:
                if len(arg) == 1:
                    return arg[0]
                raise
