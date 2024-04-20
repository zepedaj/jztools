import time
import threading


class RatePacer:
    """
    Ensures a minimum period before the consecutive entries into the context manager in any thread sharing the pacer. The actual period will be slightly lower. True periods are guaranteed to be higher than the nominal period, with precision around 5% of nominal period for periods=10ms. Higher period result in better precision.

    .. code-block::

        import time, threading

        rate = 100  # 10 ms
        times = []

        def run(th_id, pacer, iters=10):
            for k in range(iters):
                with pacer:
                    times.append((th_id, time.time()))
        #
        pacer = RatePacer(rate)
        threads = [threading.Thread(target=run, args=(th_id, pacer)) for th_id in range(10)]
        [_t.start() for _t in threads]
        [_t.join() for _t in threads]
        #
        times.sort(key=lambda x: x[1])
        periods = np.diff(np.array(times)[:, 1])


    """

    def __init__(self, rate):
        """
        :param period: Minimum time between last exit and next enter.
        :param timeout: Timeout after this many seconds when waiting to enter the pacer.
        """
        self.last_call_time = float("-inf")
        self.period = 1.0 / rate
        self.lock = threading.Lock()

    def time_left(self):
        return max(self.period - (time.time() - self.last_call_time), 0)

    def __enter__(self):
        #
        with self.lock:
            while True:
                time_left = self.time_left()
                if time_left == 0:
                    break
                time.sleep(time_left)
            self.last_call_time = time.time()

    def __exit__(self, *args):
        pass


class ConcurrencyPacer:
    """
    Ensures a maximum number of calls are running simultaneously.
    """

    def __init__(self, num: int, timeout=-1):
        """
        :param num: Max number of simultaneous calls.
        :param timeout: Timeout after this many seconds when waiting to enter the pacer.
        """
        self.current_calls = 0
        self.max_calls = num
        self.timeout = timeout
        self.cv = threading.Condition()

    def __enter__(self):
        with self.cv:
            self.cv.wait_for(lambda: self.current_calls < self.max_calls)
            self.current_calls += 1

    def __exit__(self, *args):
        with self.cv:
            self.current_calls -= 1
            self.cv.notify()
