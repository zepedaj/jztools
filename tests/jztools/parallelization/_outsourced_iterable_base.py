# from jztools.parallelization.multiprocessing import outsourced_iterable as mdl
import abc


class IterObj:
    def __init__(self, N):
        self.N = N

    def __iter__(self):
        yield from range(self.N)


def lt5(x):
    return x < 5


def times2(x):
    return x * 2


class _TestOutsourcedIterable(abc.ABC):
    @property
    @abc.abstractmethod
    def OutsourcedIterable(self):
        pass

    def test_all(self):
        self.assertEqual(
            list(self.OutsourcedIterable(IterObj(N := 10))), list(IterObj(N))
        )

    def test_filter_mapper_post_filter(self):
        # mapper
        oi = self.OutsourcedIterable(IterObj(N := 10), mapper=times2)
        self.assertEqual(list(oi), list(range(0, 20, 2)))

        # filter
        oi = self.OutsourcedIterable(IterObj(N := 10), filter=lt5)
        self.assertEqual(list(oi), list(range(5)))

        # post_filter
        oi = self.OutsourcedIterable(IterObj(N := 10), mapper=times2, post_filter=lt5)
        self.assertEqual(list(oi), [0, 2, 4])
