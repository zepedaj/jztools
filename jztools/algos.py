import bisect
from typing import Callable


class OrderedHeap(object):
    def __init__(self, L=None, key: Callable = lambda x: x):
        self._L = L if L is not None else []
        self._scores = [key(_l) for _l in self._L]
        self.key = key

    def insert(self, obj):
        score = self.key(obj)
        posn = bisect.bisect(self._scores, score)
        self._scores.insert(posn, score)
        self._L.insert(posn, obj)

    def __iter__(self):
        return iter(self._L)

    def __len__(self):
        return len(self._L)


class MinHeap(OrderedHeap):
    def __init__(self, L=None, key=lambda x: x, N=None):
        self.N = N
        super().__init__(L=L, key=key)

    def insert(self, obj):
        super().insert(obj)
        self.prune()

    def prune(self):
        if self.N is not None:
            self._L = self._L[: self.N]
            self._scores = self._scores[: self.N]


class MaxHeap(MinHeap):
    def prune(self):
        if self.N is not None:
            self._L = self._L[max(len(self._L) - self.N, 0) :]
            self._scores = self._scores[max(len(self._scores) - self.N, 0) :]

    def __iter__(self):
        return iter(self._L[::-1])
