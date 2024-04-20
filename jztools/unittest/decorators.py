from unittest.case import skip
import torch


def requires_gpu():
    """
    Unit test decorator used to skip tests that require a GPU when none is available.
    """

    def _requires_gpu(func):
        if not torch.cuda.is_available():
            return skip(f"No GPU device available.")(func)
        return func

    return _requires_gpu
