from jztools.nnets.torch import profiling as mdl
import torch


class TestCUDATime:
    def test_all(self):

        tensor = torch.ones(
            1000
        )  # optionally, specify device, e.g., ``device='cuda'``.
        with mdl.CUDATime() as t:
            out = tensor + tensor

        # Elapsed time in seconds:
        t0 = t.elapsed
        assert isinstance(t0, float)
        assert t0 >= 0
