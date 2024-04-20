import numbers
import numpy as np
import torch


def copy(X):
    if isinstance(X, torch.Tensor):
        X = X.clone()
    elif isinstance(X, np.ndarray):
        X = X.copy()
    elif isinstance(X, numbers.Number):
        pass
    else:
        raise Exception("Invalid input arg.")
    return X


def asnumpy(X):
    if isinstance(X, torch.Tensor):
        X = X.detach().cpu().numpy()
    X = np.require(X)
    return X


def concatenate(L, default=None):
    if len(L) == 0:
        return default
    else:
        if isinstance(L[0], torch.Tensor):
            return torch.cat(L)
        elif isinstance(L[0], np.ndarray):
            return np.concatenate(L)
        else:
            raise Exception("Invalid inputs.")


cat = concatenate


def detach(X):
    #
    if isinstance(X, torch.Tensor):
        out = X.detach()
    else:
        out = X
    #
    return out


def cpu(X):
    #
    if isinstance(X, torch.Tensor):
        out = X.cpu()
    else:
        out = X
    #
    return out
