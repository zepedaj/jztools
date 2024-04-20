import os as _os
import os.path as _osp


def mkdir(path, ignore_exists=True, *args, **kwargs):
    try:
        _os.mkdir(path, *args, **kwargs)
    except FileExistsError:
        if not ignore_exists:
            raise
