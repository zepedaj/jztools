import os
import os.path as osp
import tempfile
import shutil
from contextlib import contextmanager, ExitStack

# Exceptions


class RenTempExistsException(Exception):
    pass


class RenTempFileExists(RenTempExistsException):
    pass


class RenTempDirExists(RenTempExistsException):
    def __init__(self, when, *args, **kwargs):
        self.when = when
        super().__init__(*args, **kwargs)


# Skip file exists exceptions.


@contextmanager
def skip_exists():
    try:
        yield None
    except RenTempExistsException as err:
        pass


# Managers


@contextmanager
def RenTempFile(target, delete=True, overwrite=False, partial=False, **file_kwargs):
    """
    Ensures the file is consistent (e.g., no intermediate states / partial writes) by first writing to a temporary file and then moving to the final destination.

    If two threads target the same destination simultaneously, there is no guarantee of which one wins.

    Might leave a residual temporary file (option ``delete=True`` attempts to clean it up).
    """
    temp_fo = None
    completed = False
    try:
        if osp.isfile(target) and not overwrite:
            raise RenTempFileExists(target)
        path, name = osp.split(osp.abspath(target))
        temp_fo = tempfile.NamedTemporaryFile(
            prefix=name + ".", suffix=".rentemp", dir=path, delete=False, **file_kwargs
        )
        yield temp_fo
        completed = True
    finally:
        #
        if temp_fo is not None:
            temp_fo.close()
            if completed or partial:
                shutil.move(temp_fo.name, target)
            elif delete:
                os.remove(temp_fo.name)


@contextmanager
def RenTempFiles(targets, *args, **kwargs):
    # TODO: Possibility of corrupt data if a failure happens during the final move operation
    # in RenTempFiles' __exit__ method. At least catch this and print an exception.
    with ExitStack() as stack:
        yield [
            stack.enter_context(RenTempFile(_target, *args, **kwargs))
            for _target in targets
        ]


@contextmanager
def RenTempDir(target, delete=True, fail=True):
    """
    Generates the target directory with a temporary name, and attempts to move (rename) it to the target name on exit. If the target directory already existed, raises an :exc:`RenTempDirExists` exception, with :attr:`when` attribute indicating whether the exception was raised before or after yielding. Note that directory renaming can succeed when the target directory is.

    :param target: Target directory name to create on exit.
    :param delete: If an error occurs and a temporary target directory was created, delete it.
    :param fail: If ``True`` (the default) and the target directory already existed, raise :exc:`RenTempDirExists`. If ``False``, return the target directory.
    """

    if osp.isdir(target):
        if fail:
            raise RenTempDirExists("start", f"Directory {target} exists.")
        else:
            yield target
    else:
        path, name = osp.split(osp.abspath(target))
        temp_dir = tempfile.mkdtemp(prefix=name + ".", suffix=".rentemp", dir=path)
        try:
            yield temp_dir
            try:
                os.rename(temp_dir, target)
            except OSError as err:
                if (
                    err.errno == 39
                    and err.filename == temp_dir
                    and err.filename2 == target
                ):
                    raise RenTempDirExists("end", f"Directory {target} exists.")
                else:
                    raise

        finally:
            if osp.isdir(temp_dir) and delete:
                shutil.rmtree(temp_dir)
