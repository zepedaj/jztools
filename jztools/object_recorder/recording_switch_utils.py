"""

Repeatable/Offline Testing
================================

.. |factory_info| replace:: The provided helper function :func:`factory` makes it possible to create new :func:`live_test` and :class:`recording_switch` with changed default recordings root directory and/or default recording mode environment variable.

This module enables repeatable and offline (e.g., without a live connection to a remote service) testing using object recordings. The main mechanism for this is context manager :class:`recording_switch`, which returns an object that can be used in place of live objects.

Recorded and Played Back Objects
------------------------------------------

Depending on the options of environment variable :const:`REC_MODE`, :class:`recording_switch` will return a live, recorded, or offline played-back object with the same interface as the live object:

* **Live objects** are standard objects of any type.

* **Recorded objects** are live objects that are monkey-patched with :class:`ObjectRecorder` to record all live transactions. This type of object is used during unit test development to create JSON files with recordings of transactions.

* **Played back** objects are of type :class:`ObjectPlayer` and play back recorded transactions assuming calls occur **in the same order as when they were recorded**. They ensure repeatability and enable offline testing.

Recording Files
---------------------

Transaction recordings are stored as JSON files and can be treated as source code managed with a repository. The default recording storage location is derived from the path to the source file where :class:`recording_switch` was called.

.. code-block::

  '<source_directory>/_recordings/<source module as dot-separate string>.json'

For example, for a test named as follows (using ``pytest`` convention)

.. code-block::

  'tests.excalibur.synthesizers::Sword::test_synthesize_sword'

will have, by default, a recording file at the following path:

.. code-block::

  'tests/excalibur/synthesizes/_recordings/tests.excalibur.synthesizers.Sword.test_synthesize_sword.json'

.. note:: |factory_info|

Live TWS Test Decorator
-------------------------

Some tests can only execute with a live object or connection. The :func:`live_test` test decorator can be used to designate such tests. Tests thus decorated will only execute when REC_MODE is one of ``'LIVE', 'FORCE_LIVE'``.

.. Rubric:: Example

.. code-block::

    from object_recorder import live_test

    @live_test
    def test_with_connection():
        ...

.. _REC_MODE :

Recording Modes
---------------

The way in which :class:`recording_switch` and :func:`live_test` behave depends on the recording mode. By default, the recording mode is specified by environment variable ``REC_MODE``,
which can take one of the following values:

.. code-block:: bash

   REC_MODE=<'PLAYBACK'|'FORCE_LIVE'|'RECORD'|'LIVE'>


The following table details how the recording mode will affect the behaviors of :func:`live_test` and :class:`recording_switch` and whether the latter will instantiate a live object internally:

.. csv-table::
    :widths: 8, 8, 10, 10
    :header: "Recording mode","Live Object Instantiated", ":func:`@live_test <live_test>` tests", ":func:`recording_switch`"

    "``PLAYBACK`` *(default)*", "No", "Skip", "Returns an object player of type :class:`~jztools.object_recorder.object_recorder.ObjectPlayer` when the recordings file exists or raises an error otherwise."
    "``LIVE``", "**Yes**", "**Run**", "Same as ``PLAYBACK``."
    "``FORCE_LIVE``", "**Yes**", "**Run**", "Returns a live instantiated object."
    "``RECORD``", "**Yes**", "Skip", "Same as ``PLAYBACK`` for existing recordings. For non-existing recordings, instantiates a live object and returns it wrapped in a :class:`~jztools.object_recorder.object_recorder.ObjectRecorder`."
    "``OVERWRITE``", "**Yes**", "Skip", "Same as ``RECORD`` but overwrites existing recordings."

.. note:: |factory_info|

Members
--------
"""

# Create a live test.
from contextlib import ExitStack
from enum import Enum
from .utils import utc_now
from jztools.datetime64 import FlexDateTime, as_naive_utc
from jztools.object_recorder.rec_obj_factory import RecObjFactorySpec, flex_create
from jztools.object_recorder.replicate import replicate
from jztools.py import get_caller_name, strict_zip

import inspect
from functools import partial, wraps
from typing import Any, List, Literal, Optional, Sequence, Tuple, Union
from jztools.rentemp import RenTempFile
from jztools.validation import check_option, confirm_option
from unittest.case import skip
import os
import pytest

from xerializer.serializer import Serializer
from .object_recorder import ObjectPlayer, base_get, with_vanilla_getattribute
import os.path as osp
import logging
from pathlib import Path

from .global_time import GLOBAL_TIME

global LOGGER
LOGGER = logging.getLogger(__name__)

DFLT_ENV_VAR = "REC_MODE"
"""
The default name for the env var returning the recording source.
"""

SUB_DIR = "_recordings"
"""
The sub-directory where automatically-named recordings are saved. See :func:`get_default_root`.
"""

RecMode = Enum("RecMode", ["PLAYBACK", "LIVE", "FORCE_LIVE", "RECORD", "OVERWRITE"])
""" Recording modes that users can specify. """

EffectiveRecMode = Literal[RecMode.PLAYBACK, RecMode.FORCE_LIVE, RecMode.OVERWRITE]
""" Effective recording modes. See :meth:`recording_switch.effective_rec_mode` for a mapping between :attr:`EffectiveRecMode` and :class:`RecMode`. """

APPROVED_OVERWRITE_ENV_VAR_NAMES = set()
""" When users select the :attr:`RecMode.OVERWRITE` mode, a CLI query is printed requesting approval. The approval is saved in this variable for the specified environment variable for all future calls."""


class RecordingFileNotFoundError(FileNotFoundError):
    pass


def get_rec_mode(env_var_name):
    """
    Retrieves the value of the environment variable of the specified name (the default is specified in :attr:`DFLT_ENV_VAR`).

    """

    rec_mode = (
        confirm_option(
            env_var_name,
            check_option(
                env_var_name,
                os.getenv(env_var_name),
                list(x.name for x in RecMode),
                ignore_list=[None, ""],
            ),
            (
                ["OVERWRITE"]
                if env_var_name not in APPROVED_OVERWRITE_ENV_VAR_NAMES
                else []
            ),
        )
        or "PLAYBACK"
    )

    if rec_mode == "OVERWRITE":
        APPROVED_OVERWRITE_ENV_VAR_NAMES.add(env_var_name)

    return RecMode[rec_mode]


def _get_default_root(stack_entry):
    """
    Returns a default root directory given by the sub-directory `_recordings` within the calling module's directory. This is meant to save
    recordings in a sub-directory next to unit-test source files.

    For derived classes where a method is implemented in the parent, returns the path for the child class file.
    """
    # Get caller module

    frame = stack_entry.frame

    if "self" in frame.f_locals:
        cls = frame.f_locals["self"].__class__
        module = inspect.getmodule(cls)
    else:
        module = inspect.getmodule(frame)

    out = str(Path(module.__file__).parent / SUB_DIR)

    return out


def get_default_filename(root=None, caller_skip_levels=2, suffix="", mk_root=True):
    """
    Returns the default filename used in :func:`recording_switch` when called form the same context.

    :param caller_skip_levels: Defaults to 2. Will return the same filename as used by default in :func:`recording_switch` if called from the same function as :func:`recording_switch`. Increase if called from deeper in the stack.
    :param suffix: When auto-deriving a filename, append this suffix.
    """

    fullstack = inspect.stack()
    stack_entry = fullstack[caller_skip_levels - 1]

    root = root or _get_default_root(stack_entry)
    if mk_root and not osp.isdir(root):
        os.mkdir(root)
    name = get_caller_name(stack_entry=stack_entry)
    filename = osp.join(root, name + suffix + ".json")
    return filename


class recording_switch:
    """

    Context manager that records live TWS transactions for offline playback. Depending on the settings of :const:`rec_mode` (see `REC_MODE`_) this context manager will return a live (possibly recorded) SyncIBAPI object, or a playback PlayedBackSyncIBAPI object.

    .. warning:: This function expects  **deterministic** (e.g., non-threaded, non-randomized) **execution** so that requests are made in the same order as when the recording was made. As an example, calls to :meth:`jzf_interactive_brokers.history.history.History.update` would need to have ``num_threads=1`` and ``shuffle=False`` (further setting ``do_raise=True`` will produce error messages useful for debugging).

    If no **filename** is provided, this context manager first assembles a file name with parent directory **root** and name derived from the calling test function. When using this functionality, a **single** :meth:`recording_switch` **call should be made in that function** for a given **suffix**.

    .. Rubric:: Parameters

    This function has the same signature as class SyncIBAPI and returns an object of that same class or the derived class PlayedBackSyncIBAPI. Besides these parameters, it also has the following parameters:

    :param root: The root where automatically-generated filenames are stored. The default position is sub-directory :attr:`SUB_DIR` in the calling module's root folder.
    :param obj_factories: A callable (or tuple/list thereof) that produces the object(s) to record. The callable will only be called if the live object is required (e.g., when forcing live or when the recording does not exist).
    :param rec_mode: The recording source. See `REC_MODE`_ .
    :param filename: Filename to use (ignore _caller_skip_levels and suffix.
    :param suffix: When auto-deriving a filename, append this suffix
    :param _caller_skip_levels: When auto-deriving a filename, derive it based on the caller this many levels up.

    .. Rubric:: Example

    .. code-block::

        def test_placeOrder(self):
            with recording_switch(host='127.0.0.1', port=7497, clientId=0, suffix='') as async_ib:
                # Can use async_ib as a async_ib.AsyncIB object.
                responses = async_ib.placeOrder(...)

    """

    rec_mode: RecMode
    _warned_recording = False

    def __init__(
        self,
        *obj_factories: RecObjFactorySpec,
        rec_mode: Optional[Union[str, RecMode]] = None,
        root: Optional[Union[str, Path]] = None,
        filename: Optional[Union[str, Path]] = "",
        suffix: str = "",
        serializer: Optional[Serializer] = None,
        rec_mode_env_var_name=DFLT_ENV_VAR,
        warp_time: bool = True,
        _mk_root=True,
        _caller_skip_levels: int = 3,
    ):
        if not (obj_factories):
            # TODO: This hack avoids bug when no args -- fix the bug instead.
            obj_factories = (replicate(None),)

        self.obj_factories = [flex_create(x) for x in obj_factories]
        self.rec_mode = (
            spec
            if isinstance(
                spec := (rec_mode or get_rec_mode(rec_mode_env_var_name)), RecMode
            )
            else RecMode[spec]
        )
        self.rec_mode_env_var_name = rec_mode_env_var_name
        self.warp_time = warp_time
        self.serializer = serializer or Serializer()
        # Get filename
        self.filename = Path(
            filename
            or get_default_filename(
                caller_skip_levels=_caller_skip_levels,
                suffix=suffix,
                root=root,
                mk_root=_mk_root,
            )
        )
        self._exit_stack = None
        self._start_time = None
        self._recorded_components = None
        self._loaded_components = None
        self._entered_rec_mode = None

    @property
    def effective_rec_mode(self) -> EffectiveRecMode:
        """Returns an effective recording mode given the user-specified recording mode."""
        if self.rec_mode == RecMode.RECORD:
            if not osp.isfile(self.filename):
                return RecMode.OVERWRITE
            else:
                return RecMode.PLAYBACK
        elif self.rec_mode == RecMode.LIVE:
            # Note that 'LIVE' means that tests annotated with @live_test will not be skipped.
            # But any `recording_switch` context will continue to behave like a
            # recorded test even when REC_MODE='LIVE'.
            return RecMode.PLAYBACK
        elif self.rec_mode in [
            RecMode.FORCE_LIVE,
            RecMode.PLAYBACK,
            RecMode.OVERWRITE,
        ]:
            return self.rec_mode
        else:
            raise ValueError(f"Invalid rec_mode {self.rec_mode}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        global GLOBAL_TIME
        try:
            if exc_type is None and self.effective_rec_mode == RecMode.OVERWRITE:
                self._save(indent=0, separators=(",", ":"))
        finally:
            if self._exit_stack is not None:
                self._exit_stack.__exit__(exc_type, exc_val, exc_tb)
            self._exit_stack = None
            self._recorded_components = None
            self._loaded_components = None
            self._entered_rec_mode = None

    def _deref(self, x: Sequence):
        return x[0] if len(x) == 1 else x

    def _build_exit_stack(self, recorded_components):
        self._exit_stack = self._exit_stack or ExitStack()
        self._exit_stack.__enter__()
        try:
            self._extend_exit_stack(recorded_components)
        except Exception as err:
            self.__exit__(type(err), err, err.__traceback__)
            raise

    def _extend_exit_stack(self, recorded_components):
        [
            self._exit_stack.enter_context(base_get(_rc, "as_context_manager")())
            for _subset in recorded_components
            for _rc in _subset
        ]

    def extend_enter(
        self, *obj_factories: RecObjFactorySpec, _extend_obj_factories=True
    ):
        """
        Appends new object recording factories to an existing recording switch.


        By default, it is assumed that the switch's context is already entered, and
        thus the recording obj is built and its context entered.

        :param enter: Whether to build the object depending on the switch's recording mode.
        By default (``enter=None``), the object will be built if the recording switch's context
        manager has been entered. If it has not, the object will be built and returned when
        the context is entered along with the outputs of other object factories.

        :return: If the object is built, the built object is returned. Otherwise None.
        """

        if self._exit_stack is None:
            raise Exception(
                "The context needs to be entered before calling `extend_enter`."
            )

        obj_factories = [flex_create(x) for x in obj_factories]

        _load_posns = {"start": 0, "end": len(self.obj_factories)}
        if _extend_obj_factories:
            _load_posns = {
                "start": len(self.obj_factories),
                "end": len(self.obj_factories) + len(obj_factories),
            }
            self.obj_factories.extend(obj_factories)

        if self.effective_rec_mode == RecMode.FORCE_LIVE:
            live_objs = [_of.build_live() for _of in obj_factories]
            return self._deref(live_objs)

        elif self.effective_rec_mode == RecMode.OVERWRITE:
            if not self._warned_recording:
                LOGGER.warning(f"Recording transaction to file {self.filename}.")
                self._warned_recording = True
            rec_objs, recorded_components = zip(
                *[_of.build_recorded() for _of in obj_factories]
            )
            self._recorded_components.extend(recorded_components)
            self._extend_exit_stack(recorded_components)
            return self._deref(rec_objs)

        elif self.effective_rec_mode == RecMode.PLAYBACK:
            # Note that 'LIVE' means that tests annotated with @live_test will not be skipped.
            # But any `recording_switch` context will continue to behave like a
            # recorded test even when REC_MODE='LIVE'.
            managed_objs, components = self._load(**_load_posns)
            self._extend_exit_stack(components)
            return self._deref(managed_objs)

        else:
            raise Exception("Unexpected case.")

    def __enter__(self) -> Union[Any, Tuple[Any, ...]]:
        #
        global GLOBAL_TIME
        try:
            self._exit_stack = self._exit_stack or ExitStack()
            self._exit_stack.__enter__()
            if self.effective_rec_mode == RecMode.OVERWRITE:
                self._start_time = utc_now()
                self._recorded_components = []

            # extend_enter calls self._load, which sets self._start_time.
            out = self.extend_enter(*self.obj_factories, _extend_obj_factories=False)

            #
            if self.effective_rec_mode == RecMode.PLAYBACK and self.warp_time:
                self._exit_stack.enter_context(GLOBAL_TIME)
                GLOBAL_TIME.move_to(self._start_time, monotonic=False)

            return out
        except Exception as err:
            self.__exit__(type(err), err, err.__traceback__)
            raise

    def _save(self, *args, **kwargs):
        """
        Saves the specified object recorder to its file.
        """

        with RenTempFile(self.filename, mode="w", delete=True, overwrite=True) as fo:
            content_to_dump = self._recorded_components
            with with_vanilla_getattribute():
                self.serializer.dump(
                    {
                        "version": 0,
                        "start_time": self._start_time,
                        "data": content_to_dump,
                    },
                    fo,
                    *args,
                    **kwargs,
                )

    def _load(self, start=0, end=None) -> List["ObjectPlayer"]:
        with with_vanilla_getattribute():
            if self._loaded_components is None:
                try:
                    contents = self.serializer.load(self.filename)
                except FileNotFoundError:
                    raise RecordingFileNotFoundError(
                        f"No recording file `{self.filename}`. Create one by setting env var `{self.rec_mode_env_var_name}=RECORD`."
                    )
                self._start_time = contents["start_time"]
                self._loaded_components = contents["data"]
        pb_objs, components = zip(
            *[
                _of.build_played_back(_ocs)
                for _of, _ocs in strict_zip(
                    self.obj_factories[start:end],
                    self._loaded_components[start:end],
                )
            ]
        )

        return pb_objs, components


def live_test(
    func, rec_mode: Optional[str] = None, rec_mode_env_var_name: str = DFLT_ENV_VAR
):
    """
    Unit test decorator used to indicate if the test requires a live TWS session. See `REC_MODE`_
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        _rec_mode = (
            RecMode[rec_mode]
            if rec_mode is not None
            else get_rec_mode(rec_mode_env_var_name)
        )

        live_options = [RecMode.LIVE, RecMode.FORCE_LIVE]
        if _rec_mode not in live_options:
            pytest.skip(
                f'Requires live object (set REC_MODE=[{"|".join(x.name for x in live_options)}], currently `{_rec_mode.name}`).'
            )
        else:
            return func(*args, **kwargs)

    return wrapper


def factory(root=None, rec_mode_env_var_name="REC_MODE", rec_mode=None):
    """

    Returns :func:`live_test` and :func:`recording_switch` functions with a bound **root** and default **recording mode**. The default **recording mode** will be set from the specified environment variable.

    :param root: The directory where all recordings will be stored. By default, this is given by :func:`get_default_root`. Note that this will freeze the root for all tests done with the returned partial version of :func:`recording_switch`. This is unlike calling :func:`recording_switch` directly, which will instead use a default root that depends on the calling source-file directory. To retain that functionality in the returned partial function, use ``root=''``.
    :param rec_mode_env_var_name: Name of environment variable controlling recording/playback modality.
    :return: Partial versions of :func:`live_test` and :func:`recording_switch` with bound values for parameters **root** and **rec_mode**.


    .. rubric:: Example


    .. testcode::

        # (Not part of group)
        custom_dir = "./my_custom_recordings_dir"

    .. testsetup:: factory

        from tempfile import TemporaryDirectory

        _temp_dir = TemporaryDirectory()
        custom_dir = _temp_dir.__enter__()

    .. testcode:: factory

        from jztools.object_recorder import factory, replicate
        from jztools.contextlib import environ

        # You can create a custom live_test decorator and recording_switch
        # using `factory`
        live_test, recording_switch = factory(custom_dir, "MY_REC_MODE")

    .. testcode:: factory

        for rec_mode in ["RECORD", "PLAYBACK"]:
            with environ({"MY_REC_MODE": rec_mode}):
                rec_switch = recording_switch(replicate({"a": 0}))
                with rec_switch as my_dir:
                    assert len(my_dir) == 1
                    assert rec_switch.rec_mode == rec_mode

    .. testcleanup:: factory

        _temp_dir.__exit__(None, None, None)


    """

    return (
        partial(
            live_test, rec_mode=rec_mode, rec_mode_env_var_name=rec_mode_env_var_name
        ),
        partial(
            recording_switch,
            rec_mode=rec_mode,
            rec_mode_env_var_name=rec_mode_env_var_name,
            root=root,
        ),
    )
