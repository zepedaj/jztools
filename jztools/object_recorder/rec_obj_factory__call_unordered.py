import abc
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union
from jztools.validation import checked_get_single
from .object_recorder import ObjectPlayer, ObjectRecorder
from .rec_obj_factory__defs import (
    RecordedManagedType,
    PlayedBackManagedType,
)
from jztools.object_recorder.call_unordered import call_unordered
from jztools.object_recorder.object_recorder import (
    ObjectPlayer,
    ObjectRecorder,
    base_get,
)
from jztools.object_recorder.rec_obj_factory import RecObjFactory
from jztools.py import strict_zip
from jztools.validation import checked_get_single


class RecObjFactoryWithUnorderedCallMethods(RecObjFactory):
    """
    Supports automatically handling methods that will be called using :class:`~jztools.object_recorder.unordered_call.unordered_call`.
    """

    @property
    @abc.abstractmethod
    def unordered_methods(self) -> Tuple[str]:
        """
        Specifies the managed object's methods that will be called using :class:`~jztools.object_recorder.unordered_call.unordered_call`.
        """

    def build_recorded(
        self,
    ) -> Tuple[RecordedManagedType, List[ObjectRecorder]]:
        orig_obj = self.build_live()

        all_call_recordings = []
        all_rec_fxns = {}
        for meth_name in self.unordered_methods:
            rec_fxn, call_recordings = call_unordered(
                [], base_get(orig_obj, meth_name)
            ).build_recorded()

            all_rec_fxns[meth_name] = rec_fxn
            all_call_recordings.append(checked_get_single(call_recordings))

        rec_obj = ObjectRecorder(orig_obj, attribute_overloads=all_rec_fxns)

        return rec_obj, ([rec_obj] + all_call_recordings)

    def build_played_back(
        self, recordings: List[ObjectPlayer]
    ) -> Tuple[PlayedBackManagedType, List[ObjectPlayer]]:
        pb_obj, *all_call_recordings = recordings
        all_rec_fxns = {}
        all_pb_call_recs = []

        for meth_name, call_recordings in strict_zip(
            self.unordered_methods, all_call_recordings
        ):
            pb_call, pb_call_recs = call_unordered(
                [], f"`{meth_name}`"
            ).build_played_back([call_recordings])
            all_rec_fxns[meth_name] = pb_call
            all_pb_call_recs.extend(pb_call_recs)

        base_get(pb_obj, "_attribute_overloads").update(all_rec_fxns)
        return pb_obj, ([pb_obj] + all_pb_call_recs)
