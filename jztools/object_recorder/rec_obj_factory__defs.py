from typing import Any, Union

# from jztools.object_recorder import ObjectRecorder, ObjectPlayer

ManagedType = Any
RecordedManagedType = Union["ObjectRecorder", ManagedType]
PlayedBackManagedType = Union["ObjectPlayer", ManagedType]
