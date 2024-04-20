from numpy import datetime64
from datetime import datetime
import pytz


def utc_now() -> datetime64:
    """
    Returns the current date and time in UTC.
    """
    return datetime64(datetime.now(pytz.UTC).replace(tzinfo=None))


def base_get(object_recorder, name):
    """
    By-passes :meth:`ObjectRecorder.__getattribute__` to return the specified attriute.
    """
    return object.__getattribute__(object_recorder, name)


def get_obj(object_recorder):
    return base_get(object_recorder, "obj")


def get_meta(object_recorder):
    """
    Returns the meta info stored in the object recorder.
    """
    return base_get(object_recorder, "meta")
