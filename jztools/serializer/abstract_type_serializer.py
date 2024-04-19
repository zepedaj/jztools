import abc


class AbstractTypeSerializer(abc.ABC):
    """
    Serializes a specific type of object. A target class to serialize can inherit form this class or be handled with a standalone class that implements this interface. In the second case, class method :meth:`check_type` needs to be overloaded.
    """

    @classmethod
    def check_type(cls, obj):
        """
        Returns true if the obj is of a type handled by this class.
        """
        return type(obj) == cls

    @classmethod
    @abc.abstractmethod
    def _as_serializable(cls, obj):
        """
        Returns an object that is serializable with :class:`Serializer`. For example, this could be a dictionary containing numpy.dtype objects in the values.
        """

    @classmethod
    @abc.abstractmethod
    def _from_serializable(cls, val):
        """
        Inverts the output of :meth:`_as_serializable`.
        """
