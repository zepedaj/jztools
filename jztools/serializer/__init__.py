"""
Examples:

.. ipython:: python

    from jztools.serializer import Serializer
    import numpy as np

    srlzr = Serializer()
    for obj_str in [
        'np.array([0, 1, 2, 3])',
        "np.dtype('f')",
        "np.dtype([('f1','f'), ('f2', 'datetime64')])",
        'slice(1, 2, 3)',
        'tuple([1, 2, 3])',
        '{1, 2, 3}',
        "{'one':1, 'two':2}"
    ]:
        print('# ', obj_str)
        obj = eval(obj_str)
        print(srlzr.as_serializable(obj))
        print()

"""

from .abstract_type_serializer import AbstractTypeSerializer
from .serializer import Serializer

#
__all__ = ["Serializer", "AbstractTypeSerializer"]
