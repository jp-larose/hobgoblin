from __future__ import annotations
from typing import Any, Callable

import logging

from gremlin_python.process.traversal import Cardinality
from gremlin_python.statics import long

from hobgoblin import element, exception, manager
from hobgoblin import typehints as th

logger = logging.getLogger(__name__)


def no_conversion(var):
    return var


class DataType:
    """
    Abstract base class for Hobgoblin Data Types. All custom data types should
    inherit from :py:class:`DataType`.
    """

    py_type: th.ClassVar[th.Callable[[th.Any], th.Any]]

    @classmethod
    def __init_subclass__(cls, py_type: th.Callable[[th.Any], th.Any], **_kwargs: th.Any):
        # if not py_type or not callable(py_type):  # mypy: no-warn-unreachable, no-warn-redundant
        #     raise ValueError("py_type must be a valid callable", py_type)
        cls.py_type = py_type

    def __init__(self, val: th.Any = None):
        if val:
            val = self.validate(val)
        self._val = val

    def validate(self, val: th.Any):
        """Validate property value"""
        if val is not None:
            try:
                return type(self).py_type(val)
            except ValueError as err:
                raise exception.ValidationError(
                        f'Not a valid {type(self).py_type}: {val}') from err
        return None

    def to_db(self, val=None):
        """
        Convert property value to db compatible format. If no value passed, try
        to use default bound value
        """
        if val is None:
            val = self._val
        return val

    def to_ogm(self, val):
        """Convert property value to a Python compatible format"""
        return val

    def validate_vertex_prop(self, val, card, vertex_prop, data_type):
        if card == Cardinality.list_:
            if isinstance(val, list):
                pass
            elif isinstance(val, (set, tuple)):
                val = list(val)
            else:
                val = [val]
            vertex_props = []
            for v in val:
                vp = vertex_prop(data_type, card=card)
                vp.value = self.validate(v)
                vertex_props.append(vp)
            val = manager.ListVertexPropertyManager(data_type, vertex_prop,
                                                    card, vertex_props)
        elif card == Cardinality.set_:
            if isinstance(val, set):
                pass
            elif isinstance(val, (list, tuple)):
                val = set(val)
            else:
                val = {val}
            vertex_props = set()
            for v in val:
                if not isinstance(v, element.VertexProperty):
                    vp = vertex_prop(data_type, card=card)
                    vp.value = self.validate(v)
                else:
                    vp = v
                vertex_props.add(vp)
            val = manager.SetVertexPropertyManager(data_type, vertex_prop,
                                                   card, vertex_props)
        else:
            vp = vertex_prop(data_type)
            vp.value = self.validate(val)
            val = vp
        return val

    def __str__(self):
        return f'<Datatype: {self.__class__.__name__} (py_type={str(type(self).py_type)})>'

    __repr__ = __str__


class Generic(DataType, py_type=no_conversion):
    pass


class String(DataType, py_type=str):
    """Simple string datatype"""


class Integer(DataType, py_type=int):
    """Simple integer datatype"""

    def validate(self, val):
        if val is not None:
            try:
                if isinstance(val, long):
                    return long(val)
                return int(val)
            except (ValueError, TypeError) as err:
                raise exception.ValidationError(
                    f'Not a valid integer: {val}') from err
        return None

class Float(DataType, py_type=float):
    """Simple float datatype"""


class Boolean(DataType, py_type=bool):
    """Simple boolean datatype"""
