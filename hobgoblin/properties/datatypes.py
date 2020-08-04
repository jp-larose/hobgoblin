from __future__ import annotations
from typing import Any

import logging
import abc

from gremlin_python.process.traversal import Cardinality
from gremlin_python.statics import long

from hobgoblin import element, exception, manager

logger = logging.getLogger(__name__)


class DataType(abc.ABC):
    """
    Abstract base class for Hobgoblin Data Types. All custom data types should
    inherit from :py:class:`DataType`.
    """
    py_type = None

    def __init__(self, val: Any = None):
        if val:
            val = self.validate(val)
        self._val = val

    @abc.abstractmethod
    def validate(self, val):
        """Validate property value"""
        return val

    @abc.abstractmethod
    def to_db(self, val=None):
        """
        Convert property value to db compatible format. If no value passed, try
        to use default bound value
        """
        if val is None:
            val = self._val
        return val

    @abc.abstractmethod
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

    def __repr__(self):
        return f'{self.__class__.__name__} (py_type={self.py_type})'


class Generic(DataType):
    def validate(self, val):
        return super().validate(val)

    def to_db(self, val=None):
        return super().to_db(val=val)

    def to_ogm(self, val):
        return super().to_ogm(val)


class String(DataType):
    """Simple string datatype"""
    py_type = str

    def validate(self, val):
        if val is not None:
            try:
                return str(val)
            except ValueError as e:
                raise exception.ValidationError(
                    'Not a valid string: {}'.format(val)) from e

    def to_db(self, val=None):
        return super().to_db(val=val)

    def to_ogm(self, val):
        return super().to_ogm(val)


class Integer(DataType):
    """Simple integer datatype"""
    py_type = int

    def validate(self, val):
        if val is not None:
            try:
                if isinstance(val, long):
                    return long(val)
                return int(val)
            except (ValueError, TypeError) as e:
                raise exception.ValidationError(
                    'Not a valid integer: {}'.format(val)) from e

    def to_db(self, val=None):
        return super().to_db(val=val)

    def to_ogm(self, val):
        return super().to_ogm(val)


class Float(DataType):
    """Simple float datatype"""
    py_type = float

    def validate(self, val):
        try:
            val = float(val)
        except ValueError as e:
            raise exception.ValidationError(
                "Not a valid float: {}".format(val)) from e
        return val

    def to_db(self, val=None):
        return super().to_db(val=val)

    def to_ogm(self, val):
        return super().to_ogm(val)


class Boolean(DataType):
    """Simple boolean datatype"""
    py_type = bool

    def validate(self, val: Any):
        try:
            val = bool(val)
        except ValueError as e:
            raise exception.ValidationError(
                "Not a valid boolean: {}".format(val)) from e
        return val

    def to_db(self, val=None):
        return super().to_db(val=val)

    def to_ogm(self, val):
        return super().to_ogm(val)
