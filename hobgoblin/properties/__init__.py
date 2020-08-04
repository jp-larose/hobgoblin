"""Classes to handle properties and data type definitions"""
from __future__ import annotations
from typing import Any, Union, Type
import types

import logging

from .. import exception, meta
from .datatypes import DataType

from gremlin_python.statics import long


logger = logging.getLogger(__name__)


def noop_factory(_x, _y):
    return None


class BaseProperty(metaclass=meta.PropertyMeta):
    """Base class that implements the property interface"""

    def __init__(self, data_type: Union[Type[DataType], DataType]):
        if isinstance(data_type, type):
            data_type = data_type()
        self._data_type = data_type

    @property
    def data_type(self):
        return self._data_type

    @property
    def db_name_factory(self):
        return noop_factory


class PropertyDescriptor:
    """
    Descriptor that validates user property input and gets/sets properties
    as instance attributes. Not instantiated by user.
    """

    def __init__(self, name: str, prop: Any):
        self._prop_name = name
        self._name = '_' + name
        self._data_type = prop.data_type
        self._default = prop.default

    def __get__(self, obj, obj_type):
        if obj is None:
            return getattr(obj_type.__mapping__, self._prop_name)
        return getattr(obj, self._name, self._default)

    def __set__(self, obj, val):
        val = self._data_type.validate(val)
        setattr(obj, self._name, val)

    def __delete__(self, obj):
        # hmm what is the best approach here
        attr = getattr(obj, self._name, None)
        if attr:
            del attr


class Property(BaseProperty, descriptor=PropertyDescriptor):
    """
    API class used to define properties. Replaced with
    :py:class:`PropertyDescriptor` by :py:class:`hobgoblin.element.ElementMeta`.

    :param hobgoblin.abc.DataType data_type: Str or class of data type
    :param str db_name: User defined custom name for property in db
    :param default: Default value for this property.
    """

    def __init__(self,
                 data_type: Union[Type[DataType], DataType],
                 *,
                 db_name=None,
                 default=None,
                 db_name_factory=None):
        super(Property, self).__init__(data_type)
        if not db_name_factory:
            db_name_factory = noop_factory  # noop
        self._db_name_factory = db_name_factory
        self._db_name = db_name
        self._default = default

    def get_db_name(self):
        return self._db_name

    def set_db_name(self, val):
        self._db_name = val

    db_name = property(get_db_name, set_db_name)

    @property
    def db_name_factory(self):
        return self._db_name_factory

    @property
    def default(self):
        return self._default


class IdPropertyDescriptor:
    def __init__(self, name, prop):
        assert name == 'id', 'ID properties must be named "id"'
        self._data_type = prop.data_type
        self._name = '_' + name
        self._serializer = prop.serializer

    def __get__(self, obj, obj_type=None):
        if obj is None:
            raise exception.ElementError(
                "Only instantiated elements have ID property")
        return getattr(obj, self._name)

    def __set__(self, obj, val):
        if self._serializer:
            val = self._serializer(val)
        val = self._data_type.validate(val)
        setattr(obj, self._name, val)


def default_id_serializer(val):
    if isinstance(val, int):
        val = long(val)
    return val


class IdProperty(BaseProperty, descriptor=IdPropertyDescriptor):

    def __init__(self, data_type: Union[Type[DataType], DataType], *, serializer=None):
        super().__init__(data_type)
        if not serializer:
            serializer = default_id_serializer
        self._serializer = serializer

    @property
    def serializer(self):
        return self._serializer
