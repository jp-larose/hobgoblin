"""Module defining graph elements."""
from __future__ import annotations
from typing import Optional, MutableMapping

import logging
from autologging import traced, logged

from gremlin_python.process.traversal import Cardinality

from . import meta
from .properties import datatypes, BaseProperty, Property, IdProperty, PropertyDescriptor

logger = logging.getLogger(__name__)


@traced
@logged
class Element(metaclass=meta.ElementMeta):
    """Base class for classes that implement the Element property interface"""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if not (hasattr(self, key) and isinstance(
                    getattr(self, key), PropertyDescriptor)):
                raise AssertionError(
                    "No such property: {} for element {}".format(
                        key, self.__class__.__name__))
            setattr(self, key, value)

    id = IdProperty(datatypes.Generic)
    dirty = Property(datatypes.String)

    @classmethod
    def is_edge(cls):
        return False

    @classmethod
    def is_vertex(cls):
        return False

    @classmethod
    def is_vertex_property(cls):
        return False


@traced
@logged
class VertexPropertyDescriptor:
    """
    Descriptor that validates user property input and gets/sets properties
    as instance attributes.
    """

    def __init__(self, name, vertex_property):
        self._prop_name = name
        self._name = '_' + name
        self._vertex_property = vertex_property.__class__
        self._data_type = vertex_property.data_type
        self._default = vertex_property.default
        self._cardinality = vertex_property.cardinality

    def __get__(self, obj, obj_type):
        if obj is None:
            return getattr(obj_type.__mapping__, self._prop_name)
        default = self._default
        if default is not None:

            default = self._data_type.validate_vertex_prop(
                default, self._cardinality, self._vertex_property,
                self._data_type)
        return getattr(obj, self._name, default)

    def __set__(self, obj, val):
        if val is not None:
            val = self._data_type.validate_vertex_prop(
                val, self._cardinality, self._vertex_property, self._data_type)
        setattr(obj, self._name, val)


@traced
@logged
class VertexProperty(Element, BaseProperty, metaclass=meta.ElementPropertyMeta, descriptor=VertexPropertyDescriptor):
    """Base class for user defined vertex properties."""

    def __init__(self,
                 data_type,
                 *,
                 default=None,
                 db_name=None,
                 card=None,
                 db_name_factory=None,
                 **kwargs):
        super().__init__(**kwargs)

        if not db_name_factory:
            def db_name_factory(_x, _y):
                pass
        if isinstance(data_type, type):
            data_type = data_type()
        self._db_name_factory = db_name_factory
        self._data_type = data_type
        self._default = default
        self._db_name = db_name
        self._val = None
        if card is None:
            card = Cardinality.single
        self._cardinality = card

    def to_dict(self):
        result = {
            '_label': self._label,
            '_type': self._type,
            '__value__': self._val
        }
        for key, value in self.__properties__.items():
            prop = getattr(self, key, None)
            result[key] = prop
        return result

    def from_dict(self, d):
        d.pop('_label')
        d.pop('_type')
        d.pop('__value__')
        for key, value in d.items():
            setattr(self, key, value)

    @property
    def default(self):
        return self._default

    @property
    def data_type(self):
        return self._data_type

    @property
    def db_name_factory(self):
        return self._db_name_factory

    def get_value(self):
        return self._val

    def set_value(self, val):
        self._val = val

    value = property(get_value, set_value)

    def get_db_name(self):
        return self._db_name

    def set_db_name(self, val):
        self._db_name = val

    db_name = property(get_db_name, set_db_name)

    @property
    def cardinality(self):
        return self._cardinality

    def __repr__(self):
        return f'<{self.__class__.__name__}(type={self._data_type}, value={self.value})>'

    @classmethod
    def is_vertex_property(cls):
        return True


class Vertex(Element):
    """Base class for user defined Vertex classes"""

    def to_dict(self):
        result = {'_label': self._label, '_type': self._type}
        for key, value in self.__properties__.items():
            vert_prop = getattr(self, key, None)
            if isinstance(vert_prop, (list, set)):
                vert_prop = [vp.to_dict() for vp in vert_prop]
            elif isinstance(vert_prop, VertexProperty):
                vert_prop = vert_prop.to_dict()
            result[key] = vert_prop
        return result

    @classmethod
    def from_dict(cls, d: MutableMapping):
        elem = cls()
        d.pop('_label')
        d.pop('_type')
        for key, value in d.items():
            if isinstance(value, list):
                first_prop = value[0]
                setattr(elem, key, first_prop['__value__'])
                if isinstance(getattr(elem, key), list):
                    getattr(elem, key)[0].from_dict(first_prop)
                    for prop in value[1:]:
                        getattr(elem, key).append(prop['__value__'])
                        getattr(elem, key)[-1].from_dict(prop)

                elif isinstance(getattr(elem, key), set):
                    getattr(elem,
                            key)(first_prop['__value__']).from_dict(first_prop)
                    for prop in value[1:]:
                        val = prop['__value__']
                        getattr(elem, key).add(val)
                        getattr(elem, key)(val).from_dict(prop)
                else:
                    raise Exception("not a list or set property")
            elif isinstance(value, dict):
                setattr(elem, key, value['__value__'])
                getattr(elem, key).from_dict(value)
            else:
                setattr(elem, key, value)
        return elem

    @classmethod
    def is_vertex(cls):
        return True


class GenericVertex(Vertex):
    """
    Class used to build vertices when user defined vertex class is not
    available. Generally not instantiated by end user.
    """
    pass


class Edge(Element):
    """
    Base class for user defined Edge classes.

    :param Vertex source: Source (outV) vertex
    :param Vertex target: Target (inV) vertex
    """

    def __init__(self, source: MaybeVertex = None, target: MaybeVertex = None, **kwargs):
        super().__init__(**kwargs)
        self.source = source
        self.target = target

    def to_dict(self, source: MaybeVertex = None, target: MaybeVertex = None):
        if not source:
            source = (self.source or GenericVertex()).to_dict()
        if not target:
            target = (self.target or GenericVertex()).to_dict()
        result = {
            '_label': self._label,
            '_type': self._type,
            'source': source,
            'target': target
        }
        for key, value in self.__properties__.items():
            prop = getattr(self, key, None)
            result[key] = prop
        return result

    @classmethod
    def from_dict(cls, d):
        elem = cls()
        d.pop('_label')
        d.pop('_type')
        for key, value in d.items():
            setattr(elem, key, value)
        return elem

    def get_source(self):
        return self._source

    def set_source(self, vertex):
        assert isinstance(vertex, Vertex) or vertex is None
        self._source = vertex

    def del_source(self):
        del self._source

    source = property(get_source, set_source, del_source)

    def get_target(self):
        return self._target

    def set_target(self, vertex):
        assert isinstance(vertex, Vertex) or vertex is None
        self._target = vertex

    def del_target(self):
        del self._target

    target = property(get_target, set_target, del_target)

    @classmethod
    def is_edge(cls):
        return True


class GenericEdge(Edge):
    """
    Class used to build edges when user defined edges class is not available.
    Generally not instantiated by end user.
    """
    pass


# type hints
MaybeElement = Optional[Element]
MaybeVertex = Optional[Vertex]
MaybeEdge = Optional[Edge]
