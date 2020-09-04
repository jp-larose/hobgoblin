"""Module defining graph elements."""
from __future__ import annotations

from autologging import traced, logged
from gremlin_python.process.traversal import Cardinality

import hobgoblin._log_config  # noqa F401  pylint: disable=unused-import
from . import exception
from .mapper import map_vertex_property_to_ogm, map_edge_to_ogm, map_vertex_to_ogm, Mapping
from .meta import ElementMeta, ElementPropertyMeta, Metadata
from . import typehints as th
from .properties import datatypes, BaseProperty, Property, IdProperty, PropertyDescriptor, default_db_name_factory, DBNameFactory


@traced
class Element(metaclass=ElementMeta):
    """Base class for classes that implement the Element property interface"""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if not (hasattr(self, key) and isinstance(
                    getattr(self, key), PropertyDescriptor)):
                raise AssertionError(
                    f"No such property: {key} for element {self.__class__.__name__}")
            setattr(self, key, value)

    id = IdProperty(datatypes.Generic)
    dirty = Property(datatypes.String)

    # Allow access to these class properties from instances
    metadata = property(ElementMeta.metadata.fget)      # type: Metadata
    mapping = property(ElementMeta.mapping.fget)        # type: Mapping
    properties = property(ElementMeta.properties.fget)  # type: th.MutableMapping[str, BaseProperty]

    @property
    def label(self):
        if hasattr(self, '_label'):
            return self._label
        return self.metadata.label

    @classmethod
    def is_edge(cls) -> bool:
        return False

    @classmethod
    def is_vertex(cls) -> bool:
        return False

    @classmethod
    def is_vertex_property(cls) -> bool:
        return False


@traced
class VertexPropertyDescriptor:
    """
    Descriptor that validates user property input and gets/sets properties
    as instance attributes.
    """

    def __init__(self, name: str, vertex_property: VertexProperty):
        self._prop_name = name
        self._name = '_' + name
        self._vp_cls = vertex_property.__class__
        self._data_type = vertex_property.data_type
        self._default = vertex_property.default
        self._cardinality = vertex_property.cardinality

    def __get__(self, obj, obj_type):
        if obj is None:
            return getattr(obj_type.mapping, self._prop_name)
        default = self._default
        if default is not None:

            default = self._data_type.validate_vertex_prop(
                default, self._cardinality, self._vp_cls,
                self._data_type)
        return getattr(obj, self._name, default)

    def __set__(self, obj, val):
        if val is not None:
            val = self._data_type.validate_vertex_prop(
                val, self._cardinality, self._vp_cls, self._data_type)
        setattr(obj, self._name, val)


@traced
@logged
class VertexProperty(BaseProperty, Element, metaclass=ElementPropertyMeta,
                     descriptor=VertexPropertyDescriptor):
    """Base class for user defined vertex properties."""

    def __init__(self,
                 data_type: datatypes.DataType = datatypes.Generic,
                 *,
                 default=None,
                 db_name=None,
                 card: Cardinality = Cardinality.single,
                 db_name_factory: DBNameFactory = default_db_name_factory,
                 **kwargs):
        super().__init__(data_type=data_type, db_name=db_name, **kwargs)

        self._db_name_factory = db_name_factory
        self._default = default
        self._val = None
        self._cardinality = card

    _metadata = Metadata(map_fn=map_vertex_property_to_ogm)

    def to_dict(self):
        result = {
            'label': self._label,
            'value': self._val
        }
        for key, _ in self.properties.items():
            prop = getattr(self, key, None)
            result[key] = prop
        return result

    def from_dict(self, d):
        d.pop('label', None)
        d.pop('type', None)
        d.pop('value', None)
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


@traced
class Vertex(Element):
    """Base class for user defined Vertex classes"""

    _metadata = Metadata(map_fn=map_vertex_to_ogm)

    def to_dict(self):
        result = {'label': self._label, 'type': self._type}
        for key, _ in self.properties.items():
            vert_prop = getattr(self, key, None)
            if isinstance(vert_prop, (list, set)):
                vert_prop = [vp.to_dict() for vp in vert_prop]
            elif isinstance(vert_prop, VertexProperty):
                vert_prop = vert_prop.to_dict()
            result[key] = vert_prop
        return result

    @classmethod
    def from_dict(cls, d: th.MutableMapping):
        elem = cls()
        d.pop('label')
        d.pop('type')
        for key, value in d.items():
            if isinstance(value, list):
                first_prop = value[0]
                setattr(elem, key, first_prop['value'])
                if isinstance(getattr(elem, key), list):
                    getattr(elem, key)[0].from_dict(first_prop)
                    for prop in value[1:]:
                        getattr(elem, key).append(prop['value'])
                        getattr(elem, key)[-1].from_dict(prop)

                elif isinstance(getattr(elem, key), set):
                    getattr(elem,
                            key)(first_prop['value']).from_dict(first_prop)
                    for prop in value[1:]:
                        val = prop['value']
                        getattr(elem, key).add(val)
                        getattr(elem, key)(val).from_dict(prop)
                else:
                    raise Exception("not a list or set property")
            elif isinstance(value, dict):
                setattr(elem, key, value['value'])
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


@traced #('__init__', '__init_subclass__')
class Edge(Element):
    """
    Base class for user defined Edge classes.

    :param Vertex source: Source (outV) vertex
    :param Vertex target: Target (inV) vertex
    """

    _metadata = Metadata(map_fn=map_edge_to_ogm)

    @classmethod
    def __init_subclass__(cls, **_kwargs):
        for prop in cls.properties.values():
            if hasattr(prop, 'cardinality'):
                raise exception.MappingError('Edge property cannot have set/list cardinality')

    def __init__(self, source: OptVertex = None, target: OptVertex = None, **kwargs):
        super().__init__(**kwargs)
        self._source = source
        self._target = target

    def to_dict(self, source: OptVertex = None, target: OptVertex = None):
        if not source:
            source = (self.source or GenericVertex()).to_dict()
        if not target:
            target = (self.target or GenericVertex()).to_dict()
        result = {
            'label': self._label,
            'source': source,
            'target': target
        }
        for key, _ in self.properties.items():
            prop = getattr(self, key, None)
            result[key] = prop
        return result

    @classmethod
    def from_dict(cls, d):
        elem = cls()
        d.pop('label')
        for key, value in d.items():
            setattr(elem, key, value)
        return elem

    def get_source(self):
        return self._source

    def set_source(self, vertex):
        if not isinstance(vertex, Vertex) and vertex is not None:
            raise TypeError(f"Source vertex must be an instance of Vertex, "
                            f"one of its subclasses, or the value None.  Got {vertex}", True)
        self._source = vertex

    def del_source(self):
        self._source = None

    source = property(get_source, set_source, del_source)

    def get_target(self):
        return self._target

    def set_target(self, vertex):
        if not isinstance(vertex, Vertex) and vertex is not None:
            raise TypeError(f"Target vertex must be an instance of Vertex, "
                            f"one of its subclasses, or the value None.  Got {vertex}", True)
        self._target = vertex

    def del_target(self):
        self._target = None

    target = property(get_target, set_target, del_target)

    @classmethod
    def is_edge(cls):
        return True


class GenericEdge(Edge):
    """
    Class used to build edges when user defined edges class is not available.
    Generally not instantiated by end user.
    """


# type hints
OptElement = th.Optional[Element]
OptVertex = th.Optional[Vertex]
OptEdge = th.Optional[Edge]
OptVertexProperty = th.Optional[VertexProperty]
