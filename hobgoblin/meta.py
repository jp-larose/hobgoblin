from __future__ import annotations

from enum import Enum

import inflection
import types

from . import exception, mapper


class ImmutableMode(Enum):
    OFF = 0
    SIMPLE = 1


class LockingMode(Enum):
    OFF = 0
    OPTIMISTIC_LOCKING = 1


def _cls_prop(cls, prop):
    r_cls = cls
    if not hasattr(r_cls, prop) and not isinstance(r_cls, type):
        r_cls = type(cls)
    return getattr(r_cls, prop, None)


class ElementMeta(type):
    """
    Metaclass for graph elements. Responsible for creating the
    :py:class:`Mapping<hobgoblin.mapper.Mapping>` object and replacing user
    defined :py:class:`hobgoblin.properties.Property` with
    :py:class:`hobgoblin.properties.PropertyDescriptor`.
    """

    @classmethod
    def __prepare__(mcs, name, bases, **kwargs):
        new_attr = {
            'type': property(mcs.type.fget),
            'label': property(mcs.label.fget),
        }
        return new_attr

    def __new__(mcs, name, bases, namespace, e_type: str = None, label: str = None, **kwargs):
        props = {}

        # cache the type
        if name == 'VertexProperty':
            # TODO: Why is VertexProperty isolated like this?
            element_type = name.lower()
        elif bases:
            element_type = bases[0].type
            if element_type not in ['vertex', 'edge']:
                element_type = bases[0].__name__.lower()

            # Collect properties from each parent class
            for base in bases:
                base_props = getattr(base, '__properties__', {})
                props.update(base_props)
        else:
            element_type = name.lower()
        namespace['_type'] = element_type

        # cache the label
        if not namespace.get('_label', None):
            namespace['_label'] = inflection.underscore(name)

        # Collect the properties defined in the class being created
        new_namespace = {}
        props.pop('id', None)  # Treat the 'id' property specially
        for k, v in namespace.items():
            v_type = type(v)
            if isinstance(v_type, PropertyMeta):
                if element_type == 'edge' and hasattr(v, 'cardinality'):
                    raise exception.MappingError(
                        'Edge property cannot have set/list cardinality')
                props[k] = v
                if k != 'id':
                    if not v.db_name:
                        v.db_name = v.db_name_factory(k,
                                                      namespace['_label'])
                if v_type.descriptor:
                    v = v_type.descriptor(k, v)
                else:
                    raise ValueError('descriptor not assigned', v, v_type)
            new_namespace[k] = v
        new_namespace['__mapping__'] = mapper.create_mapping(namespace, props)
        new_namespace['__properties__'] = props
        new_namespace['__immutable__'] = namespace.get('__immutable__', ImmutableMode.OFF)
        new_namespace['__locking__'] = namespace.get('__locking__', LockingMode.OFF)
        new_cls = super().__new__(mcs, name, bases, new_namespace, **kwargs)
        return new_cls

    def __init__(cls, name, bases, namespace, e_type: str = None, label: str = None, **kwargs):
        super().__init__(name, bases, namespace)
        if e_type:
            cls._type = e_type
        if label:
            cls._label = label

    # Properties on the classes

    @property
    def type(cls):
        return _cls_prop(cls, '_type')

    @property
    def label(cls):
        return _cls_prop(cls, '_label')


class PropertyMeta(type):
    def __new__(mcs, name, bases, namespace, descriptor: types.GetSetDescriptorType = None, **kwargs):
        new_cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        if descriptor or not hasattr(new_cls, '_descriptor'):
            new_cls._descriptor = descriptor
        return new_cls

    @property
    def descriptor(cls):
        return cls._descriptor


class ElementPropertyMeta(PropertyMeta, ElementMeta):
    def __new__(mcs, name, bases, namespace, **kwargs):
        new_cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        return new_cls
