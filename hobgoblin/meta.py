from __future__ import annotations
import types
from dataclasses import MISSING

from enum import Enum, auto
from collections import OrderedDict
from logging import Logger

import inflection
from autologging import traced, logged

import hobgoblin  # pylint: disable=unused-import
from .mapper import Mapping, map_element_to_ogm
from . import typehints as th  # Includes everything from std 'typing' library



class ImmutableMode(Enum):
    OFF = 0
    SIMPLE = 1


class LockingMode(Enum):
    OFF = 0
    OPTIMISTIC_LOCKING = 1


class AutoNameEnum(Enum):
    def _generate_next_value_(name, start, count, last_values):  # pylint: disable=no-self-argument, unused-argument
        return name


class _Sentinel(AutoNameEnum):
    DEFAULT = auto()
    INHERIT = auto()
    REQUIRED = auto()
    PROVIDED = auto()


DEFAULT = _Sentinel.DEFAULT
INHERIT = _Sentinel.INHERIT
REQUIRED = _Sentinel.REQUIRED
PROVIDED = _Sentinel.PROVIDED
L_DEFAULT = th.Literal[_Sentinel.DEFAULT]
L_INHERIT = th.Literal[_Sentinel.INHERIT]
L_REQUIRED = th.Literal[_Sentinel.REQUIRED]
L_PROVIDED = th.Literal[_Sentinel.PROVIDED]

T = th.TypeVar('T')


def default_label(cls_name):
    return inflection.underscore(cls_name)


def _cls_prop(obj_or_cls: th.Any, prop: str) -> T:
    r_cls = obj_or_cls
    if not hasattr(r_cls, prop) and not isinstance(r_cls, type):
        r_cls = type(obj_or_cls)
    return getattr(r_cls, prop, None)


class WriteOnceDict(dict):
    """This class is a dictionary for which keys can only be assigned a value once.

    Essentially, this means that given `wod = WriteOnlyDict()`,
    for any given `key`, `wod[key] = something` can only happen once.
    However, you can read any number of times."""
    def __setitem__(self, key, value):
        if key in self:
            raise KeyError(f"Key '{key}' already exists in WriteOnceDict, value not replaceable.")
        super().__setitem__(key, value)

    # Disallow deletions
    def __delitem__(self, key):
        raise TypeError("Deletions not permitted on WriteOnceDict")

    # Allow keys to be accessed as attributes
    def __getattr__(self, item):
        return self[item]

    def __setattr__(self, key, value):
        self[key] = value

    # Disallow deletions
    def __delattr__(self, key):
        raise TypeError("Deletions not permitted on WriteOnceDict")


class Metadata(WriteOnceDict):
    def __init__(self,
                 label:      th.Union[str, th.Callable[[type], str], L_REQUIRED, L_DEFAULT] = _Sentinel.DEFAULT,
                 locking:    th.Union[LockingMode, L_DEFAULT, L_INHERIT]                    = _Sentinel.INHERIT,
                 immutable:  th.Union[ImmutableMode, L_DEFAULT, L_INHERIT]                  = _Sentinel.INHERIT,
                 map_fn:     th.Union[th.Callable, L_REQUIRED, L_INHERIT]                   = _Sentinel.INHERIT,
                 **kwargs):
        super().__init__(label=label, locking=locking, immutable=immutable, map_fn=map_fn, **kwargs)

#    mapping:    th.Union[mapper.Mapping, th.Literal[PROVIDED]]                                      = PROVIDED
#    properties: th.Union[th.Mapping[str,'hobgoblin.properties.BaseProperty'], th.Literal[PROVIDED]] = PROVIDED
#     elem_cls:   th.Union[th.Type['hobgoblin.element.Element'], th.Literal[PROVIDED]]                = PROVIDED

    def __str__(self):
        return repr(self)


default_metadata = Metadata(
    label=default_label,
    locking=LockingMode.OFF,
    immutable=ImmutableMode.OFF,
    map_fn=map_element_to_ogm,
#    mapping=PROVIDED,
#    properties=field(default_factory=dict),
#     elem_cls=PROVIDED
)


def dict_from_class(cls):
    excluded_keys = _Sentinel.__dict__
    return {key: val for (key, val) in cls.__dict__.items() if key not in excluded_keys}


@traced(('properties', 'metadata', 'mapping'), exclude=True)
@logged
class ElementMeta(type):
    """
    Metaclass for graph elements. Responsible for creating the
    :py:class:`Mapping<hobgoblin.mapper.Mapping>` object and replacing user
    defined :py:class:`hobgoblin.properties.Property` with
    :py:class:`hobgoblin.properties.PropertyDescriptor`.
    """
    __log: th.ClassVar[Logger]

    def __new__(mcs, name: str, bases: th.Bases, namespace: th.IMapStrAny,
                **kwargs: th.Any):
        mcs.__log.debug([mcs, name, bases, namespace, kwargs])

        new_namespace: th.MMapStrAny = {}

        props = mcs._collect_props(bases, namespace, new_namespace)
        new_namespace['_properties'] = props

        metadata = mcs._nested_meta(name, bases, namespace, kwargs)
        new_namespace['_metadata'] = metadata

        mapping = mcs._mapping(metadata, props)
        new_namespace['_mapping'] = mapping

        # new_namespace['properties'] = property(mcs.properties.fget)
        # new_namespace['metadata'] = property(mcs.metadata.fget)
        # new_namespace['mapping'] = property(mcs.mapping.fget)
        mcs.__log.debug(f'{new_namespace=}')

        new_cls = super().__new__(mcs, name, bases, new_namespace)

        mcs.__log.debug(new_cls)
        return new_cls

    # def __init__(cls,  name: str, bases: th.Bases, namespace: th.IMapStrAny,
    #              **kwargs: th.Any):
    #     super().__init__(name, bases, namespace, **kwargs)
    #     cls._props = cls._collect_props(bases=bases, namespace=namespace)
    #     cls._metadata = cls._nested_meta(bases=bases, namespace=namespace)

    @classmethod
    def _collect_props(mcs, bases: th.Bases, namespace: th.IMapStrAny, new_namespace: th.MMapStrAny):
        props = OrderedDict()

        for base in bases:
            if isinstance(base, ElementMeta):
                props.update(base.properties)

        # Collect the properties defined in the class being created
        props.pop('id', None)  # Treat the 'id' property specially
        props.pop('_label', None)
        for key, val in namespace.items():
            v_type = type(val)
            # cls.__log.debug(f'{key=}, {val=}, {v_type=}')
            if isinstance(v_type, PropertyMeta):
                props[key] = val
                # cls.__log.debug(f'Added props["{key}"] = {val}')
                # if not val.db_name:
                #     val.db_name = val.db_name_factory(key, label)
                if callable(v_type.descriptor):
                    val = v_type.descriptor(val, key)
                else:
                    raise ValueError('descriptor not assigned', val, v_type)

            new_namespace[key] = val

        return props

    @classmethod
    def _nested_meta(mcs, name: str,  bases: th.Bases, namespace: th.IMapStrAny, kwargs: th.IMapStrAny):
        nested_meta = namespace.get('_metadata', Metadata())
        nested_meta_dict = {**nested_meta}

        # Merge Metadata nested classes from parent classes (i.e. inherit metadata from parent classes)
        # default_metadata_dict = {k.name: default_metadata.__dict__[k.name] for k in fields(default_metadata)}
        base_nested_meta = {**default_metadata}  # Copy base values to a regular mutable dict
        for base in bases:
            if isinstance(base, ElementMeta):
                base_nested_meta.update(base.metadata)

        # Don't implicitly inherit the label from parent classes
        base_nested_meta['label'] = REQUIRED

        for meta_key, meta_val in nested_meta_dict.items():
            if meta_val in [MISSING, INHERIT]:
                nested_meta_dict[meta_key] = base_nested_meta[meta_key]
            elif meta_val is DEFAULT:
                nested_meta_dict[meta_key] = default_metadata[meta_key]

        label = nested_meta_dict['label']
        if callable(label):
            nested_meta_dict['label'] = label(name)

        # Sanity check
        missing_meta = [key for key, val in nested_meta_dict.items() if isinstance(val, _Sentinel)]
        if len(missing_meta) > 0:
            raise ValueError(f'Required Metadata value(s) for {missing_meta} '
                             f'not provided for Element class {name}')

        for meta_key, meta_val in nested_meta_dict.items():
            if meta_val is PROVIDED:
                raise ValueError(f'Metadata value for {meta_key} '
                                 f'erroneously marked as provided in Element class {name}')

        return Metadata(**nested_meta_dict)

    @classmethod
    def _mapping(mcs, metadata: Metadata, props: th.IMapStrAny):
        label = metadata.label
        map_fn = metadata.map_fn
        try:
            mapping = Mapping(label, map_fn, props)
        except TypeError as te:
            raise NotImplementedError(f"map_fn expected to be callable; got {map_fn}") from te
        return mapping

    # Properties on the classes
    properties = th.Property[th.Dict[str, 'hobgoblin.properties.BaseProperty']](
            lambda cls: _cls_prop(cls, '_properties')
    )
    metadata = th.Property[Metadata](lambda cls: _cls_prop(cls, '_metadata'))
    mapping = th.Property[Mapping](lambda cls: _cls_prop(cls, '_mapping'))

    # @property
    # def properties(cls) -> th.Dict[str, 'hobgoblin.properties.BaseProperty']:
    #     return _cls_prop(cls, '_properties')
    #
    # @property
    # def metadata(cls) -> Metadata:
    #     return _cls_prop(cls, '_metadata')
    #
    # @property
    # def mapping(cls) -> Mapping:
    #     return _cls_prop(cls, '_mapping')

    def __str__(cls):
        return f'<{cls.__name__} Element class>'

    __repr__ = __str__


# @traced('__new__')
class PropertyMeta(type):
    _descriptor: types.GetSetDescriptorType

    def __new__(mcs: type, name: str, bases: th.Bases, namespace: th.IMapStrAny,
                descriptor: types.GetSetDescriptorType = None, **kwargs: th.Any):

        new_cls = super().__new__(mcs, name, bases, namespace, **kwargs)

        # Order of priority: param (if not None), inherited, None
        if descriptor or not hasattr(new_cls, '_descriptor'):
            new_cls._descriptor = descriptor

        return new_cls

    @property
    def descriptor(cls) -> th.Callable:
        return cls._descriptor

    def __str__(cls):
        return f'<{cls.__name__} Property class>'

    __repr__ = __str__


# @traced('__new__')
class ElementPropertyMeta(PropertyMeta, ElementMeta):
    def __new__(mcs: type, name: str, bases: th.Bases, namespace: th.IMapStrAny, **kwargs: th.Any):
        new_cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        return new_cls

    def __str__(cls):
        return f'<{cls.__name__} Element Property class>'

    __repr__ = __str__
