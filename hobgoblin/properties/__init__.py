"""Classes to handle properties and data type definitions"""
from __future__ import annotations

import logging

from autologging import traced
from gremlin_python.statics import long

import hobgoblin._log_config  # noqa F401
from hobgoblin import exception, meta
from hobgoblin import typehints as th
from .datatypes import DataType

logger = logging.getLogger(__name__)

# Local type hint aliases
DataTypeArg = th.Union[th.Type[DataType], DataType]
DBNameFactory = th.Callable[[str, th.Any], str]
OptDBNameFactory = th.Optional[DBNameFactory]
DefaultFactory = th.Callable[[], th.Any]
OptDefaultFactory = th.Optional[DefaultFactory]


def default_db_name_factory(prop_name: str, owner: th.Any) -> str:      # pylint: disable=unused-argument
    return prop_name


@traced
class BaseProperty(metaclass=meta.PropertyMeta):
    """Base class that implements the property interface"""

    def __init__(self, data_type: DataTypeArg, *, db_name: th.OptStr = None, **_kwargs):
        if data_type:
            if isinstance(data_type, type):
                data_type = data_type()
            self._data_type = data_type
        self._db_name = db_name

    @property
    def data_type(self):
        return self._data_type

    @property
    def db_name_factory(self):
        return default_db_name_factory

    def get_db_name(self):
        return self._db_name

    def set_db_name(self, val):
        self._db_name = val

    db_name = property(get_db_name, set_db_name)

    def __str__(self):
        return f"<property {self.__class__.__name__} data_type={str(self._data_type)}>"

    __repr__ = __str__


@traced
class PropertyDescriptor:
    """
    Descriptor that validates user property input and gets/sets properties
    as instance attributes. Not instantiated by user.
    """

    def __init__(self, prop: Property, name: str):
        self._prop_name = name
        self._name = '_' + name
        self._data_type = prop.data_type
        self._default = prop.default

    def __get__(self, obj, obj_type):
        if obj is None:
            return getattr(obj_type.mapping, self._prop_name)
        return getattr(obj, self._name, self._default)

    def __set__(self, obj, val):
        val = self._data_type.validate(val)
        setattr(obj, self._name, val)

    def __delete__(self, obj):
        # hmm what is the best approach here
        attr = getattr(obj, self._name, None)
        if attr:
            del attr


@traced
class Property(BaseProperty, descriptor=PropertyDescriptor):
    """
    API class used to define properties. Replaced with
    :py:class:`PropertyDescriptor` by :py:class:`hobgoblin.element.ElementMeta`.

    :param hobgoblin.abc.DataType data_type: Str or class of data type
    :param str db_name: User defined custom name for property in db
    :param default: Default value for this property.
    """

    def __init__(self,
                 data_type: DataTypeArg,
                 *,
                 db_name: th.OptStr = None,
                 default: th.Any = None,
                 default_factory: OptDefaultFactory = None,
                 db_name_factory: OptDBNameFactory = None):
        super().__init__(data_type, db_name=db_name)
        if not db_name_factory:
            db_name_factory = default_db_name_factory
        self._db_name_factory = db_name_factory
        self._default = default
        self._default_factory = default_factory

    @property
    def db_name_factory(self):
        return self._db_name_factory

    @property
    def default(self):
        if not self._default:
            if self._default_factory:
                self._default = self._default_factory()
        return self._default


@traced
class IdPropertyDescriptor:
    def __init__(self, prop: IdProperty, name: str = 'id'):
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


@traced
def default_id_serializer(val):
    if isinstance(val, int):
        val = long(val)
    return val


@traced
class IdProperty(BaseProperty, descriptor=IdPropertyDescriptor):

    def __init__(self, data_type: DataTypeArg, *, db_name='id', serializer=None):
        super().__init__(data_type, db_name=db_name)
        if not serializer:
            serializer = default_id_serializer
        self._serializer = serializer

    @property
    def serializer(self):
        return self._serializer
