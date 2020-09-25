"""Helper functions and class to map between OGM Elements <-> DB Elements"""
from __future__ import annotations

import functools
import logging
from types import MappingProxyType

from autologging import traced, logged
from gremlin_python.process.traversal import T

import hobgoblin._log_config        # pylint: disable=unused-import
from . import exception, manager
from . import typehints as th


logger = logging.getLogger(__name__)
# logger.setLevel(1)


@traced
def map_props_to_db(element, mapping: Mapping):
    """Convert OGM property names/values to DB property names/values"""
    logger.debug(f"{element=}\n{mapping=}")
    property_tuples = []
    props = mapping.ogm_properties
    for ogm_name, (db_name, data_type) in props.items():
        val = getattr(element, ogm_name, None)
        if val and isinstance(val, (list, set)):
            card = None
            for v in val:
                metaprops = get_metaprops(v, v.mapping)
                property_tuples.append((card, db_name, data_type.to_db(
                    v.value), metaprops))
                card = v.cardinality
        else:
            if hasattr(val, 'mapping'):
                metaprops = get_metaprops(val, val.mapping)
                val = val.value
            else:
                metaprops = None
            property_tuples.append((None, db_name, data_type.to_db(val),
                                    metaprops))
    return property_tuples


@traced
def get_metaprops(vertex_property, mapping):
    props = mapping.ogm_properties
    metaprops = {}
    for ogm_name, (db_name, data_type) in props.items():
        val = getattr(vertex_property, ogm_name, None)
        metaprops[db_name] = data_type.to_db(val)
    return metaprops


def map_element_to_ogm(_result, _props, _element, *, mapping=None):     # pylint: disable=unused-argument
    pass


@traced
def map_vertex_to_ogm(result, props, element, *, mapping=None):
    """Map a vertex returned by DB to OGM vertex"""
    _vid = props.pop('id')
    label = props.pop('label')
    for db_name, value in props.items():
        metaprops = []
        values = []
        for v in value:
            if isinstance(v, dict):
                val = v.pop('value')
                _key = v.pop('key')
                vid = v.pop('id')
                if v:
                    v['id'] = vid
                    metaprops.append((val, v))
                values.append(val)
            else:
                values.append(v)

        if len(values) == 1:
            value = values[0]
        elif len(values) > 1:
            value = values

        name, data_type = mapping.db_properties.get(db_name, (db_name, None))

        if data_type:
            value = data_type.to_ogm(value)
        setattr(element, name, value)

        if metaprops:
            vert_prop = getattr(element, name)
            if hasattr(vert_prop, 'mapper_func'):
                # Temporary hack for managers
                vert_prop.mapper_func(metaprops, vert_prop,
                                      mapping=vert_prop.vertex_property.mapping)
            else:
                vert_prop.mapping.mapper_func(metaprops, vert_prop,
                                              mapping=vert_prop.mapping)
    setattr(element, '_label', label)
    setattr(element, 'id', result.id)
    return element


# temp hack
@traced
def get_hashable_id(val: th.Dict[str, th.Any]) -> th.Any:
    # Use the value "as-is" by default.
    if isinstance(val, dict) and "@type" in val and "@value" in val:
        if val["@type"] == "janusgraph:RelationIdentifier":
            val = val["@value"].get("value", val["@value"]["relationId"])
    return val


@traced
def map_vertex_property_to_ogm(result, element, *, mapping=None):
    """Map a vertex property returned by DB to OGM vertex"""
    logger.debug(f"{result=}\n{element=}\n{mapping.db_properties=}\n{mapping.ogm_properties=}")

    if not mapping:
        # TODO: Set params
        mapping = Mapping()

    for (val, metaprops) in result:
        if isinstance(element, manager.ListVertexPropertyManager):
            eid = get_hashable_id(metaprops['id'])
            current = element.vp_map.get(eid)
            # TODO: This whole system needs to be reevaluated
            if not current:
                current = element(val)
                if isinstance(current, list):
                    for vp in current:
                        if not hasattr(vp, '_id'):
                            element.vp_map[eid] = vp
                            current = vp
                            break
        elif isinstance(element, manager.SetVertexPropertyManager):
            current = element(val)
        else:
            current = element

        for db_name, value in metaprops.items():
            name, data_type = mapping.db_properties.get(
                db_name, (db_name, None))
            if data_type:
                value = data_type.to_ogm(value)
            setattr(current, name, value)


@traced
def map_edge_to_ogm(result, props, element, *, mapping=None):
    """Map an edge returned by DB to OGM edge"""

    # Can't import at module level, causes circular references
    from hobgoblin.element import GenericVertex     # pylint: disable=import-outside-toplevel

    logger.debug(f'{props=}')
    _eid = props.pop(T.id)
    label = props.pop(T.label)
    # label = props.pop('label')

    for db_name, value in props.items():
        name, data_type = mapping.db_properties.get(db_name, (db_name, None))
        if data_type:
            value = data_type.to_ogm(value)
        setattr(element, name, value)
    setattr(element, '_label', label)
    setattr(element, 'id', result.id)
    # Currently not included in graphson
    # setattr(element.source, '_label', result.outV.label)
    # setattr(element.target, '_label', result.inV.label)
    sid = result.outV.id
    esid = getattr(element.source, 'id', None)
    if _check_id(sid, esid):
        element.source = GenericVertex()
    tid = result.inV.id
    etid = getattr(element.target, 'id', None)
    if _check_id(tid, etid):
        element.target = GenericVertex()
    setattr(element.source, 'id', sid)
    setattr(element.target, 'id', tid)
    return element


@traced
def _check_id(rid, eid):
    if eid and rid != eid:
        logger.warning('Edge vertex id has changed')
        return True
    return False


@traced
@logged
class Mapping:
    """
    This class stores the information necessary to map between an OGM element
    and a DB element.
    """

    def __init__(self, label, mapper_func, properties):
        self._label = label
        self._mapper_func = functools.partial(mapper_func, mapping=self)
        self._db_properties = {}
        self._ogm_properties = {}

        # Populate the two dictionaries
        self._map_properties(properties)

    @property
    def label(self) -> str:
        """Element label"""
        return self._label

    @property
    def mapper_func(self):
        """Function responsible for mapping db results to ogm"""
        return self._mapper_func

    @property
    def db_properties(self):
        """A dictionary of property mappings"""
        return MappingProxyType(self._db_properties)

    @property
    def ogm_properties(self):
        """A dictionary of property mappings"""
        return MappingProxyType(self._ogm_properties)

    def __getattr__(self, value):
        try:
            db_name, _ = self._ogm_properties[value]
            return db_name
        except KeyError as e:
            raise exception.MappingError(
                f"unrecognized property {value}") from e

    def _map_properties(self, properties):
        for name, prop in properties.items():
            self.__log.debug(f"Processing {name=}, {prop=}")
            data_type = prop.data_type
            if prop.db_name:
                db_name = prop.db_name
            else:
                db_name = prop.db_name_factory(name, self._label,)
            self.__log.debug(f"{data_type=}, {db_name=}")
            # if hasattr(prop, 'mapping') and not self._elem_cls.is_vertex():
            #     raise exception.MappingError(
            #         'Only vertices can have vertex properties')
            self._db_properties[db_name] = (name, data_type)
            self._ogm_properties[name] = (db_name, data_type)
            self.__log.debug(f"{self._db_properties[db_name]=}, {self._ogm_properties[name]=}")

    def __str__(self):
        cls_name = self.__class__.__name__
        label = self._label
        ogm_props = self._ogm_properties
        return f'<{cls_name}(label={label}, properties={ogm_props})>'
