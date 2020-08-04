"""Helper functions and class to map between OGM Elements <-> DB Elements"""
from __future__ import annotations
from typing import Any, Dict

import functools
import logging
from autologging import traced, logged

from gremlin_python.process.traversal import T

from . import exception

logger = logging.getLogger(__name__)


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
                metaprops = get_metaprops(v, v.__mapping__)
                property_tuples.append((card, db_name, data_type.to_db(
                    v.value), metaprops))
                card = v.cardinality
        else:
            if hasattr(val, '__mapping__'):
                metaprops = get_metaprops(val, val.__mapping__)
                val = val.value
            else:
                metaprops = None
            property_tuples.append((None, db_name, data_type.to_db(val),
                                    metaprops))
    return property_tuples


def get_metaprops(vertex_property, mapping):
    props = mapping.ogm_properties
    metaprops = {}
    for ogm_name, (db_name, data_type) in props.items():
        val = getattr(vertex_property, ogm_name, None)
        metaprops[db_name] = data_type.to_db(val)
    return metaprops


def map_vertex_to_ogm(result, props, element, *, mapping=None):
    """Map a vertex returned by DB to OGM vertex"""
    props.pop('id')
    label = props.pop('label')
    for db_name, value in props.items():
        metaprops = []
        values = []
        for v in value:
            if isinstance(v, dict):
                val = v.pop('value')
                v.pop('key')
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
                vert_prop.mapper_func(metaprops, vert_prop)
            else:
                vert_prop.__mapping__.mapper_func(metaprops, vert_prop)
    setattr(element, '_label', label)
    setattr(element, 'id', result.id)
    return element


# temp hack
def get_hashable_id(val: Dict[str, Any]) -> Any:
    # Use the value "as-is" by default.
    if isinstance(val, dict) and "@type" in val and "@value" in val:
        if val["@type"] == "janusgraph:RelationIdentifier":
            val = val["@value"].get("value", val["@value"]["relationId"])
    return val


def map_vertex_property_to_ogm(result, element, *, mapping=None):
    """Map a vertex property returned by DB to OGM vertex"""
    logger.debug(f"{result=}\n{element=}\n{mapping=}")

    for (val, metaprops) in result:
        if isinstance(element, list):
            eid = get_hashable_id(metaprops['id'])
            current = element.vp_map.get(eid)
            # This whole system needs to be reevaluated
            if not current:
                current = element(val)
                if isinstance(current, list):
                    for vp in current:
                        if not hasattr(vp, '_id'):
                            element.vp_map[eid] = vp
                            current = vp
                            break
        elif isinstance(element, set):
            current = element(val)
        else:
            current = element

        for db_name, value in metaprops.items():
            name, data_type = mapping.db_properties.get(
                db_name, (db_name, None))
            if data_type:
                value = data_type.to_ogm(value)
            setattr(current, name, value)
            if isinstance(name, T):
                logger.debug(f"{name=}")
                setattr(element, name._name_), value


def map_edge_to_ogm(result, props, element, *, mapping=None):
    """Map an edge returned by DB to OGM edge"""
    id = props.pop(T.id)
    label = props.pop(T.label)
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
        from hobgoblin.element import GenericVertex
        element.source = GenericVertex()
    tid = result.inV.id
    etid = getattr(element.target, 'id', None)
    if _check_id(tid, etid):
        from hobgoblin.element import GenericVertex
        element.target = GenericVertex()
    setattr(element.source, 'id', sid)
    setattr(element.target, 'id', tid)
    return element


def _check_id(rid, eid):
    if eid and rid != eid:
        logger.warning('Edge vertex id has changed')
        return True
    return False


# DB <-> OGM Mapping
def create_mapping(namespace, properties):
    """Constructor for :py:class:`Mapping`"""
    logger.debug(f"{namespace=}\n{properties=}")
    element_type = namespace['_type']

    if element_type == 'vertex':
        mapping_func = map_vertex_to_ogm
    elif element_type == 'edge':
        mapping_func = map_edge_to_ogm
    elif element_type == 'vertexproperty':
        mapping_func = map_vertex_property_to_ogm
    else:
        return None

    mapping = Mapping(namespace, element_type, mapping_func, properties)
    return mapping


@traced
@logged
class Mapping:
    """
    This class stores the information necessary to map between an OGM element
    and a DB element.
    """

    def __init__(self, namespace, element_type, mapper_func, properties):
        self._label = namespace['_label']
        self._element_type = element_type
        self._mapper_func = functools.partial(mapper_func, mapping=self)
        self._db_properties = {}
        self._ogm_properties = {}
        self._map_properties(properties)

    @property
    def label(self):
        """Element label"""
        return self._label

    @property
    def mapper_func(self):
        """Function responsible for mapping db results to ogm"""
        return self._mapper_func

    @property
    def db_properties(self):
        """A dictionary of property mappings"""
        return self._db_properties

    @property
    def ogm_properties(self):
        """A dictionary of property mappings"""
        return self._ogm_properties

    def __getattr__(self, value):
        try:
            mapping, _ = self._ogm_properties[value]
            return mapping
        except KeyError:
            raise exception.MappingError(
                "unrecognized property {} for class: {}".format(
                    value, self._element_type))

    def _map_properties(self, properties):
        self.__log.debug(f"{properties=}")
        for name, prop in properties.items():
            data_type = prop.data_type
            if prop.db_name:
                db_name = prop.db_name
            else:
                db_name = name
            if hasattr(prop, '__mapping__'):
                if not self._element_type == 'vertex':
                    raise exception.MappingError(
                        'Only vertices can have vertex properties')
            self._db_properties[db_name] = (name, data_type)
            self._ogm_properties[name] = (db_name, data_type)

    def __repr__(self):
        return '<{}(type={}, label={}, properties={})>'.format(
            self.__class__.__name__, self._element_type, self._label,
            self._ogm_properties)
