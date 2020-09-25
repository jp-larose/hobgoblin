"""Main OGM API classes and constructors"""
from __future__ import annotations
from typing import Any, Dict, Optional, Callable, Awaitable, Type # noqa

import asyncio
import collections
import logging
# import weakref
import uuid
from logging import Logger
from autologging import logged, traced

from gremlin_python.process.graph_traversal import __, GraphTraversal
from gremlin_python.driver.remote_connection import RemoteTraversal
from gremlin_python.process.traversal import Binding, Cardinality, Traverser, T
from gremlin_python.structure import graph

import aiogremlin
from aiogremlin.driver.protocol import Message
from aiogremlin.driver.resultset import ResultSet

from . import exception, mapper
from . import typehints as th
from .element import Element, Vertex, Edge, GenericEdge, GenericVertex, VertexProperty
from .meta import ImmutableMode, LockingMode
# from .manager import VertexPropertyManager

logger = logging.getLogger(__name__)


def bind_prop(element_class, ogm_name, val, *, binding=None):
    """
    Helper function for binding ogm properties/values to corresponding db
    properties/values for traversals.

    :param hobgoblin.element.Element element_class: User defined element class
    :param str ogm_name: Name of property as defined in the ogm
    :param val: The property value
    :param str binding: The binding for val (optional)

    :returns: tuple object ('db_property_name', ('binding(if passed)', val))
    """
    db_name = getattr(element_class, ogm_name, ogm_name)
    _, data_type = element_class.mapping.ogm_properties[ogm_name]
    val = data_type.to_db(val)
    if binding:
        val = (binding, val)
    return db_name, val


@traced
@logged
class Session:
    """
    Provides the main API for interacting with the database. Does not
    necessarily correspond to a database session. Don't instantiate directly,
    instead use :py:meth:`Hobgoblin.session<hobgoblin.app.Hobgoblin.session>`.

    :param hobgoblin.app.Hobgoblin app:
    :param aiogremlin.driver.connection.Connection remote_connection:
    """
    __log: th.ClassVar[Logger]

    def __init__(self, app, remote_connection, get_hashable_id):
        self._app = app
        self._remote_connection = remote_connection
        self._loop = self._app._loop
        self._use_session = False
        self._pending = collections.deque()
        self._current = dict()
        self._get_hashable_id = get_hashable_id
        self._graph = aiogremlin.Graph()

    @property
    def graph(self):
        return self._graph

    @property
    def app(self):
        return self._app

    @property
    def remote_connection(self):
        return self._remote_connection

    @property
    def current(self):
        return self._current

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.close()

    def close(self):
        """
        """
        self._remote_connection = None
        self._app = None

    # Traversal API
    @property
    def g(self):
        """
        Get a simple traversal source.

        :returns:
            `gremlin_python.process.GraphTraversalSource`
            object
        """
        return self.traversal()

    @property
    def _g(self):
        """
        Traversal source for internal use. Uses underlying conn. Doesn't
        trigger complex deserialization.
        """
        return self.graph.traversal().withRemote(self.remote_connection)

    def traversal(self, element_class: Type[Element] = None):
        """
        Generate a traversal using a user defined element class as a
        starting point.

        :param hobgoblin.element.Element element_class: An optional element
            class that will dictate the element type (vertex/edge) as well as
            the label for the traversal source

        :returns: `aiogremlin.process.graph_traversal.AsyncGraphTraversal`
        """
        traversal = self.graph.traversal().withRemote(self)
        if element_class:
            label = element_class.mapping.label
            if issubclass(element_class, Vertex):
                traversal = traversal.V()
            if issubclass(element_class, Edge):
                traversal = traversal.E()
            traversal = traversal.hasLabel(label)
        return traversal

    async def submit(self, bytecode):
        """
        Submit a query to the Gremlin Server.

        :returns:
            `gremlin_python.driver.remove_connection.RemoteTraversal`
            object
        """
        await self.flush()
        remote_traversal = await self.remote_connection.submit(bytecode)
        traversers = remote_traversal.traversers
        side_effects = remote_traversal.side_effects
        result_set = ResultSet(traversers.request_id, traversers._timeout,
                               self._loop)
        coro = self._receive(traversers, result_set)
        if self._loop:
            self._loop.create_task(coro)
        else:
            asyncio.create_task(coro)
        return RemoteTraversal(result_set, side_effects)

    async def _receive(self, traversers, result_set):
        try:
            async for result in traversers:
                result = await self._deserialize_result(result)
                msg = Message(200, result, '')
                result_set.queue_result(msg)
        except Exception as e:
            msg = Message(500, None, e.args[0])
            result_set.queue_result(msg)
        finally:
            result_set.queue_result(None)

    async def _deserialize_result(self, result):
        if isinstance(result, Traverser):
            bulk = result.bulk
            obj = result.object
            logger.debug(f"{obj=} {result=}")
            if isinstance(obj, (graph.Vertex, graph.Edge)):
                hashable_id = self._get_hashable_id(obj.id)
                current = self.current.get(hashable_id, None)
                label = obj.label
                # breakpoint()

                if isinstance(obj, graph.Vertex):
                    if not current:
                        current = self.app.vertices.get(label, GenericVertex)()
                    props = await self._get_vertex_properties(obj.id, label)
                else:
                    assert isinstance(obj, graph.Edge)
                    props = await self._g.E(obj.id).valueMap(True).next()
                    if not current:
                        current = self.app.edges.get(label, GenericEdge)()
                        current.source = GenericVertex()
                        current.target = GenericVertex()
                element = current.mapping.mapper_func(obj, props, current)
                self.current[hashable_id] = element
                return Traverser(element, bulk)
            else:
                return result
        # Recursive serialization is broken in hobgoblin
        elif isinstance(result, dict):
            new_result = {key: self._deserialize_result(result[key]) for key in result.keys()}
            # for key in result:
            #     result[key] = self._deserialize_result(result[key])
            return new_result
        elif isinstance(result, list):
            return [self._deserialize_result(item) for item in result]
        else:
            return result

    async def _get_vertex_properties(self, vid, label):
        # breakpoint()
        projection = self._g.V(vid).properties() \
            .project('id', 'key', 'value', 'meta') \
            .by(__.id()).by(__.key()).by(__.value()) \
            .by(__.valueMap())
        props = await projection.toList()
        self.__log.debug(f"{props=}")
        new_props = {'label': label, 'id': vid}
        for prop in props:
            key = prop['key']
            val = prop['value']
            new_props['value'] = [val]
            # print('val_type', type(val))
            meta = prop['meta']
            new_props.setdefault(key, [])
            if meta:
                meta['key'] = key
                meta['value'] = val
                meta['id'] = prop['id']
                val = meta

            new_props[key].append(val)
        self.__log.debug(f"{props=}\n{new_props=}")
        return new_props

    # Creation API
    def add(self, *elements):
        """
        Add elements to session pending queue.

        :param hobgoblin.element.Element elements: Elements to be added
        """
        for elem in elements:
            self._pending.append(elem)

    async def flush(
            self,
            conflicts_query: Optional[GraphTraversal] = None
    ) -> None:
        """
        Issue creation/update queries to database for all elements in the
        session pending queue.
        """
        transaction_id = str(uuid.uuid4())
        processed = []
        try:
            while self._pending:
                elem = self._pending.popleft()
                actual_id = self.__dirty_element(elem, id=transaction_id)
                # if actual_id:
                processed.append(await self.save(elem))
                # else:
                #    await self.save(elem)
                #    continue

                if not processed:
                    return
                if not conflicts_query:
                    await self.__commit_transaction(transaction_id)
                else:
                    await (
                        self.
                        g.
                        E().
                        has('dirty', transaction_id).
                        aggregate('x').
                        fold().
                        V().
                        has('dirty', transaction_id).
                        aggregate('x').
                        choose(
                            conflicts_query,
                            __.
                                select('x').
                                unfold().
                                properties('dirty').
                                drop()
                        ).
                        iterate()
                    )
                    await self.__rollback_transaction(transaction_id)
        except Exception as e:
            self.__log.error("Encountered exception during flush; Rolling back", exc_info=e)
            await self.__rollback_transaction(transaction_id)
            raise e

        for elem in processed:
            elem.dirty = None

    async def _remove_element(self, traversal, element):
        result = await self._simple_traversal(traversal, element)
        hashable_id = self._get_hashable_id(element.id)
        if hashable_id in self.current:
            element = self.current.pop(hashable_id)
        else:
            msg = f'Element {element} does not belong to this session obj {self}'
            logger.warning(msg)
        del element
        return result

    async def remove_vertex(self, vertex):
        """
        Remove a vertex from the db.

        :param hobgoblin.element.Vertex vertex: Vertex to be removed
        """
        traversal = self._g.V(Binding('vid', vertex.id)).drop()
        result = await self._remove_element(traversal, vertex)
        return result

    async def remove_edge(self, edge):
        """
        Remove an edge from the db.

        :param hobgoblin.element.Edge edge: Element to be removed
        """
        eid = edge.id
        if isinstance(eid, dict):
            eid = Binding('eid', edge.id)
        traversal = self._g.E(eid).drop()
        result = await self._remove_element(traversal, edge)
        return result

    async def save(self, elem):
        """
        Save an element to the db.

        :param hobgoblin.element.Element elem: Vertex or Edge to be saved

        :returns: :py:class:`Element<hobgoblin.element.Element>` object
        """
        if isinstance(elem, Vertex):
            result = await self.save_vertex(elem)
        elif isinstance(elem, Edge):
            result = await self.save_edge(elem)
        else:
            raise exception.ElementError(f"Unknown element type: {elem.__class__.__name__}")
        return result

    async def save_vertex(self, vertex):
        """
        Save a vertex to the db.

        :param hobgoblin.element.Vertex vertex: Vertex to be saved

        :returns: :py:class:`Vertex<hobgoblin.element.Vertex>` object
        """
        result = await self._save_element(
                vertex, self._check_vertex, self._add_vertex, self._update_vertex)
        hashable_id = self._get_hashable_id(result.id)
        self.current[hashable_id] = result
        return result

    async def save_edge(self, edge):
        """
        Save an edge to the db.

        :param hobgoblin.element.Edge edge: Edge to be saved

        :returns: :py:class:`Edge<hobgoblin.element.Edge>` object
        """
        if not getattr(edge, 'source', None) or not getattr(edge, 'target', None):
            raise exception.ElementError(
                    "Edges require both source and target vertices")
        result = await self._save_element(edge, self._check_edge,
                                          self._add_edge, self._update_edge)
        hashable_id = self._get_hashable_id(result.id)
        self.current[hashable_id] = result
        return result

    async def get_vertex(self, vertex):
        """
        Get a vertex from the db. Vertex must have id.

        :param hobgoblin.element.Vertex vertex: Vertex to be retrieved

        :returns: :py:class:`Vertex<hobgoblin.element.Vertex>` | None
        """
        return await self.g.V(Binding('vid', vertex.id)).next()

    async def get_edge(self, edge):
        """
        Get a edge from the db. Edge must have id.

        :param hobgoblin.element.Edge edge: Edge to be retrieved

        :returns: :py:class:`Edge<hobgoblin.element.Edge>` | None
        """
        eid = edge.id
        if isinstance(eid, dict):
            eid = Binding('eid', edge.id)
        return await self.g.E(eid).next()

    def __dirty_element(self, elem, id=str(uuid.uuid4())):
        if elem.metadata.locking == LockingMode.OPTIMISTIC_LOCKING:
            if not elem.dirty:
                elem.dirty = id
            return elem.dirty
        return None

    async def __commit_transaction(self, id):
        if id:
            await self._g.E().has('dirty', id).aggregate('x').fold().V().has('dirty', id).aggregate('x').select(
                    'x').unfold().properties('dirty').drop().iterate()

    async def __rollback_transaction(self, id):
        print(f"id of: {id}")
        if id:
            await self._g.E().has('dirty', id).aggregate('x').fold().V().has('dirty', id).aggregate('x').select(
                    'x').unfold().drop().iterate()

    async def _update_vertex(self, vertex):
        """
        Update a vertex, generally to change/remove property values.

        :param hobgoblin.element.Vertex vertex: Vertex to be updated

        :returns: :py:class:`Vertex<hobgoblin.element.Vertex>` object
        """
        props = mapper.map_props_to_db(vertex, vertex.mapping)
        traversal = self._g.V(Binding('vid', vertex.id))
        return await self._update_vertex_properties(vertex, traversal, props)

    async def _update_edge(self, edge):
        """
        Update an edge, generally to change/remove property values.

        :param hobgoblin.element.Edge edge: Edge to be updated

        :returns: :py:class:`Edge<hobgoblin.element.Edge>` object
        """
        props = mapper.map_props_to_db(edge, edge.mapping)
        eid = edge.id
        if isinstance(eid, dict):
            eid = Binding('eid', edge.id)
        traversal = self._g.E(eid)
        return await self._update_edge_properties(edge, traversal, props)

    # *special private methods for creation API

    async def _simple_traversal(self, traversal, element):
        elem = await traversal.next()
        if elem:
            if isinstance(element, Vertex):
                # Look into this
                label = await self._g.V(elem.id).label().next()
                props = await self._get_vertex_properties(elem.id, label)
            elif isinstance(element, Edge):
                props = await self._g.E(elem.id).valueMap(True).next()
            self.__log.debug(f"{props=}")
            elem = element.mapping.mapper_func(elem, props, element)
        self.__log.debug(f"{elem=}")
        return elem

    async def __handle_create_func(self, elem, create_func):
        transaction_id = elem.dirty
        if not transaction_id:
            transaction_id = self.__dirty_element(elem)
            if transaction_id:
                try:
                    result = await create_func(elem)
                    await self.__commit_transaction(transaction_id)
                    result.dirty = None
                except Exception as e:
                    await self.__rollback_transaction(transaction_id)
                    raise e
                return result

        return await create_func(elem)

    async def _save_element(self, elem, check_func, create_func, update_func):
        if hasattr(elem, 'id'):
            exists = await check_func(elem)
            if not exists:
                result = await self.__handle_create_func(elem, create_func)
            else:
                if elem.metadata.immutable != ImmutableMode.OFF:
                    raise AttributeError(
                            f"Trying to update an immutable element: {elem}")
                result = await update_func(elem)
        else:
            result = await self.__handle_create_func(elem, create_func)
        return result

    async def _add_vertex(self, vertex):
        """Convenience function for generating crud traversals."""
        props = mapper.map_props_to_db(vertex, vertex.mapping)
        traversal = self._g.addV(vertex.metadata.label)
        return await self._add_properties(traversal, props, vertex)

    async def _add_edge(self, edge):
        """Convenience function for generating crud traversals."""
        props = mapper.map_props_to_db(edge, edge.mapping)
        traversal = self._g.V(Binding('sid', edge.source.id))
        traversal = traversal.addE(edge.metadata.label)
        traversal = traversal.to(__.V(Binding('tid', edge.target.id)))
        return await self._add_properties(traversal, props, edge)

    async def _check_vertex(self, vertex):
        """Used to check for existence, does not update session vertex"""
        msg = await self._g.V(Binding('vid', vertex.id)).next()
        return msg

    async def _check_edge(self, edge):
        """Used to check for existence, does not update session edge"""
        eid = edge.id
        if isinstance(eid, dict):
            eid = Binding('eid', edge.id)
        return await self._g.E(eid).next()

    async def _update_vertex_properties(self, vertex, traversal, props):
        await self._g.V(vertex.id).properties().drop().iterate()
        return await self._add_properties(traversal, props, vertex)

    async def _update_edge_properties(self, edge, traversal, props):
        await self._g.E(edge.id).properties().drop().iterate()
        return await self._add_properties(traversal, props, edge)

    async def _add_properties(self, traversal, props, elem):
        binding = 0
        for card, db_name, val, meta_props in props:
            if not meta_props:
                meta_props = {}
            if val is not None:
                key = db_name
                # key = ('k' + str(binding), db_name)
                # val = ('v' + str(binding), val)
                metas = [obj for item in meta_props.items() for obj in item]
                if card:
                    # metas = [
                    #         j
                    #         for i in zip(meta_props.keys(), meta_props.values())
                    #         for j in i
                    # ]
                    traversal = traversal.property(card, key, val, *metas)
                else:
                    # metas = [
                    #         j
                    #         for i in zip(meta_props.keys(), meta_props.values())
                    #         for j in i
                    # ]
                    traversal = traversal.property(key, val, *metas)
                binding += 1
        result = await self._simple_traversal(traversal, elem)
        self.__log.debug(f"{result=}")
        return result
