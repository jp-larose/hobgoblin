"""Hobgoblin application class and class constructor"""
from __future__ import annotations
from typing import Any

import collections
import importlib
import logging

from aiogremlin import DriverRemoteConnection, Cluster, util

from .element import Element, Edge, Vertex, VertexProperty, GenericEdge, GenericVertex
from .meta import ElementMeta
from .provider import Provider, TinkerGraph
from .session import Session

from . import typehints as th

logger = logging.getLogger(__name__)

__all__ = ['Hobgoblin']


# Main API classes
class Hobgoblin:
    """
    Class used to encapsulate database connection configuration and generate
    database connections Used as a factory to create
    :py:class:`Session<hobgoblin.session.Session>` objects.

    :param loop: Event loop implementation
    :param provider: Implementation of the graph provider
    :param get_hashable_id: Callable to

    .. deprecated:: 3.0
    The loop parameter is deprecated in the asyncio package since Python 3.8.
    """

    def __init__(self,
                 cluster: Cluster,
                 *,
                 loop: th.OptLoop = None,
                 provider: th.Type[Provider] = TinkerGraph,  # noqa
                 get_hashable_id: th.Optional[th.Callable[[Any], Any]] = None,
                 aliases=None):
        util.check_loop_deprecation(loop)

        self._cluster = cluster
        self._loop = loop if loop else cluster._loop  # noqa
        self._cluster = cluster
        self._vertices: th.DefaultDict[str, th.Type[Vertex]] = collections.defaultdict(lambda: GenericVertex)
        self._edges: th.DefaultDict[str, th.Type[Edge]] = collections.defaultdict(lambda: GenericEdge)
        self._vertex_properties: th.Dict[str, th.Type[VertexProperty]] = {}
        self._provider = provider
        if not get_hashable_id:
            get_hashable_id = self._provider.get_hashable_id
        self._get_hashable_id = get_hashable_id
        if aliases is None:
            aliases = {}
        self._aliases = aliases
        self._closed = False

    @classmethod
    async def open(cls,
                   *,
                   loop=None,
                   provider=TinkerGraph, # noqa
                   get_hashable_id=None,
                   aliases=None,
                   **cluster_config):
        # App currently only supports GraphSON 1
        # aiogremlin does not yet support providers
        util.check_loop_deprecation(loop)
        cluster = await Cluster.open(
            loop=loop, aliases=aliases, **cluster_config)
        app = cls(
            cluster=cluster,
            provider=provider,
            get_hashable_id=get_hashable_id,
            aliases=aliases)
        return app

    @property
    def cluster(self):
        return self._cluster

    @property
    def config(self):
        return self.cluster.config

    @property
    def vertices(self):
        """Registered vertex classes"""
        return self._vertices

    @property
    def vertex_properties(self):
        """Registered vertex classes"""
        return self._vertex_properties

    @property
    def edges(self):
        """Registered edge classes"""
        return self._edges

    # @property
    # def url(self):
    #     """Database url"""
    #     return self._url

    def register(self, *elements: th.Type[Element]):
        """
        Register user created Element classes.

        :param hobgoblin.element.Element elements: User defined Element classes
        """
        for elem in elements:
            label = elem.metadata.label
            if issubclass(elem, Vertex):
                self._vertices[label] = elem
            if issubclass(elem, Edge):
                self._edges[label] = elem
            if issubclass(elem, VertexProperty):
                self._vertex_properties[label] = elem

    def config_from_file(self, filename):
        """
        Load configuration from from file.

        :param str filename: Path to the configuration file.
        """
        self._cluster.config_from_file(filename)

    def config_from_yaml(self, filename):
        self._cluster.config_from_yaml(filename)

    def config_from_json(self, filename):
        """
        Load configuration from from JSON file.

        :param str filename: Path to the configuration file.
        """
        self._cluster.config_from_json(filename)

    def config_from_module(self, module):
        self._cluster.config_from_module(module)

    def register_from_module(self, module, *, package=None):
        if isinstance(module, str):
            module = importlib.import_module(module, package)
        elements = list()
        for item_name in dir(module):
            item = getattr(module, item_name)
            if isinstance(item, ElementMeta):
                elements.append(item)
        self.register(*elements)

    async def session(self, *, processor='', op='eval', aliases=None):
        """
        Create a session object.

        :param processor: TODO
        :param op: TODO
        :param aliases: Gremlin aliases

        :returns: :py:class:`Session<hobgoblin.session.Session>` object
        """
        if not aliases:
            aliases = self._aliases
        remote_connection = await DriverRemoteConnection.using(
            self._cluster, aliases=aliases)
        return Session(self, remote_connection, self._get_hashable_id)

    async def close(self):
        await self._cluster.close()
        self._closed = True

    @property
    def closed(self):
        return self._closed
