"""Managers for multi cardinality vertex properties"""
from __future__ import annotations
from abc import ABC
from collections.abc import Iterable

from gremlin_python.process.traversal import Cardinality

import hobgoblin  # pylint: disable=unused-import
from . import typehints as th

# # Can't use following import lines because it causes circular imports
# from .element import VertexProperty
# from .properties.datatypes import DataType

# # Explicit forward references for type hinting
# # ============================================
# # ForwardRef not recognized in mypy
# DataType = th.ForwardRef('hobgoblin.properties.datatypes.DataType')
# VertexProperty = th.ForwardRef('hobgoblin.element.VertexProperty')
#
# # Setting as string variable doesn't work either.
# DataType = 'hobgoblin.properties.datatypes.DataType
# VertexProperty = 'hobgoblin.element.VertexProperty'
#
# (Ab)using Union for it's implicit use of ForwardRef
DataType = th.Union['hobgoblin.properties.datatypes.DataType']
VertexProperty = th.Union['hobgoblin.element.VertexProperty']


class VertexPropertyManager(Iterable, ABC):
    def __init__(self, data_type: DataType, vertex_prop: VertexProperty, card: Cardinality):
        self._data_type = data_type
        self._vertex_prop = vertex_prop
        self._card = card
        self._mapper_func = vertex_prop.metadata.map_fn

    @property
    def mapper_func(self):
        return self._mapper_func

    def __call__(self, val):
        results = []
        for v in self:
            if v.value == val:
                results.append(v)
        if len(results) == 1:
            results = results[0]
        elif not results:
            results = None
        return results

    @property
    def vertex_property(self):
        return self._vertex_prop


class ListVertexPropertyManager(list, VertexPropertyManager):
    def __init__(self, data_type, vertex_prop, card, obj):
        VertexPropertyManager.__init__(self, data_type, vertex_prop, card)
        list.__init__(self, obj)
        self._vp_map = {}

    @property
    def vp_map(self):
        return self._vp_map

    def append(self, val):
        vp = self._vertex_prop(self._data_type, card=self._card)
        vp.value = self._data_type.validate(val)
        super().append(vp)


class SetVertexPropertyManager(set, VertexPropertyManager):
    def __init__(self, data_type, vertex_prop, card, obj):
        VertexPropertyManager.__init__(self, data_type, vertex_prop, card)
        set.__init__(self, obj)

    def add(self, val):
        vp = self._vertex_prop(self._data_type, card=self._card)
        vp.value = self._data_type.validate(val)
        super().add(vp)
