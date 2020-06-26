from aiogremlin.process import graph_traversal as graph_traversal
from aiogremlin.process.traversal import AsyncTraversalStrategies as AsyncTraversalStrategies
from gremlin_python.structure import graph
from typing import Any, Optional

class Graph(graph.Graph):

    def __init__(self) -> None: ...

    def traversal(self, traversal_source_class: Optional[Any] = ...): ...
