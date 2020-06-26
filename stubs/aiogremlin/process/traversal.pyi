from gremlin_python.process import traversal
from typing import Any

class AsyncTraversalStrategies(traversal.TraversalStrategies):

    async def apply_strategies(self, traversal: Any) -> None: ...
