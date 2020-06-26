from gremlin_python.process import traversal
from typing import Any

class AsyncRemoteTraversalSideEffects(traversal.TraversalSideEffects):

    def __init__(self, side_effect: Any, client: Any) -> None: ...

    async def __getitem__(self, key: Any): ...

    async def keys(self): ...

    async def get(self, key: Any): ...

    async def close(self): ...
