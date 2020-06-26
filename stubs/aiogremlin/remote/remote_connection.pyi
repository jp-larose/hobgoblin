from gremlin_python.driver.remote_connection import RemoteStrategy
from typing import Any

class AsyncRemoteStrategy(RemoteStrategy):

    async def apply(self, traversal: Any) -> None: ...
