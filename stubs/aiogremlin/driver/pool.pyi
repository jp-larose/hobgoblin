from aiogremlin.driver import connection as connection
from typing import Any

class PooledConnection:

    def __init__(self, conn: Any, pool: Any) -> None: ...

    @property
    def times_acquired(self): ...

    def increment_acquired(self) -> None: ...

    def decrement_acquired(self) -> None: ...

    async def write(self, message: Any): ...

    submit: Any = ...

    async def release_task(self, resp: Any) -> None: ...

    def release(self) -> None: ...

    async def close(self) -> None: ...

    @property
    def closed(self): ...

class ConnectionPool:

    def __init__(self, url: Any, loop: Any, ssl_context: Any, username: Any, password: Any, max_conns: Any, min_conns: Any,
                 max_times_acquired: Any, max_inflight: Any, response_timeout: Any, message_serializer: Any,
                 provider: Any) -> None: ...

    @property
    def url(self): ...

    async def init_pool(self) -> None: ...

    def release(self, conn: Any) -> None: ...

    async def acquire(self): ...

    async def close(self) -> None: ...
