from aiogremlin.driver import provider as provider, resultset as resultset
from aiogremlin.driver.aiohttp.transport import AiohttpTransport as AiohttpTransport
from aiogremlin.driver.protocol import GremlinServerWSProtocol as GremlinServerWSProtocol
from typing import Any, Optional

logger: Any

class Connection:

    def __init__(self, url: Any, transport: Any, protocol: Any, loop: Any, username: Any, password: Any, max_inflight: Any,
                 response_timeout: Any, message_serializer: Any, provider: Any) -> None: ...

    @classmethod
    async def open(cls, url: Any, loop: Any, *, protocol: Optional[Any] = ..., transport_factory: Optional[Any] = ...,
                   ssl_context: Optional[Any] = ..., username: str = ..., password: str = ..., max_inflight: int = ...,
                   response_timeout: Optional[Any] = ..., message_serializer: Any = ..., provider: Any = ...): ...

    @property
    def message_serializer(self): ...

    @property
    def closed(self): ...

    @property
    def url(self): ...

    async def write(self, message: Any): ...

    submit: Any = ...

    async def close(self) -> None: ...

    async def __aenter__(self): ...

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None: ...
