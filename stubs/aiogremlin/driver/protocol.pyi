from collections import namedtuple
from gremlin_python.driver import protocol, serializer as serializer
from typing import Any

logger: Any

Message = namedtuple('Message', ['status_code', 'data', 'message'])

class GremlinServerWSProtocol(protocol.AbstractBaseProtocol):

    def __init__(self, message_serializer: Any, username: str = ..., password: str = ...) -> None: ...

    def connection_made(self, transport: Any) -> None: ...

    async def write(self, request_id: Any, request_message: Any) -> None: ...

    async def data_received(self, data: Any, results_dict: Any) -> None: ...
