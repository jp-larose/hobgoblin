from aiogremlin import exception as exception
from aiogremlin.driver.resultset import ResultSet
from typing import Any, Optional

class Client:

    def __init__(self, cluster: Any, loop: Any, *, hostname: Optional[Any] = ..., aliases: Optional[Any] = ...) -> None: ...

    @property
    def aliases(self): ...

    @property
    def message_serializer(self): ...

    @property
    def cluster(self): ...

    async def close(self) -> None: ...

    def alias(self, aliases: Any): ...

    async def submit(self, message: Any, bindings: Optional[Any] = ...) -> ResultSet: ...
