from aiogremlin import driver as driver, exception as exception
from gremlin_python.driver import serializer as serializer
from typing import Any, Optional

def my_import(name: str) -> type: ...

class Cluster:
    DEFAULT_CONFIG: Any = ...

    def __init__(self, loop: Any, aliases: Optional[Any] = ..., **config: Any) -> None: ...

    @classmethod
    async def open(cls, loop: Any, *, aliases: Optional[Any] = ..., configfile: Optional[Any] = ..., **config: Any): ...

    @property
    def hosts(self): ...

    @property
    def config(self): ...

    async def get_connection(self, hostname: Optional[Any] = ...): ...

    async def establish_hosts(self) -> None: ...

    def config_from_file(self, filename: Any) -> None: ...

    def config_from_yaml(self, filename: Any) -> None: ...

    def config_from_json(self, filename: Any) -> None: ...

    def config_from_module(self, module: Any) -> None: ...

    async def connect(self, hostname: Optional[Any] = ..., aliases: Optional[Any] = ...) -> Client: ...

    async def close(self) -> None: ...
