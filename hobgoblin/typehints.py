from typing import *    # pylint: disable=unused-wildcard-import,wildcard-import
import asyncio

OptLoop = Optional[asyncio.BaseEventLoop]
OptStr = Optional[str]
OptInt = Optional[int]

Bases = Tuple[type, ...]
IMapStrAny = Mapping[str, Any]
MMapStrAny = MutableMapping[str, Any]
