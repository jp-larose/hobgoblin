from typing import *    # pylint: disable=unused-wildcard-import,wildcard-import
import asyncio

OptLoop = Optional[asyncio.BaseEventLoop]
OptStr = Optional[str]
OptInt = Optional[int]

Bases = Tuple[type, ...]
IMapStrAny = Mapping[str, Any]
MMapStrAny = MutableMapping[str, Any]

O = TypeVar('O')
T = TypeVar('T')


class Property(property, Generic[T]):
    def __init__(self,
                 fget: Optional[Callable[[Any], T]] = None,
                 fset: Optional[Callable[[Any, T], None]] = None,
                 fdel: Optional[Callable[[Any], None]] = None,
                 doc: OptStr = None):
        super().__init__(fget=fget, fset=fset, fdel=fdel, doc=doc)

    def __get__(self, obj: Any, objtype: type = None) -> T:
        return cast(T, super().__get__(obj, objtype))

    def __set__(self, obj: Any, value: T) -> None:
        super().__set__(obj, value)

    def __delete__(self, obj: Any) -> None:
        super().__delete__(obj)

    def getter(self, fget: Callable[[Any], T]) -> 'Property[T]':
        return cast(Property[T], super().getter(fget))

    def setter(self, fset: Callable[[Any, T], None]) -> 'Property[T]':
        return cast(Property[T], super().setter(fset))

    def deleter(self, fdel: Callable[[Any], None]) -> 'Property[T]':
        return cast(Property[T], super().deleter(fdel))
