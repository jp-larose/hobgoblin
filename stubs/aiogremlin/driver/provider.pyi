from typing import Any

class Provider:
    DEFAULT_OP_ARGS: Any = ...

    @classmethod
    def get_default_op_args(cls, processor: Any): ...

class TinkerGraph(Provider):

    @staticmethod
    def get_hashable_id(val: Any): ...
