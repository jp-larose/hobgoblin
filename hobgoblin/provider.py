from __future__ import annotations
from typing import Any, Dict


class Provider:
    """Superclass for provider plugins"""
    DEFAULT_OP_ARGS: Dict[Any, Any] = {}

    @classmethod
    def get_default_op_args(cls, processor):
        return cls.DEFAULT_OP_ARGS.get(processor, dict())

    @staticmethod
    def get_hashable_id(val):
        raise NotImplementedError


class TinkerGraph(Provider):  # TODO
    """Default provider"""

    @staticmethod
    def get_hashable_id(val):
        return val
