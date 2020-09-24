from pathlib import Path
from typing import Type, TypeVar

from scone.common.chanpro import Channel
from scone.common.pools import Pools

T = TypeVar("T")


class Worktop:
    def __init__(self, dir: Path, pools: Pools):
        # mostly-persistent worktop space for utensils
        self.dir = dir
        self.pools = pools


class Utensil:
    def __init__(self):
        pass

    async def execute(self, channel: Channel, worktop: Worktop):
        raise NotImplementedError


def utensil_namer(c: Type) -> str:
    return f"{c.__module__}.{c.__name__}"
