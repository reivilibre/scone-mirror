#  Copyright 2020, Olivier 'reivilibre'.
#
#  This file is part of Scone.
#
#  Scone is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Scone is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Scone.  If not, see <https://www.gnu.org/licenses/>.

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
