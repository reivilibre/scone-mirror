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

import typing
from typing import Any, Dict, Optional

import attr

if typing.TYPE_CHECKING:
    from scone.head.head import Head
    from scone.head.kitchen import Kitchen, Preparation


def recipe_name_getter(c: typing.Type["Recipe"]) -> Optional[str]:
    if hasattr(c, "_NAME"):
        return c._NAME  # type: ignore
    return None


@attr.s(auto_attribs=True)
class RecipeContext:
    sous: str

    user: str

    slug: Optional[str]

    hierarchical_source: Optional[str]

    human: str


class Recipe:
    def __init__(
        self, recipe_context: RecipeContext, args: Dict[str, Any], head: "Head"
    ):
        self.recipe_context = recipe_context
        self.arguments = args

    @classmethod
    def new(cls, recipe_context: RecipeContext, args: Dict[str, Any], head: "Head"):
        return cls(recipe_context, args, head)

    def prepare(self, preparation: "Preparation", head: "Head") -> None:
        preparation.needs("os-user", self.recipe_context.user)

        # TODO(feature) allow merging per-task and per-menu tweaks
        # TODO(feature) allow need/provide custom things, not just user-units

    async def cook(self, kitchen: "Kitchen") -> None:
        raise NotImplementedError

    def __str__(self):
        cls = self.__class__
        if hasattr(cls, "RECIPE_NAME"):
            return (
                f"{cls.RECIPE_NAME}({cls.__name__})"  # type: ignore
                f" {self.recipe_context.human} "
                f"on {self.recipe_context.sous} ({self.arguments})"
            )
        else:
            return (
                f"{cls.__name__} {self.recipe_context.human}"
                f" on {self.recipe_context.sous} ({self.arguments})"
            )
