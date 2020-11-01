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

import crypt
import logging
from typing import Optional

from scone.default.steps import linux_steps
from scone.default.utensils.linux_utensils import GetPasswdEntry
from scone.head.head import Head
from scone.head.kitchen import Kitchen, Preparation
from scone.head.recipe import Recipe, RecipeContext
from scone.head.utils import check_type, check_type_opt

logger = logging.getLogger(__name__)


class LinuxUser(Recipe):
    _NAME = "os-user"

    def __init__(self, recipe_context: RecipeContext, args: dict, head):
        super().__init__(recipe_context, args, head)

        self.user_name = check_type(args.get("name"), str)
        self.make_group = check_type(args.get("make_group", True), bool)
        self.make_home = check_type(args.get("make_home", True), bool)
        self.home: Optional[str] = check_type_opt(args.get("home"), str)
        self.password: Optional[str] = check_type_opt(args.get("password"), str)

    def prepare(self, preparation: Preparation, head: "Head") -> None:
        super().prepare(preparation, head)
        preparation.provides("os-user", self.user_name)
        if self.make_group:
            preparation.provides("os-group", self.user_name)

    async def cook(self, kitchen: Kitchen) -> None:
        # TODO(documentation): note this does not update users
        # acknowledge tracking
        kitchen.get_dependency_tracker()
        if self.password:
            password_hash: Optional[str] = crypt.crypt(self.password)
        else:
            password_hash = None

        pwd_entry = await kitchen.ut1a(
            GetPasswdEntry(self.user_name), GetPasswdEntry.Result
        )

        if pwd_entry:
            logger.warning(
                "Not updating existing os-user '%s' as it exists already and "
                "modifications could be dangerous in any case. Modification "
                "support may be implemented in the future.",
                self.user_name,
            )
        else:
            # create the user fresh
            await linux_steps.create_linux_user(
                kitchen,
                self.user_name,
                password_hash,
                self.make_home,
                self.make_group,
                self.home,
            )


class DeclareLinuxUser(Recipe):
    _NAME = "declare-os-user"

    def __init__(self, recipe_context: RecipeContext, args: dict, head):
        super().__init__(recipe_context, args, head)

        self.user_name = check_type(args.get("name"), str)

    def prepare(self, preparation: Preparation, head: "Head") -> None:
        preparation.provides("os-user", self.user_name)

    async def cook(self, kitchen: Kitchen) -> None:
        kitchen.get_dependency_tracker()


class DeclareLinuxGroup(Recipe):
    _NAME = "declare-os-group"

    def __init__(self, recipe_context: RecipeContext, args: dict, head):
        super().__init__(recipe_context, args, head)

        self.name = check_type(args.get("name"), str)

    def prepare(self, preparation: Preparation, head: "Head") -> None:
        preparation.provides("os-group", self.name)

    async def cook(self, kitchen: Kitchen) -> None:
        kitchen.get_dependency_tracker()
