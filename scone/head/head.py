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

import itertools
import logging
import re
import sys
from os import path
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, cast

import toml
from nacl.encoding import URLSafeBase64Encoder

from scone.common.loader import ClassLoader
from scone.common.misc import eprint
from scone.common.pools import Pools
from scone.head.dag import RecipeDag
from scone.head.menu_reader import MenuLoader
from scone.head.recipe import Recipe, recipe_name_getter
from scone.head.secrets import SecretAccess
from scone.head.variables import Variables, merge_right_into_left_inplace

logger = logging.getLogger(__name__)


class Head:
    def __init__(
        self,
        directory: str,
        recipe_loader: ClassLoader[Recipe],
        sous: Dict[str, dict],
        groups: Dict[str, List[str]],
        secret_access: Optional[SecretAccess],
        pools: Pools,
    ):
        self.directory = directory
        self.recipe_loader = recipe_loader
        self.dag = RecipeDag()
        self.souss = sous
        self.groups = groups
        self.secret_access = secret_access
        self.variables: Dict[str, Variables] = dict()
        self.pools = pools

    @staticmethod
    def open(directory: str):
        with open(path.join(directory, "scone.head.toml")) as head_toml:
            head_data = toml.load(head_toml)

        secret_access: Optional[SecretAccess] = None
        if "freezer" in head_data and "restaurant_id" in head_data["freezer"]:
            secret_access = SecretAccess(head_data["freezer"]["restaurant_id"])
            secret_access.get_existing()
            if not secret_access.key:
                eprint("Failed to load freezer secret.")
                sys.exit(12)

        recipe_module_roots = head_data.get("recipe_roots", ["scone.default.recipes"])

        # load available recipes
        recipe_loader: ClassLoader[Recipe] = ClassLoader(Recipe, recipe_name_getter)
        for recipe_root in recipe_module_roots:
            recipe_loader.add_package_root(recipe_root)

        sous = head_data.get("sous", dict())
        groups = head_data.get("group", dict())
        groups["all"] = list(sous.keys())

        pools = Pools()

        head = Head(directory, recipe_loader, sous, groups, secret_access, pools)
        head._load_variables()
        head._load_menus()
        return head

    def _preload_variables(self, who_for: str) -> Tuple[dict, dict]:
        out_frozen: Dict[str, Any] = {}
        out_chilled: Dict[str, Any] = {}
        vardir = Path(self.directory, "vars", who_for)

        logger.debug("preloading vars for %s in %s", who_for, str(vardir))

        for file in vardir.glob("*.vf.toml"):
            if not file.is_file():
                continue
            with file.open() as var_file:
                logger.debug("Opened %s for frozen vars", file)
                frozen_vars = cast(Dict[Any, Any], toml.load(var_file))

            merge_right_into_left_inplace(out_frozen, frozen_vars)

        for file in vardir.glob("*.v.toml"):
            if not file.is_file():
                continue
            with file.open() as var_file:
                logger.debug("Opened %s for vars", file)
                chilled_vars = cast(Dict[Any, Any], toml.load(var_file))

            merge_right_into_left_inplace(out_chilled, chilled_vars)

        to_transform = [out_frozen]
        while to_transform:
            next_dict = to_transform.pop()
            for k, v in next_dict.items():
                if isinstance(v, str):
                    b64_secret = re.sub(r"\s", "", v)
                    if not self.secret_access:
                        raise RuntimeError("Secret access disabled; cannot thaw.")
                    next_dict[k] = self.secret_access.decrypt_bytes(
                        b64_secret.encode(), encoder=URLSafeBase64Encoder
                    ).decode()
                elif isinstance(v, dict):
                    to_transform.append(v)
                else:
                    raise ValueError(f"Not permitted in frozen variables file: '{v}'.")

        return out_chilled, out_frozen

    def _load_variables(self):
        preload: Dict[str, Tuple[dict, dict]] = dict()
        for who_name in itertools.chain(self.souss, self.groups):
            preload[who_name] = self._preload_variables(who_name)

        for sous_name in self.souss:
            order = ["all"]
            order += [
                group
                for group, members in self.groups.items()
                if sous_name in members and group != "all"
            ]
            order.append(sous_name)

            chilled: Dict[str, Any] = {}
            frozen: Dict[str, Any] = {}

            for who_name in order:
                in_chilled, in_frozen = preload[who_name]
                merge_right_into_left_inplace(chilled, in_chilled)
                merge_right_into_left_inplace(frozen, in_frozen)

            sous_vars = Variables(None)
            sous_vars.load_plain(frozen)
            sous_vars.load_vars_with_substitutions(chilled)

            self.variables[sous_name] = sous_vars

    def _load_menus(self):
        loader = MenuLoader(Path(self.directory, "menu"), self)
        loader.load_menus_in_dir()
        loader.dagify_all()

    # TODO remove
    # def _construct_hostmenu_for(
    #     self, hostmenu: "HostMenu", host: str, recipe_list: List[Recipe], head: "Head"
    # ) -> None:
    #     for recipe_id, dishes in hostmenu.dishes.items():
    #         recipe_cls = self.recipe_loader.get_class(recipe_id)
    #         if not recipe_cls:
    #             raise RuntimeError(f"Unable to find recipe class for '{recipe_id}'.")
    #         for slug, args in dishes.items():
    #             args = copy.deepcopy(args)
    #             self.variables[host].substitute_inplace_in_dict(args)
    #             recipe = recipe_cls.from_menu(host, slug, args, head)
    #             recipe_list.append(recipe)
    #
    # def construct_recipes(self):
    #     recipes = {}
    #     for sous in self.souss:
    #         logger.debug("Constructing recipes for %s", sous)
    #         sous_recipe_list: List[Recipe] = []
    #
    #         # construct recipes for it only
    #         sous_hm = self.menu.hostmenus.get(sous)
    #         if sous_hm is not None:
    #             self._construct_hostmenu_for(sous_hm, sous, sous_recipe_list, self)
    #
    #         # construct recipes for it that are for groups it is in
    #         for group, members in self.groups.items():
    #             if sous in members:
    #                 group_hm = self.menu.hostmenus.get(group)
    #                 if group_hm is not None:
    #                     self._construct_hostmenu_for(
    #                         group_hm, sous, sous_recipe_list, self
    #                     )
    #         recipes[sous] = sous_recipe_list
    #         logger.info("Constructed %d recipes for %s.", len(sous_recipe_list), sous)
    #     return recipes

    def debug_info(self) -> str:
        lines = []
        lines.append("Head Configuration")
        lines.append("  Sous List")
        for name, sous in self.souss.items():
            lines.append(f"   - {name} = {sous}")
        lines.append("")
        lines.append("  Sous Groups")
        for name, group in self.groups.items():
            lines.append(f"   - {name} = {group}")
        # lines.append("")
        # lines += ["  " + line for line in str(self.recipe_loader).splitlines()]
        # lines.append("")
        # lines += ["  " + line for line in str(self.menu).splitlines()]
        # lines.append("")

        return "\n".join(lines)

    def get_souss_for_hostspec(self, hostspec: str) -> Iterable[str]:
        if hostspec in self.souss:
            return (hostspec,)
        else:
            return self.groups[hostspec]
