import os
from os import path
from pathlib import Path
from typing import Any, Dict

import toml


class Menu:
    def __init__(self):
        self.hostmenus = {}

    def get_host(self, name: str):
        if name in self.hostmenus:
            return self.hostmenus[name]
        else:
            new = HostMenu()
            self.hostmenus[name] = new
            return new

    def __str__(self):
        lines = ["Menu"]

        for hostspec, hostmenu in self.hostmenus.items():
            lines.append(f"  on {hostspec} :-")
            lines += ["    " + line for line in str(hostmenu).split("\n")]
            lines.append("")

        return "\n".join(lines)


class HostMenu:
    def __init__(self):
        self.dishes = {}

    def __str__(self):
        lines = ["Menu"]

        for recipe, dishes in self.dishes.items():
            lines.append(f"- recipe {recipe}")
            lines += [f"  - {slug} {args}" for slug, args in dishes.items()]
            lines.append("")

        return "\n".join(lines)


def parse_toml_menu_descriptor(
    filename: str, menu: Menu, default_hostspec: str, source_name: str = None
) -> None:
    source_name = source_name or filename

    with open(filename, "r") as f:
        menu_desc: Dict[str, Any] = toml.load(f)  # type: ignore

    if "-----" in menu_desc:
        magic_tweaks = menu_desc["-----"]
        del menu_desc["-----"]
    else:
        magic_tweaks = {}

    for key, dishes in menu_desc.items():
        # print(key, "=", dishes)
        key_parts = key.split("--")
        lkp = len(key_parts)
        if lkp == 1:
            # pg-db.synapse
            hostspec = default_hostspec
            recipe = key_parts[0]
        elif lkp == 2:
            if key_parts[1] == "":
                # fridge-copy--
                hostspec = default_hostspec
                recipe = key_parts[0]
            else:
                # server1--pg-db.synapse
                hostspec = key_parts[0]
                recipe = key_parts[1]
        elif lkp == 3 and key_parts[2] == "":
            # server2--fridge-copy--
            hostspec = key_parts[0]
            recipe = key_parts[1]
        else:
            raise ValueError(f"Don't understand key: {key}")

        hostmenu = menu.get_host(hostspec)
        if recipe in hostmenu.dishes:
            mdishes = hostmenu.dishes[recipe]
        else:
            mdishes = {}
            hostmenu.dishes[recipe] = mdishes

        if isinstance(dishes, dict):
            for slug, args in dishes.items():
                if slug in mdishes:
                    raise ValueError(
                        f"Conflict in: Host {hostspec} Recipe {recipe} Dish Slug {slug}"
                    )
                mdishes[slug] = args
                args[".source"] = (source_name, key, slug)
                args[".m"] = magic_tweaks
        elif isinstance(dishes, list):
            for idx, args in enumerate(dishes):
                slug = f"@{source_name}@{idx}"
                if slug in mdishes:
                    raise ValueError(
                        f"Conflict in: Host {hostspec} Recipe {recipe} Dish Slug {slug}"
                    )
                mdishes[slug] = args
                args[".source"] = (source_name, key, idx)
                args[".m"] = magic_tweaks


def parse_toml_menu_descriptors(menu_dir: str) -> Menu:
    menu = Menu()
    for root, dirs, files in os.walk(menu_dir):
        for file in files:
            full_path = path.join(root, file)
            if file.endswith(".toml"):
                # load this as a menu file
                pieces = file.split(".")
                default_hostspec = pieces[-2]
                relative = str(Path(full_path).relative_to(menu_dir))
                parse_toml_menu_descriptor(full_path, menu, default_hostspec, relative)

    return menu
