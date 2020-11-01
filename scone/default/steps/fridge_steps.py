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

from enum import Enum
from pathlib import Path, PurePath
from typing import List, Optional, Tuple, Union

from jinja2 import DictLoader, Environment

from scone.head.head import Head
from scone.head.kitchen import Kitchen

SUPERMARKET_RELATIVE = ".scone-cache/supermarket"


def get_fridge_dirs(head: Head) -> List[Path]:
    # TODO expand with per-sous/per-group dirs?
    return [Path(head.directory, "fridge")]


def search_in_dirlist(
    dirlist: List[Path], relative: Union[str, PurePath]
) -> Optional[Path]:
    for directory in dirlist:
        potential_path = directory.joinpath(relative)
        if potential_path.exists():
            return potential_path
    return None


def search_in_fridge(
    head: Head, relative: Union[str, PurePath]
) -> Optional[Tuple[str, Path]]:
    """
    :param head: Head
    :param relative: Relative fridge path
    :return: (desugared fridge path, path to file on filesystem)
    """
    fridge_dirs = get_fridge_dirs(head)
    # TODO(feature): try sous and group-prefixed paths, and return the desugared
    #   path alongside.
    final = search_in_dirlist(fridge_dirs, relative)
    if final:
        return str(relative), final
    else:
        return None


class FridgeMetadata(Enum):
    FRIDGE = 0
    FROZEN = 1
    TEMPLATE = 2


def decode_fridge_extension(path: str) -> Tuple[str, FridgeMetadata]:
    exts = {
        ".frozen": FridgeMetadata.FROZEN,
        ".j2": FridgeMetadata.TEMPLATE,
        # don't know if we want to support .j2.frozen, but we could in the future
    }

    for ext, meta in exts.items():
        if path.endswith(ext):
            return path[: -len(ext)], meta

    return path, FridgeMetadata.FRIDGE


async def load_and_transform(
    kitchen: Kitchen, meta: FridgeMetadata, fullpath: Path, sous: str
) -> bytes:
    head = kitchen.head
    # TODO(perf) don't do this in async loop
    with fullpath.open("rb") as file:
        data = file.read()
    if meta == FridgeMetadata.FROZEN:
        # decrypt
        if head.secret_access is None:
            raise RuntimeError("Frozen file but no secret access enabled!")
        data = head.secret_access.decrypt_bytes(data)
    elif meta == FridgeMetadata.TEMPLATE:
        # pass through Jinja2
        try:
            env = Environment(
                loader=DictLoader({str(fullpath): data.decode()}), autoescape=False
            )
            template = env.get_template(str(fullpath))
            proxies = kitchen.get_dependency_tracker().get_j2_var_proxies(
                head.variables[sous]
            )
            data = template.render(proxies).encode()
        except Exception as e:
            raise RuntimeError(f"Error templating: {fullpath}") from e

        # try:
        #     return jinja2.utils.concat(
        #         template.root_render_func(template.new_context(proxies))
        #     )
        # except Exception:
        #     template.environment.handle_exception()

    return data
