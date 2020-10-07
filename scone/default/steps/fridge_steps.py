from enum import Enum
from pathlib import Path, PurePath
from typing import List, Optional, Tuple, Union

from jinja2 import Template

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


def search_in_fridge(head: Head, relative: Union[str, PurePath]) -> Optional[Path]:
    fridge_dirs = get_fridge_dirs(head)
    return search_in_dirlist(fridge_dirs, relative)


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
        template = Template(data.decode())
        proxies = kitchen.get_dependency_tracker().get_j2_compatible_dep_var_proxies(
            head.variables[sous]
        )
        data = template.render(proxies).encode()
    print("data", fullpath, data)
    return data
