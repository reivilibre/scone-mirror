import asyncio
from asyncio import Future
from pathlib import Path
from typing import Dict, cast
from urllib.parse import urlparse
from urllib.request import urlretrieve

from scone.common.misc import sha256_file
from scone.common.modeutils import DEFAULT_MODE_FILE, parse_mode
from scone.default.steps import fridge_steps
from scone.default.steps.fridge_steps import (
    SUPERMARKET_RELATIVE,
    FridgeMetadata,
    load_and_transform,
)
from scone.default.utensils.basic_utensils import Chown, WriteFile
from scone.head.head import Head
from scone.head.kitchen import Kitchen, Preparation
from scone.head.recipe import Recipe, RecipeContext
from scone.head.utils import check_type


class FridgeCopy(Recipe):
    """
    Declares that a file should be copied from the head to the sous.
    """

    _NAME = "fridge-copy"

    def __init__(self, recipe_context: RecipeContext, args: dict, head: Head):
        super().__init__(recipe_context, args, head)

        search = fridge_steps.search_in_fridge(head, args["src"])
        if search is None:
            raise ValueError(f"Cannot find {args['src']} in the fridge.")

        desugared_src, fp = search

        unextended_path_str, meta = fridge_steps.decode_fridge_extension(str(fp))
        unextended_path = Path(unextended_path_str)

        dest = args["dest"]
        if not isinstance(dest, str):
            raise ValueError("No destination provided or wrong type.")

        if dest.endswith("/"):
            self.destination: Path = Path(args["dest"], unextended_path.parts[-1])
        else:
            self.destination = Path(args["dest"])

        mode = args.get("mode", DEFAULT_MODE_FILE)
        assert isinstance(mode, str) or isinstance(mode, int)

        self.fridge_path: str = check_type(args["src"], str)
        self.real_path: Path = fp
        self.fridge_meta: FridgeMetadata = meta
        self.mode = parse_mode(mode, directory=False)

        self._desugared_src = desugared_src

    def prepare(self, preparation: Preparation, head: Head) -> None:
        super().prepare(preparation, head)
        preparation.provides("file", str(self.destination))
        preparation.needs("directory", str(self.destination.parent))

    async def cook(self, k: Kitchen) -> None:
        data = await load_and_transform(
            k, self.fridge_meta, self.real_path, self.recipe_context.sous
        )
        dest_str = str(self.destination)
        chan = await k.start(WriteFile(dest_str, self.mode))
        await chan.send(data)
        await chan.send(None)
        if await chan.recv() != "OK":
            raise RuntimeError(f"WriteFail failed on fridge-copy to {self.destination}")

        # this is the wrong thing
        # hash_of_data = sha256_bytes(data)
        # k.get_dependency_tracker().register_remote_file(dest_str, hash_of_data)

        k.get_dependency_tracker().register_fridge_file(self._desugared_src)


class Supermarket(Recipe):
    """
    Downloads an asset (cached if necessary) and copies to sous.
    """

    _NAME = "supermarket"

    # dict of target path → future that will complete when it's downloaded
    in_progress: Dict[str, Future] = dict()

    def __init__(self, recipe_context: RecipeContext, args: dict, head):
        super().__init__(recipe_context, args, head)
        self.url = args.get("url")
        assert isinstance(self.url, str)

        self.sha256 = check_type(args.get("sha256"), str).lower()

        dest = args["dest"]
        if not isinstance(dest, str):
            raise ValueError("No destination provided or wrong type.")

        if dest.endswith("/"):
            file_basename = urlparse(self.url).path.split("/")[-1]
            self.destination: Path = Path(args["dest"], file_basename).resolve()
        else:
            self.destination = Path(args["dest"]).resolve()

        self.owner = check_type(args.get("owner", self.recipe_context.user), str)
        self.group = check_type(args.get("group", self.owner), str)

        mode = args.get("mode", DEFAULT_MODE_FILE)
        assert isinstance(mode, str) or isinstance(mode, int)
        self.mode = parse_mode(mode, directory=False)

    def prepare(self, preparation: Preparation, head: "Head"):
        super().prepare(preparation, head)
        preparation.provides("file", str(self.destination))

    async def cook(self, kitchen: "Kitchen"):
        # need to ensure we download only once, even in a race…

        supermarket_path = Path(
            kitchen.head.directory, SUPERMARKET_RELATIVE, self.sha256
        )

        if self.sha256 in Supermarket.in_progress:
            await Supermarket.in_progress[self.sha256]
        elif not supermarket_path.exists():
            note = f"""
Scone Supermarket

This file corresponds to {self.url}

Downloaded by {self}
""".strip()

            Supermarket.in_progress[self.sha256] = cast(
                Future,
                asyncio.get_running_loop().run_in_executor(
                    kitchen.head.pools.threaded,
                    self._download_file,
                    self.url,
                    str(supermarket_path),
                    self.sha256,
                    note,
                ),
            )

        # TODO(perf): load file in another thread
        with open(supermarket_path, "r") as fin:
            data = fin.read()
        chan = await kitchen.start(WriteFile(str(self.destination), self.mode))
        await chan.send(data)
        await chan.send(None)
        if await chan.recv() != "OK":
            raise RuntimeError(f"WriteFail failed on supermarket to {self.destination}")

        await kitchen.ut0(Chown(str(self.destination), self.owner, self.group))

    @staticmethod
    def _download_file(url: str, dest_path: str, check_sha256: str, note: str):
        urlretrieve(url, dest_path)
        real_sha256 = sha256_file(dest_path)
        if real_sha256 != check_sha256:
            raise RuntimeError(
                f"sha256 hash mismatch {real_sha256} != {check_sha256} (wanted)"
            )
        with open(dest_path + ".txt", "w") as fout:
            # leave a note so we can find out what this is if we need to.
            fout.write(note)
