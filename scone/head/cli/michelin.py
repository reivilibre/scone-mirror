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

import asyncio
import os
import shutil
import sys
import tempfile
from argparse import ArgumentParser
from os.path import join
from pathlib import Path

import requests
import toml

from scone.common.misc import eprint, sha256_file


def cli() -> None:
    code = asyncio.get_event_loop().run_until_complete(cli_async())
    sys.exit(code)


async def cli_async() -> int:
    args = sys.argv[1:]

    parser = ArgumentParser(description="Compose a menu!")
    subs = parser.add_subparsers()
    supermarket = subs.add_parser("supermarket", help="generate a [[supermarket]] dish")
    supermarket.add_argument("url", help="HTTPS URL to download")
    supermarket.add_argument("-a", "--as", help="Alternative filename")
    supermarket.set_defaults(func=supermarket_cli)

    argp = parser.parse_args(args)

    if not hasattr(argp, "func"):
        parser.print_help()
        return 127

    cdir = Path(os.getcwd())

    while not Path(cdir, "scone.head.toml").exists():
        cdir = cdir.parent
        if len(cdir.parts) <= 1:
            eprint("Don't appear to be in a head. STOP.")
            sys.exit(1)

    with open(join(cdir, "scone.head.toml")) as head_toml:
        head_data = toml.load(head_toml)

    return await argp.func(argp, head_data, cdir)


async def supermarket_cli(argp, head_data: dict, head_dir: Path) -> int:
    eprint("Want to download", argp.url)

    r = requests.get(argp.url, stream=True)
    with tempfile.NamedTemporaryFile(delete=False) as tfp:
        filename = tfp.name
        for chunk in r.iter_content(4 * 1024 * 1024):
            tfp.write(chunk)

    eprint("Hashing", filename)
    real_sha256 = sha256_file(filename)

    note = f"""
Scone Supermarket

This file corresponds to {argp.url}

Downloaded by michelin.
    """.strip()

    target_path = Path(head_dir, ".scone-cache", "supermarket", real_sha256)
    target_path.parent.mkdir(parents=True, exist_ok=True)

    shutil.move(filename, str(target_path))

    with open(str(target_path) + ".txt", "w") as fout:
        # leave a note so we can find out what this is if we need to.
        fout.write(note)

    print("[[supermarket]]")
    print(f'url = "{argp.url}"')
    print(f'sha256 = "{real_sha256}"')
    print("dest = ")
    print("#owner = bob")
    print("#group = laura")
    print('#mode = "ug=rw,o=r"')

    return 0
