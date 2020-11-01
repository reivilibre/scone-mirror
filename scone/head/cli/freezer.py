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

import os
import re
import sys
from os.path import join
from pathlib import Path

import toml
from nacl.encoding import URLSafeBase64Encoder

from scone.common.misc import eprint
from scone.head.secrets import SecretAccess


def cli() -> None:
    args = sys.argv[1:]

    if len(args) < 1:
        eprint("Not enough arguments.")
        eprint("Usage: scone-freezer <subcommand>:")
        eprint("    freezefile <file> [small files only for now!]")
        eprint("    thawfile <file>")
        eprint("    freezevar <key> (value as 1 line in stdin)")
        eprint("    thawvar <key>")
        eprint("    genkey")
        eprint("    test")
        sys.exit(127)

    cdir = Path(os.getcwd())

    while not Path(cdir, "scone.head.toml").exists():
        cdir = cdir.parent
        if len(cdir.parts) <= 1:
            eprint("Don't appear to be in a head. STOP.")
            sys.exit(1)

    with open(join(cdir, "scone.head.toml")) as head_toml:
        head_data = toml.load(head_toml)

    if "freezer" in head_data and "restaurant_id" in head_data["freezer"]:
        restaurant_id = head_data["freezer"]["restaurant_id"]
    else:
        eprint("Tip: Set a freezer.restaurant_id in your scone.head.toml")
        eprint(" to enable the ability to store your secret in the secret service.")
        restaurant_id = None

    secret_access = SecretAccess(restaurant_id)

    if args[0] == "genkey":
        assert len(args) == 1
        secret_access.generate_new()
    elif args[0] == "test":
        secret_access.get_existing()
        if secret_access.key:
            eprint("Great! Key found.")
        else:
            eprint("Oh no! Key not found.")
    elif args[0] == "freezefile":
        secret_access.get_existing()
        if not secret_access.key:
            eprint("No key found!")
            sys.exit(12)

        assert len(args) >= 2
        filepaths = [Path(p) for p in args[1:]]
        ec = 0

        for path in filepaths:
            if not path.exists():
                eprint(f"Can't freeze: no such file '{path}'")
                sys.exit(10)

        for path in filepaths:
            eprint(f"Freezing {path}")
            if not path.is_file():
                eprint(f"Can't freeze (skipping): not a regular file '{path}'")
                ec = 5

            # slurping here for simplicity;
            file_bytes = path.read_bytes()
            enc_bytes = secret_access.encrypt_bytes(file_bytes)
            dest_path = Path(str(path) + ".frozen")
            dest_path.write_bytes(enc_bytes)

        sys.exit(ec)
    elif args[0] == "thawfile":
        secret_access.get_existing()
        if not secret_access.key:
            eprint("No key found!")
            sys.exit(12)

        assert len(args) >= 2
        filepaths = [Path(p) for p in args[1:]]
        ec = 0

        for path in filepaths:
            if not path.exists():
                eprint(f"Can't thaw: no such file '{path}'")
                sys.exit(10)

        for path in filepaths:
            eprint(f"Thawing {path}")
            if not path.is_file():
                eprint(f"Can't thaw (skipping): not a regular file '{path}'")
                ec = 5
                continue

            pathstr = str(path)
            if not pathstr.endswith(".frozen"):
                eprint(f"Can't thaw (skipping): not .frozen '{path}'")
                continue

            # slurping here for simplicity;
            file_bytes = path.read_bytes()
            dec_bytes = secret_access.decrypt_bytes(file_bytes)
            dest_path = Path(str(pathstr[: -len(".frozen")]))
            dest_path.write_bytes(dec_bytes)

        sys.exit(ec)
    elif args[0] == "freezevar":
        assert len(args) == 2
        secret_access.get_existing()
        if not secret_access.key:
            eprint("No key found!")
            sys.exit(12)
        key = args[1]
        eprint("Enter value to freeze: ", end="", flush=True)
        value = input()
        enc_b64 = secret_access.encrypt_bytes(
            value.encode(), encoder=URLSafeBase64Encoder
        ).decode()
        n = 78
        str_contents = "\n".join(
            ["  " + enc_b64[i : i + n] for i in range(0, len(enc_b64), n)]
        )
        print(f'{key} = """')
        print(str_contents)
        print('"""')
    elif args[0] == "thawvar":
        assert len(args) == 1
        secret_access.get_existing()
        if not secret_access.key:
            eprint("No key found!")
            sys.exit(12)
        eprint("Enter base64 to thaw (whitespace removed painlessly) then EOF (^D):")
        value = re.sub(r"\s", "", sys.stdin.read())
        dec_str = secret_access.decrypt_bytes(
            value.encode(), encoder=URLSafeBase64Encoder
        ).decode()
        print(dec_str)
