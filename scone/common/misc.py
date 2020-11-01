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
import sys
from hashlib import sha256


def eprint(*args, **kwargs):
    kwargs["file"] = sys.stderr
    print(*args, **kwargs)


def sha256_dir(path: str) -> str:
    items = {}
    with os.scandir(path) as scandir:
        for dir_entry in scandir:
            if dir_entry.is_dir():
                items[dir_entry.name] = sha256_dir(dir_entry.path)
            else:
                items[dir_entry.name] = sha256_file(dir_entry.path)
    items_sorted = list(items.items())
    items_sorted.sort()
    buf = b""
    for fname, fhash in items_sorted:
        buf += fname.encode()
        buf += b"\0"
        buf += fhash.encode()
        buf += b"\0"
    return sha256_bytes(buf)


def sha256_file(path: str) -> str:
    hasher = sha256(b"")
    with open(path, "rb") as fread:
        while True:
            data = fread.read(8192 * 1024)
            if not data:
                break
            hasher.update(data)
    return hasher.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return sha256(data).hexdigest()
