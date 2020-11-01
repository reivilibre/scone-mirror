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

import re
from typing import Union

# Opinionated default modes for personal use.
# Security conscious but also reasonable.
DEFAULT_MODE_FILE = 0o660
DEFAULT_MODE_DIR = 0o775


def parse_mode(mode_code: Union[str, int], directory: bool) -> int:
    look_up = {"r": 0o4, "w": 0o2, "x": 0o1}
    mults = {"u": 0o100, "g": 0o010, "o": 0o001, "a": 0o111}
    mode = 0

    if isinstance(mode_code, int):
        return mode_code

    pieces = mode_code.split(",")

    for piece in pieces:
        piecebits = 0
        match = re.fullmatch(
            r"(?P<affected>[ugoa]+)(?P<op>[-+=])(?P<value>[rwxXst]*)", piece
        )
        if match is None:
            raise ValueError(f"Did not understand mode string {piece}")
        affected = set(match.group("affected"))
        op = match.group("op")
        values = set(match.group("value"))
        if "X" in values:
            values.remove("X")
            if directory:
                values.add("x")

        mult = 0
        for affectee in affected:
            mult |= mults[affectee]

        for value in values:
            if value in ("r", "w", "x"):
                piecebits |= look_up[value] * mult
            elif value == "s":
                if "u" in affected:
                    piecebits |= 0o4000
                if "g" in affected:
                    piecebits |= 0o2000
            elif value == "t":
                piecebits |= 0o1000

        if op == "=":
            # OR with piecebits allows setting suid, sgid and sticky.
            mask = (mult * 0o7) | piecebits
            mode &= ~mask
            mode |= piecebits
        elif op == "+":
            mode |= piecebits
        elif op == "-":
            mode &= ~piecebits
        else:
            raise RuntimeError("op not [-+=].")

    return mode
