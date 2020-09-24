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
