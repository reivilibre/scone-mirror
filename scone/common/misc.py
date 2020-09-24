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
