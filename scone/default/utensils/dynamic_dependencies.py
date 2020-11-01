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
import sqlite3
from pathlib import Path
from typing import Dict, List

import attr

from scone.common.chanpro import Channel
from scone.common.misc import sha256_file
from scone.sous import Utensil
from scone.sous.utensils import Worktop


@attr.s(auto_attribs=True)
class CanSkipDynamic(Utensil):
    sous_file_hashes: Dict[str, str]

    async def execute(self, channel: Channel, worktop: Worktop):
        for file, tracked_hash in self.sous_file_hashes.items():
            try:
                real_hash = await asyncio.get_running_loop().run_in_executor(
                    worktop.pools.threaded, sha256_file, file
                )
                if real_hash != tracked_hash:
                    await channel.send(False)
                    return
            except IOError:
                await channel.send(False)
                # TODO should we log this? NB includes FileNotFound...
                return

        await channel.send(True)


@attr.s(auto_attribs=True)
class HasChangedInSousStore(Utensil):
    purpose: str
    paths: List[str]

    def _sync_execute(self, worktop: Worktop) -> bool:
        with sqlite3.connect(Path(worktop.dir, "sous_store.db")) as db:
            db.execute(
                """
            CREATE TABLE IF NOT EXISTS hash_store
            (purpose TEXT, path TEXT, hash TEXT, PRIMARY KEY (purpose, path))
            """
            )
            changed = False
            for file in self.paths:
                real_hash = sha256_file(file)
                c = db.execute(
                    "SELECT hash FROM hash_store WHERE purpose=? AND path=?",
                    (self.purpose, file),
                )
                db_hash = c.fetchone()
                if db_hash is None:
                    changed = True
                    db.execute(
                        "INSERT INTO hash_store VALUES (?, ?, ?)",
                        (self.purpose, file, real_hash),
                    )
                elif db_hash[0] != real_hash:
                    changed = True
                    db.execute(
                        "UPDATE hash_store SET hash=? WHERE purpose=? AND path=?",
                        (real_hash, self.purpose, file),
                    )
        return changed

    async def execute(self, channel: Channel, worktop: Worktop):
        answer = await asyncio.get_running_loop().run_in_executor(
            worktop.pools.threaded, self._sync_execute, worktop
        )
        await channel.send(answer)
