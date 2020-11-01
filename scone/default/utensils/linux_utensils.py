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

import pwd

import attr

from scone.common.chanpro import Channel
from scone.sous import Utensil
from scone.sous.utensils import Worktop


@attr.s(auto_attribs=True)
class GetPasswdEntry(Utensil):
    user_name: str

    @attr.s(auto_attribs=True)
    class Result:
        uid: int
        gid: int
        home: str
        shell: str

    async def execute(self, channel: Channel, worktop: Worktop):
        try:
            entry = pwd.getpwnam(self.user_name)
        except KeyError:
            await channel.send(None)
            return

        await channel.send(
            GetPasswdEntry.Result(
                uid=entry.pw_uid,
                gid=entry.pw_gid,
                home=entry.pw_dir,
                shell=entry.pw_shell,
            )
        )
