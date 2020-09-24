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
