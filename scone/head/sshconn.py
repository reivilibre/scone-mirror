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

import logging
from typing import Optional, Tuple

import asyncssh
from asyncssh import SSHClientConnection, SSHClientConnectionOptions, SSHClientProcess

from scone.common.chanpro import Channel, ChanPro

logger = logging.getLogger(__name__)


class AsyncSSHChanPro(ChanPro):
    def __init__(self, connection: SSHClientConnection, process: SSHClientProcess):
        super(AsyncSSHChanPro, self).__init__(process.stdout, process.stdin)
        self._process = process
        self._connection = connection

    async def close(self) -> None:
        await super(AsyncSSHChanPro, self).close()
        await self._process.close()
        await self._connection.close()


async def open_ssh_sous(
    host: str,
    user: str,
    client_key: Optional[str],
    requested_user: str,
    sous_command: str,
    debug_logging: bool = False,
) -> Tuple[ChanPro, Channel]:
    if client_key:
        opts = SSHClientConnectionOptions(username=user, client_keys=[client_key])
    else:
        opts = SSHClientConnectionOptions(username=user)

    logger.debug("Connecting to %s[%s]@%s over SSH...", user, requested_user, host)
    conn: SSHClientConnection = await asyncssh.connect(host, options=opts)

    if requested_user != user:
        command = f"sudo -u {requested_user} {sous_command}"
    else:
        command = sous_command

    if debug_logging:
        command = (
            f"tee /tmp/sconnyin-{requested_user} "
            f"| {command} 2>/tmp/sconnyerr-{requested_user} "
            f"| tee /tmp/sconnyout-{requested_user}"
        )

    process: SSHClientProcess = await conn.create_process(command, encoding=None)

    cp = AsyncSSHChanPro(conn, process)
    ch = cp.new_channel(number=0, desc="Root channel")
    cp.start_listening_to_channels(default_route=None)
    await ch.send({"hello": "head"})
    logger.debug("Waiting for sous hello from %s[%s]@%s...", user, requested_user, host)
    sous_hello = await ch.recv()
    assert isinstance(sous_hello, dict)
    assert sous_hello["hello"] == "sous"
    return cp, ch
