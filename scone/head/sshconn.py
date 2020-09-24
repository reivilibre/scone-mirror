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
    debug_logging: bool = False
) -> Tuple[ChanPro, Channel]:
    if client_key:
        opts = SSHClientConnectionOptions(username=user, client_keys=[client_key])
    else:
        opts = SSHClientConnectionOptions(username=user)

    conn: SSHClientConnection = await asyncssh.connect(host, options=opts)

    if requested_user != user:
        command = f"sudo -u {requested_user} {sous_command}"
    else:
        command = sous_command

    if debug_logging:
        command = f"tee /tmp/sconnyin-{requested_user} | {command} 2>/tmp/sconnyerr-{requested_user} " \
                  f"| tee /tmp/sconnyout-{requested_user}"

    process: SSHClientProcess = await conn.create_process(command, encoding=None)

    logger.debug("Constructing AsyncSSHChanPro...")
    cp = AsyncSSHChanPro(conn, process)
    logger.debug("Creating root channel...")
    ch = cp.new_channel(number=0, desc="Root channel")
    cp.start_listening_to_channels(default_route=None)
    logger.debug("Sending head hello...")
    await ch.send({"hello": "head"})
    logger.debug("Waiting for sous hello...")
    sous_hello = await ch.recv()
    logger.debug("Got sous hello... checking")
    assert isinstance(sous_hello, dict)
    assert sous_hello["hello"] == "sous"
    logger.debug("Valid sous hello...")
    return cp, ch
