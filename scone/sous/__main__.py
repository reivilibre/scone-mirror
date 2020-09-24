import asyncio
import logging
import os
import pwd
import sys
from pathlib import Path
from typing import List, cast

import cattr

from scone.common.chanpro import Channel, ChanPro
from scone.common.pools import Pools
from scone.sous import Sous, Utensil
from scone.sous.utensils import Worktop

logger = logging.getLogger(__name__)


async def main(args: List[str]):
    # loop = asyncio.get_event_loop()
    # reader = asyncio.StreamReader()
    # reader_protocol = asyncio.StreamReaderProtocol(reader)
    # await loop.connect_read_pipe(lambda: reader_protocol, sys.stdin.buffer)
    #
    # writer_transport, writer_protocol = await loop.connect_write_pipe(
    # FlowControlMixin, sys.stdout.buffer)
    # writer = StreamWriter(writer_transport, writer_protocol, None, loop)
    # data = await reader.readexactly(5)
    #
    # writer.write(data)
    # await writer.drain()

    logging.basicConfig(level=logging.DEBUG)

    if len(args) < 1:
        raise RuntimeError("Needs to be passed a sous config directory as 1st arg!")

    sous = Sous.open(args[0])
    logger.debug("Sous created")

    cp = await ChanPro.open_from_stdio()
    root = cp.new_channel(0, "Root channel")
    cp.start_listening_to_channels(default_route=root)

    await root.send({"hello": "sous"})

    remote_hello = await root.recv()
    assert isinstance(remote_hello, dict)
    assert remote_hello["hello"] == "head"

    sous_user = pwd.getpwuid(os.getuid()).pw_name

    quasi_pers = Path(args[0], "worktop", sous_user)

    if not quasi_pers.exists():
        quasi_pers.mkdir(parents=True)

    worktop = Worktop(quasi_pers, Pools())

    logger.info("Worktop dir is: %s", worktop.dir)

    while True:
        try:
            message = await root.recv()
        except EOFError:
            break
        if "nc" in message:
            # start a new command channel
            channel_num = message["nc"]
            command = message["cmd"]
            payload = message["pay"]

            utensil_class = sous.utensil_loader.get_class(command)
            utensil = cast(Utensil, cattr.structure(payload, utensil_class))

            channel = cp.new_channel(channel_num, command)

            logger.debug("going to sched task with %r", utensil)

            asyncio.create_task(run_utensil(utensil, channel, worktop))
        elif "lost" in message:
            # for a then-non-existent channel, but probably just waiting on us
            # retry without a default route.
            await cp.handle_incoming_message(message["lost"])
        else:
            raise RuntimeError(f"Unknown ch0 message {message}")


async def run_utensil(utensil: Utensil, channel: Channel, worktop: Worktop):
    try:
        await utensil.execute(channel, worktop)
    except Exception:
        logger.error("Unhandled Exception in Utensil", exc_info=True)
        await channel.close("Exception in utensil")
    else:
        logger.debug("Utensil finished with normal reason")
        await channel.close("Utensil complete")


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main(sys.argv[1:]))
