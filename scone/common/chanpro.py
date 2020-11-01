import asyncio
import logging
import struct
import sys
from asyncio import Queue, Task
from asyncio.streams import FlowControlMixin, StreamReader, StreamWriter
from typing import Any, Dict, Optional

import attr
import cattr
import cbor2

SIZE_FORMAT = "!I"
logger = logging.getLogger(__name__)


class ChanPro:
    def __init__(self, in_stream: StreamReader, out_stream: StreamWriter):
        self._in = in_stream
        self._out = out_stream
        self._channels: Dict[int, "Channel"] = {}
        self._listener: Optional[Task] = None

    async def close(self) -> None:
        # TODO cancel _listener?
        pass

    @staticmethod
    async def open_from_stdio() -> "ChanPro":
        loop = asyncio.get_event_loop()
        reader = asyncio.StreamReader()
        reader_protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: reader_protocol, sys.stdin.buffer)

        writer_transport, writer_protocol = await loop.connect_write_pipe(
            FlowControlMixin, sys.stdout.buffer
        )
        writer = StreamWriter(writer_transport, writer_protocol, None, loop)

        return ChanPro(reader, writer)

    async def _send_dict(self, dictionary: dict):
        encoded = cbor2.dumps(dictionary)
        encoded_len = struct.pack(SIZE_FORMAT, len(encoded))
        self._out.write(encoded_len)
        self._out.write(encoded)
        await self._out.drain()

    async def _recv_dict(self) -> dict:
        size = struct.calcsize(SIZE_FORMAT)
        encoded_len = await self._in.readexactly(size)
        (length,) = struct.unpack(SIZE_FORMAT, encoded_len)
        encoded = await self._in.readexactly(length)
        return cbor2.loads(encoded)

    def new_channel(self, number: int, desc: str):
        if number in self._channels:
            channel = self._channels[number]
            raise ValueError(f"Channel {number} already in use ({channel}).")
        channel = Channel(number, desc, self)
        self._channels[number] = channel
        return channel

    async def send_message(self, channel: int, payload: Any):
        await self._send_dict({"c": channel, "p": payload})

    async def send_close(self, channel: int, reason: str = None):
        # TODO extend with error indication capability (remote throw) ?
        # TODO might want to wait until other end closed?
        await self._send_dict({"c": channel, "close": True, "reason": reason})

    def start_listening_to_channels(self, default_route: Optional["Channel"]):
        async def channel_listener():
            idx = 0
            while True:
                message = await self._recv_dict()
                # logger.debug("<message> %d %r", idx, message)
                idx += 1
                await self.handle_incoming_message(message, default_route=default_route)

        self._listener = asyncio.create_task(
            channel_listener()  # py 3.8 , name="chanpro channel listener"
        )

    async def handle_incoming_message(
        self, message: dict, default_route: Optional["Channel"] = None
    ):
        if "c" not in message:
            logger.warning("Received message without channel number.")
        channel_num = message["c"]
        channel = self._channels.get(channel_num)
        if not channel:
            if default_route:
                await default_route._queue.put({"lost": message})
            else:
                logger.warning(
                    "Received message about non-existent channel number %r.",
                    channel_num,
                )
            return
        # XXX todo send msg, what about shutdown too?
        if "p" in message:
            # payload on channel
            await channel._queue.put(message["p"])
        elif "close" in message:
            channel._closed = True
            await channel._queue.put(None)
        else:
            raise ValueError(f"Unknown channel message with keys {message.keys()}")


class Channel:
    def __init__(self, number: int, desc: str, chanpro: ChanPro):
        self.number = number
        self.description = desc
        self.chanpro = chanpro
        self._queue: Queue[Any] = Queue()
        self._closed = False

    def __str__(self):
        return f"Channel №{self.number} ({self.description})"

    async def send(self, payload: Any):
        if attr.has(payload.__class__):
            payload = cattr.unstructure(payload)
        await self.chanpro.send_message(self.number, payload)

    async def recv(self) -> Any:
        if self._queue.empty() and self._closed:
            raise EOFError("Channel closed.")
        item = await self._queue.get()
        if item is None and self._queue.empty() and self._closed:
            raise EOFError("Channel closed.")
        return item

    async def close(self, reason: str = None):
        if not self._closed:
            self._closed = True
            await self._queue.put(None)
            await self.chanpro.send_close(self.number, reason)

    async def wait_close(self):
        try:
            await self.recv()
            raise RuntimeError("Message arrived when expecting closure.")
        except EOFError:
            # expected
            return

    async def consume(self) -> Any:
        """
        Consume the last item of the channel and assert closure.
        The last item is returned.
        """
        item = await self.recv()
        await self.wait_close()
        return item


class ChanProHead:
    def __init__(self, chanpro: ChanPro, channel0: Channel):
        self._chanpro = chanpro
        self._channel0 = channel0
        self._next_channel_id = 1

    async def start_command_channel(self, command: str, payload: Any) -> Channel:
        new_channel = self._chanpro.new_channel(self._next_channel_id, command)
        self._next_channel_id += 1
        await self._channel0.send(
            {"nc": new_channel.number, "cmd": command, "pay": payload}
        )
        return new_channel
