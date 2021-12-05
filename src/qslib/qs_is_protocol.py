from __future__ import annotations
import asyncio
from asyncio.futures import Future
import logging
import re
import io
from dataclasses import dataclass
from typing import Any, Coroutine, Literal, Optional, Protocol
import time


NL_OR_Q = re.compile(rb"(?:\n|<(/?)([\w.]+)[ *]*>)")
Q_ONLY = re.compile(rb"<(/?)([\w.]+)[ *]*>")
TIMESTAMP = re.compile(rb"(\d{8,}\.\d{3})")

log = logging.getLogger(__name__)


class Error(Exception):
    pass


@dataclass
class CommandError(Error):
    command: Optional[str]
    ref_index: Optional[str]
    response: str


class ReplyError(IOError):
    pass


class SubHandler(Protocol):
    def __call__(
        self, topic: bytes, message: bytes, timestamp: float | None = None
    ) -> Coroutine[None, None, None]:  # pragma: no cover
        ...


class QS_IS_Protocol(asyncio.Protocol):
    lostconnection: Future[Any]
    last_received: float

    def __init__(self) -> None:
        self.default_topic_handler = self._default_topic_handler
        self.readymsg = asyncio.get_running_loop().create_future()
        self.lostconnection = asyncio.get_running_loop().create_future()
        self.should_be_connected = False
        self.last_received = time.time()

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if self.should_be_connected:
            log.warn("Lost connection")
        else:
            log.info("Connection closed")
        self.lostconnection.set_result(exc)
        self.should_be_connected = False

        # Cancel all futures; we'll never recover them.
        for _, future in self.waiting_commands:
            if future is not None:
                future.cancel()

        self.waiting_commands = []

    async def disconnect(self) -> None:
        self.should_be_connected = False
        await self.run_command("QUIT")

    def connection_made(self, transport: Any) -> None:
        log.info("Made connection")
        self.should_be_connected = True
        # setup connection.
        self.transport = transport
        self.waiting_commands: list[
            tuple[
                bytes,
                None
                | Future[
                    tuple[
                        bytes, None | bytes, None | asyncio.Future[tuple[bytes, bytes]]
                    ]
                ],
            ]
        ] = []
        self.buffer = io.BytesIO()
        self.quote_stack: list[bytes] = []
        self.topic_handlers: dict[bytes, SubHandler] = {}
        self.last_received = time.time()

    async def _default_topic_handler(
        self, topic: bytes, message: bytes, timestamp: Optional[float] = None
    ) -> None:
        log.info(f"{topic.decode()} at {timestamp}: {message.decode()}")
        self.last_received = time.time()

    async def handle_sub_message(self, message: bytes) -> None:
        i = message.index(b" ")
        topic = message[0:i]
        if m := TIMESTAMP.match(message, i + 1):
            timestamp: float | None = float(m[1])
            i = m.end()
        else:
            timestamp = None
        asyncio.create_task(
            self.topic_handlers.get(topic, self.default_topic_handler)(
                topic, message[i + 1 :], timestamp=timestamp
            )
        )

    async def parse_message(self, ds: bytes) -> None:
        if ds.startswith((b"ERRor", b"OK", b"NEXT")):
            ms = ds.index(b" ")
            r = None
            comfut_new = None
            if ds.startswith(b"NEXT"):
                loop = asyncio.get_running_loop()
                comfut_new = loop.create_future()
            for i, (commref, comfut) in enumerate(self.waiting_commands):
                if ds.startswith(commref, ms + 1):
                    if comfut is not None:
                        comfut.set_result(
                            (ds[:ms], ds[ms + len(commref) + 2 :], comfut_new)
                        )
                    else:
                        log.info(f"{commref!r} complete: {ds!r}")
                    r = i
                    break
            if r is None:
                log.error(f"received unexpected command response: {ds!r}")
            elif ds.startswith(b"NEXT"):
                self.waiting_commands.append((self.waiting_commands[r][0], comfut_new))
                del self.waiting_commands[r]
            else:
                del self.waiting_commands[r]
        elif ds.startswith(b"MESSage"):
            await self.handle_sub_message(ds[8:])
        elif ds.startswith(b"READy"):
            self.readymsg.set_result(ds.decode())
        else:
            log.error(f"Unknown message: {ds!r}")

    def data_received(self, data: bytes) -> None:
        """Process received data packet from instrument, keeping track of quotes. If
        a newline occurs when the quote stack is empty, create a task to process
        the message, but continue processing. (TODO: consider threads/processes here.)

        :param data: bytes:

        """
        lastwrite = 0
        for m in NL_OR_Q.finditer(data):
            if m[0] == b"\n":
                if len(self.quote_stack) == 0:
                    self.buffer.write(data[lastwrite : m.end()])
                    lastwrite = m.end()
                    asyncio.create_task(self.parse_message(self.buffer.getvalue()))
                    self.buffer = io.BytesIO()
                # else:  # This is not actually needed
                #     continue
            else:
                if not m[1]:
                    self.quote_stack.append(m[2])
                else:
                    try:
                        i = self.quote_stack.index(m[2])
                    except ValueError:
                        log.error(
                            f"Close quote {m[2]!r} did not have open in stack {self.quote_stack}. Disconnecting to avoid corruption."
                        )
                        self.quote_stack = []
                        self.connection_lost(ConnectionError())
                    else:
                        self.quote_stack = self.quote_stack[:i]
                self.buffer.write(data[lastwrite : m.end()])
                lastwrite = m.end()
        self.buffer.write(data[lastwrite:])
        self.last_received = time.time()

    async def run_command(
        self,
        comm: str | bytes,
        ack_timeout: int = 300,
        just_ack: bool = True,
        uid: bool = True,
    ) -> bytes:
        if isinstance(comm, str):
            comm = comm.encode()
        log.debug(f"Running command {comm.decode()}")
        loop = asyncio.get_running_loop()

        comfut = loop.create_future()
        if uid:
            import random

            commref = str(random.randint(1, 2 ** 30)).encode()
            comm = commref + b" " + comm
        self.transport.write((comm + b"\n"))
        log.debug(f"Sent command {comm!r}")
        if m := re.match(rb"^(\d+) ", comm):
            commref = m[1]
        else:
            commref = comm
        self.waiting_commands.append((commref, comfut))

        try:
            await asyncio.wait_for(asyncio.shield(comfut), ack_timeout)
        except asyncio.CancelledError:
            raise ConnectionError

        state, msg, comnext = comfut.result()
        log.debug(f"Received ({state!r}, {msg!r})")

        if state == b"NEXT":
            if just_ack:
                # self.waiting_commands.append((commref, None))
                return b""
            else:
                await comnext
                state, msg, comnext2 = comnext.result()
                assert comnext2 is None
                log.debug(f"Received ({state!r}, {msg!r})")

        if state == b"OK":
            return msg
        elif state == b"ERRor":
            raise CommandError(
                comm.decode(), commref.decode(), msg.decode().rstrip()
            ) from None
        else:  # pragma: no cover
            raise CommandError(
                comm.decode(), commref.decode(), (state + b" " + msg).decode()
            )
