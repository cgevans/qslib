from __future__ import annotations
import asyncio
from asyncio.futures import Future
import logging
import re
import io
from dataclasses import dataclass
from typing import Any, Coroutine, Optional, Protocol


NL_OR_Q = re.compile(rb"(?:\n|<(/?)([\w.]+)[ *]*>)")
Q_ONLY = re.compile(rb"<(/?)([\w.]+)[ *]*>")
TIMESTAMP = re.compile(rb"(\d{8,}\.\d{3})")

log = logging.getLogger("qsproto")


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
    ) -> Coroutine[None, None, None]:
        ...


class QS_IS_Protocol(asyncio.Protocol):
    def __init__(self) -> None:
        self.default_topic_handler = self._default_topic_handler
        self.readymsg = asyncio.get_running_loop().create_future()
        self.lostconnection = asyncio.get_running_loop().create_future()
        self.should_be_connected = False

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if self.should_be_connected:
            log.warn("Lost connection")
        else:
            log.info("Connection closed")
        self.lostconnection.set_result(exc)
        self.should_be_connected = False

    async def disconnect(self) -> None:
        self.should_be_connected = False
        await self.run_command("QUIT")

    def connection_made(self, transport: Any) -> None:
        log.info("Made connection")
        self.should_be_connected = True
        # setup connection.
        self.transport = transport
        self.waiting_commands: list[tuple[bytes, None | Future[tuple[bytes, bytes]]]] = []
        self.buffer = io.BytesIO()
        self.quote_stack: list[bytes] = []
        self.topic_handlers: dict[bytes, SubHandler] = {}
        pass

    async def _default_topic_handler(
        self, topic: bytes, message: bytes, timestamp: Optional[float] = None
    ) -> None:
        log.info(f"{topic.decode()} at {timestamp}: {message.decode()}")

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
            for i, (commref, comfut) in enumerate(self.waiting_commands):
                if ds.startswith(commref, ms + 1):
                    if comfut is not None:
                        comfut.set_result((ds[:ms], ds[ms + len(commref) + 2 :]))
                    else:
                        log.info(f"{commref!r} complete: {ds!r}")
                    r = i
                    break
            if r is None:
                log.error(f"received unexpected command response: {ds!r}")
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
                else:
                    continue
            else:
                if not m[1]:
                    self.quote_stack.append(m[2])
                else:
                    try:
                        i = self.quote_stack.index(m[2])
                    except ValueError:
                        raise ValueError(
                            f"Close quote {m[2]!r} did not have open"
                            f" in stack {self.quote_stack}."
                        ) from None
                    else:
                        self.quote_stack = self.quote_stack[:i]
                self.buffer.write(data[lastwrite : m.end()])
                lastwrite = m.end()
        self.buffer.write(data[lastwrite:])

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

        comfut: Future[tuple[bytes, bytes]] = loop.create_future()
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

        await asyncio.wait_for(asyncio.shield(comfut), ack_timeout)
        state, msg = comfut.result()
        log.debug(f"Received ({state!r}, {msg!r})")

        if state == b"NEXT":
            if just_ack:
                self.waiting_commands.append((commref, None))
                return msg
            else:
                comnext = loop.create_future()
                self.waiting_commands.append((commref, comnext))
                await comnext
                state, msg = comnext.result()
                log.debug(f"Received ({state!r}, {msg!r})")

        if state == b"OK":
            return msg
        elif state == b"ERRor":
            raise CommandError(
                comm.decode(), commref.decode(), msg.decode().rstrip()
            ) from None
        else:
            raise CommandError(
                comm.decode(), commref.decode(), (state + b" " + msg).decode()
            )
