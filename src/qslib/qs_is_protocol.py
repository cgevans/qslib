import asyncio, logging, re, io

NL_OR_Q = re.compile(rb"(?:\n|<(/?)([\w.]+)[ *]*>)")
Q_ONLY = re.compile(rb"<(/?)([\w.]+)[ *]*>")
TIMESTAMP = re.compile(rb"(\d{8,}\.\d{3})")

log = logging.getLogger("qsproto")


class QS_IS_Protocol(asyncio.Protocol):
    def __init__(self):
        self.default_topic_handler = self._default_topic_handler
        self.readymsg = asyncio.get_running_loop().create_future()

    def connection_made(self, transport):
        # setup connection.
        self.transport = transport
        self.messages = []
        self.waiting_commands = []
        self.buffer = io.BytesIO()
        self.quote_stack = []
        self.topic_handlers = {}
        pass

    async def _default_topic_handler(self, topic, message, timestamp=None):
        print(f"{topic} at {timestamp}: {message}")

    async def handle_sub_message(self, message):
        i = message.index(b" ")
        topic = message[0:i]
        if m := TIMESTAMP.match(message, i + 1):
            timestamp = float(m[1])
            i = m.end() + 1
        else:
            timestamp = None
        asyncio.create_task(
            self.topic_handlers.get(topic, self.default_topic_handler)(
                topic, message[i + 1 :], timestamp=timestamp
            )
        )

    async def parse_message(self, ds):
        if ds.startswith((b"ERRor", b"OK", b"NEXT")):
            ms = ds.index(b" ")
            r = None
            for i, (commref, comfut) in enumerate(self.waiting_commands):
                if ds.startswith(commref, ms + 1):
                    comfut.set_result((ds[:ms], ds[ms + len(commref) + 2 :]))
                    r = i
                    break
            if r is None:
                log.error(f"received unexpected command response: {ds}")
            else:
                del self.waiting_commands[r]
        elif ds.startswith(b"MESSage"):
            await self.handle_sub_message(ds[8:])
        elif ds.startswith(b"READy"):
            self.readymsg.set_result(ds.decode())
        else:
            print(ds)

    def data_received(self, data):
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
                            f"Close quote {m[2]} did not have open in stack {self.quote_stack}."
                        ) from None
                    else:
                        self.quote_stack = self.quote_stack[:i]
                self.buffer.write(data[lastwrite : m.end()])
                lastwrite = m.end()

    async def run_command(self, comm):
        loop = asyncio.get_running_loop()

        comfut = loop.create_future()
        self.transport.write((comm + "\n").encode())
        if m := re.match(r"^(\d+) ", comm):
            commref = m[1].encode()
        else:
            commref = comm.encode()
        self.waiting_commands.append((commref, comfut))

        await asyncio.wait_for(asyncio.shield(comfut), 30)
        state, msg = comfut.result()

        if state == b"NEXT":
            comnext = loop.create_future()
            self.waiting_commands.append((commref, comnext))
            await comnext
            state, msg = comnext.result()

        if state == b"OK":
            return msg
        elif state == b"ERRor":
            raise ValueError(msg.decode().rstrip()) from None
        else:
            raise ValueError((state, msg))
