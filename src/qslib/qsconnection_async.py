from __future__ import annotations
import asyncio
from typing import Any, Optional, List, Tuple, Mapping, Union
from typing_extensions import Literal
import hmac
import io
from dataclasses import dataclass
import re
import base64
from . import data

AccessLevel = Literal["Guest", "Observer", "Controller", "Administrator", "Full"]


def _gen_auth_response(password: str, challenge_string: str) -> str:
    return hmac.digest(password.encode(), challenge_string.encode(), "md5").hex()


def _parse_argstring(argstring: str) -> Mapping[str, str]:
    unparsed = argstring.split()

    args = dict()
    # FIXME: do quotes allow spaces?
    for u in unparsed:
        m = re.match("-([^=]+)=(.*)$", u)
        if m is None:
            raise ValueError(f"Can't parse {u} in argstring.", u)
        args[m[1]] = m[2]

    return args


def _parse_fd_fn(x: str) -> Tuple[str, int, int, int, int]:
    s = re.search(r"S(\d{2})_C(\d{3})_T(\d{2})_P(\d{4})_M(\d)_X(\d)_filterdata.xml$", x)
    if s is None:
        raise ValueError
    return (f"x{s[6]}-m{s[5]}", int(s[1]), int(s[2]), int(s[3]), int(s[4]))


def _index_to_filename_ref(i: Tuple[str, int, int, int, int]) -> str:
    x, s, c, t, p = i
    return f"S{s:02}_C{c:03}_T{t:02}_P{p:04}_M{x[4]}_X{x[1]}"


class Error(Exception):
    pass


@dataclass
class CommandError(Error):
    command: Optional[str]
    ref_index: Optional[str]
    response: str


def _validate_command_format(commandstring: str) -> None:
    # This is meant to validate that the command will not mess up comms
    # The command may be completely malformed otherwise

    tagseq: List[Tuple[str, str, str]] = re.findall(
        r"<(/?)([\w.]+)[ *]*>|(\n)", commandstring.rstrip()
    )  # tuple of close?,tag,newline?

    tagstack: List[str] = []
    for c, t, n in tagseq:
        if n and not tagstack:
            raise ValueError("newline outside of quotation")
        elif t and not c:
            tagstack.append(t)
        elif c:
            if not tagstack:
                raise ValueError(f"unbalanced tag <{c}{t}>")
            opentag = tagstack.pop()
            if opentag != t:
                raise ValueError(f"unbalanced tags <{opentag}> <{c}{t}>")
        elif n:
            continue
        else:
            raise ValueError("Unknown")
    if tagstack:
        raise ValueError("Unclosed tags")


def _parse_command_reply(
    responsestring: bytes,
    command: Optional[str] = None,
    ref_index: Optional[str] = None,
) -> bytes:
    if (command is None) and (ref_index is None):
        raise TypeError("Must give either command or ref_index")
        # todo: handle commond/refindex problem here
    if responsestring.startswith(b"OK "):
        if ref_index:
            ref_index_b = str(ref_index).encode()
            if not responsestring[3:].startswith(ref_index_b):
                raise ValueError("E")  # todo
            else:
                return responsestring[3 + len(ref_index) :].rstrip()
        elif command:
            if not responsestring[3:].startswith(command.encode()):
                raise ValueError("E")  # todo
            else:
                return responsestring[3 + len(command.rstrip().encode()) + 1 :].rstrip()
    elif responsestring.startswith(b"ERRor"):
        raise CommandError(command, ref_index, responsestring.decode())

    raise NotImplementedError


class QSConnectionAsync:
    """Class for connection to a QuantStudio instrument server, using asyncio"""

    async def __aenter__(self) -> QSConnectionAsync:
        await self.connect()
        return self

    async def __aexit__(self, exc_type: type, exc: Error, tb: Any) -> None:
        self._writer.close()
        await self._writer.wait_closed()

    def __init__(
        self,
        host: str = "localhost",
        port: int = 7000,
        authenticate_on_connect: bool = True,
        initial_access_level: AccessLevel = "Observer",
        password: Optional[str] = None,
    ):
        """Create a connection to a QuantStudio Instrument Server."""
        self.host = host
        self.port = port
        self.connected = False
        self.password = password
        self._initial_access_level = initial_access_level
        self._authenticate_on_connect = authenticate_on_connect

    def _parse_access_line(self, aline: str) -> None:
        # pylint: disable=attribute-defined-outside-init
        if not aline.startswith("READy"):
            raise ConnectionError(f"Server opening seems invalid: {aline}")
        args = _parse_argstring(aline[5:])
        self.session = int(args["session"])
        self.product = args["product"]
        self.server_version = args["version"]
        self.server_build = args["build"]
        self.server_capabilities = args["capabilities"]
        self.server_hello_args = args

    async def connect(
        self,
        authenticate: Optional[bool] = None,
        initial_access_level: Literal[AccessLevel, None] = None,
        password: Optional[str] = None,
    ) -> str:

        if authenticate is not None:
            self._authenticate_on_connect = authenticate
        if password is not None:
            self.password = password
        if initial_access_level is not None:
            self._initial_access_level = initial_access_level

        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)

        resp = (await self.get_reply()).decode()
        self._parse_access_line(resp)

        if self._authenticate_on_connect:
            if self.password is None:
                raise ValueError("no password provided")
            await self.authenticate(self.password)

        if self._initial_access_level is not None:
            await self.set_access_level(self._initial_access_level)

        self.connected = True

        return resp

    async def get_reply(self, timeout: Optional[float] = 10.0) -> bytes:
        s = io.BytesIO()

        quote_stack = []

        while True:
            line = await asyncio.wait_for(self._reader.readline(), timeout)
            s.write(line)
            m = re.findall(rb"<(/?)([\w.]+)[ *]*>", line)
            for c, t in m:
                if not c:
                    quote_stack.append(t)
                else:
                    assert quote_stack[-1] == t
                    quote_stack.pop()
            if not quote_stack:
                break
        return s.getvalue()

    async def send_command_raw(
        self, command: str, timeout: Optional[float] = 10.0
    ) -> None:
        self._writer.write(command.encode())
        await asyncio.wait_for(self._writer.drain(), timeout)

    async def run_command_to_bytes(
        self,
        command: str,
        ref_index: Optional[str] = None,
        use_uuid: bool = False,
        timeout: Optional[float] = 10.0,
    ) -> bytes:
        command = command.rstrip()

        _validate_command_format(command)

        m = re.match(r"^(\d+) ", command)
        if m:
            if ref_index:
                assert int(ref_index) == int(m[1])
            else:
                ref_index = m[1]
            command = command[m.endpos :]

        if ref_index:
            command = str(ref_index) + " " + command

        await self.send_command_raw(command + "\n", timeout)

        resp = await self.get_reply(timeout)

        return _parse_command_reply(resp, command, ref_index)

    async def run_command(
        self,
        command: str,
        ref_index: Optional[str] = None,
        use_uuid: bool = False,
        timeout: Optional[float] = 10.0,
    ) -> str:
        return (
            await self.run_command_to_bytes(command, ref_index, use_uuid, timeout)
        ).decode()

    async def authenticate(self, password: str) -> str:
        challenge_key = await self.run_command("CHAL?")
        auth_rep = _gen_auth_response(password, challenge_key)
        return await self.run_command(f"AUTH {auth_rep}")

    async def set_access_level(self, level: AccessLevel) -> str:
        return await self.run_command("ACC " + level)

    async def get_expfile_list(self, glob: str) -> List[str]:
        fl = await self.run_command(f"EXP:LIST? {glob}")
        assert fl.startswith("<quote.reply>")
        assert fl.endswith("</quote.reply>")
        return fl.split("\n")[1:-1]

    async def get_run_title(self) -> str:
        return await self.run_command(f"RUNTitle?")

    async def get_exp_file(
        self, path: str, encoding: Literal["plain", "base64"] = "base64"
    ) -> bytes:
        reply = await self.run_command_to_bytes(
            f"EXP:READ? -encoding={encoding} {path}"
        )
        assert reply.startswith(b"<quote>\n")
        assert reply.endswith(b"</quote>")
        r = reply[8:-8]
        if encoding == "base64":
            return base64.decodebytes(r)
        else:
            return r

    async def get_sds_file(
        self,
        path: str,
        runtitle: Optional[str] = None,
        encoding: Literal["base64", "plain"] = "base64",
    ) -> bytes:
        if runtitle is None:
            runtitle = await self.get_run_title()
        return await self.get_exp_file(f"{runtitle}/apldbio/sds/{path}", encoding)

    async def get_run_start_time(self) -> float:
        return float(await self.run_command("RET ${RunStartTime:--}"))

    async def get_filterdata_one(
        self,
        filterset: Union[data.FilterSet, str],
        stage: int,
        cycle: int,
        step: int,
        point: int = 1,
        run: Optional[str] = None,
    ) -> data.FilterDataReading:
        if run is None:
            run = await self.get_run_title()
        if isinstance(filterset, str):
            filterset_r = data.FilterSet.fromstring(filterset)
        else:
            filterset_r = filterset

        fl = await self.get_exp_file(
            f"{run}/apldbio/sds/filter/S{stage:02}_C{cycle:03}"
            f"_T{step:02}_P{point:04}_M{filterset_r.em}"
            f"_X{filterset_r.ex}_filterdata.xml"
        )

        f = data.FilterDataReading(fl)

        ql = (
            await self.get_expfile_list(
                f"{run}/apldbio/sds/quant/" f"{f.filename_reading_string}_E*.quant"
            )
        )[-1]
        qf = await self.get_exp_file(ql)

        f.set_time_by_quantdata(qf.decode())

        return f

    async def get_all_filterdata(
        self, run: Optional[str] = None
    ) -> data.FilterDataCollection:
        if run is None:
            run = await self.get_run_title()

        pl = [
            await self.get_filterdata_one(*_parse_fd_fn(x))
            for x in await self.get_expfile_list(
                f"{run}/apldbio/sds/filter/*_filterdata.xml"
            )
        ]

        return data.FilterDataCollection.from_readings(pl)  # type:ignore
