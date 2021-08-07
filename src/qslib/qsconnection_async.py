from __future__ import annotations
import asyncio
import io
from typing import Any, Dict, Optional, List, Tuple, Union, Literal, cast, overload
import hmac
import re
import base64
from .qs_is_protocol import Error, QS_IS_Protocol

import qslib.data as data
import pandas as pd
import xml.etree.ElementTree as ET
from .base import AccessLevel


def _gen_auth_response(password: str, challenge_string: str) -> str:
    return hmac.digest(password.encode(), challenge_string.encode(), "md5").hex()


def _parse_argstring(argstring: str) -> Dict[str, str]:
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


class QSConnectionAsync:
    """Class for connection to a QuantStudio instrument server, using asyncio"""

    async def __aenter__(self) -> QSConnectionAsync:
        await self.connect()
        return self

    async def __aexit__(self, exc_type: type, exc: Error, tb: Any) -> None:
        if self._transport.is_closing():
            return
        await self.disconnect()

    async def disconnect(self):
        await self.run_command("QUIT")
        self._transport.close()
        self.connected = False

    def __init__(
        self,
        host: str = "localhost",
        port: int = 7000,
        authenticate_on_connect: bool = True,
        initial_access_level: AccessLevel = AccessLevel.Observer,
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

        self.loop = asyncio.get_running_loop()
        self._transport, proto = await self.loop.create_connection(
            QS_IS_Protocol, self.host, self.port
        )

        self._protocol = cast(QS_IS_Protocol, proto)

        await self._protocol.readymsg
        resp = self._protocol.readymsg.result()
        self._parse_access_line(resp)

        if self._authenticate_on_connect:
            if self.password is not None:
                await self.authenticate(self.password)

        if self._initial_access_level is not None:
            await self.set_access_level(cast(AccessLevel, self._initial_access_level))

        self.connected = True

        return resp

    async def run_command_to_bytes(self, command: str, just_ack: bool = True) -> bytes:
        command = command.rstrip()

        _validate_command_format(command)
        return (
            await self._protocol.run_command(
                command.rstrip().encode(), just_ack=just_ack
            )
        ).rstrip()

    async def run_command(self, command: str, just_ack: bool = False) -> str:
        return (await self.run_command_to_bytes(command, just_ack)).decode()

    async def authenticate(self, password: str):
        challenge_key = await self.run_command("CHAL?")
        auth_rep = _gen_auth_response(password, challenge_key)
        await self.run_command(f"AUTH {auth_rep}")

    async def set_access_level(self, level: AccessLevel):
        await self.run_command("ACC " + level.value)

    async def get_expfile_list(self, glob: str) -> List[str]:
        fl = await self.run_command(f"EXP:LIST? {glob}")
        assert fl.startswith("<quote.reply>")
        assert fl.endswith("</quote.reply>")
        return fl.split("\n")[1:-1]

    async def get_run_title(self) -> str:
        return await self.run_command("RUNTitle?")

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

    async def get_file(
        self, path: str, encoding: Literal["plain", "base64"] = "base64"
    ) -> bytes:
        reply = await self.run_command_to_bytes(
            f"FILE:READ? -encoding={encoding} {path}"
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
        filterset: Union[data.FilterSet, str],  # type: ignore
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

        f = data.FilterDataReading(ET.parse(io.BytesIO(fl)).find("PlatePointData/PlateData"))

        ql = (
            await self.get_expfile_list(
                f"{run}/apldbio/sds/quant/" f"{f.filename_reading_string}_E*.quant"
            )
        )[-1]
        qf = await self.get_exp_file(ql)

        f.set_timestamp_by_quantdata(qf.decode())

        return f

    @overload
    async def get_all_filterdata(
        self, run: Optional[str], as_list: Literal[True]
    ) -> List[data.FilterDataReading]:
        ...

    @overload
    async def get_all_filterdata(
        self, run: Optional[str], as_list: Literal[False]
    ) -> pd.DataFrame:
        ...

    async def get_all_filterdata(self, run=None, as_list=False):
        if run is None:
            run = await self.get_run_title()

        pl = [
            await self.get_filterdata_one(*_parse_fd_fn(x))
            for x in await self.get_expfile_list(
                f"{run}/apldbio/sds/filter/*_filterdata.xml"
            )
        ]

        if as_list:
            return pl

        return data.df_from_readings(pl)  # type:ignore
