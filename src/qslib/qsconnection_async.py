# SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
#
# SPDX-License-Identifier: AGPL-3.0-only

from __future__ import annotations

import asyncio
import base64
import hmac
import io
import logging
import re
import shlex
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Union, cast, overload

import pandas as pd

from . import data
from .qs_is_protocol import CommandError, Error, NoMatch, QS_IS_Protocol
from .scpi_commands import AccessLevel, ArgList, SCPICommand

log = logging.getLogger(__name__)


def _gen_auth_response(password: str, challenge_string: str) -> str:
    return hmac.digest(password.encode(), challenge_string.encode(), "md5").hex()


def _parse_argstring(argstring: str) -> Dict[str, str]:
    unparsed = argstring.split()

    args: dict[str, str] = dict()
    # FIXME: do quotes allow spaces?
    for u in unparsed:
        m = re.match("-([^=]+)=(.*)$", u)
        if m is None:
            raise ValueError(f"Can't parse {u} in argstring.", u)
        args[m[1]] = m[2]

    return args


class AlreadyCollectedError(Exception):
    ...


class RunNotFinishedError(Exception):
    ...


@dataclass(frozen=True, order=True, eq=True)
class FilterDataFilename:
    filterset: data.FilterSet
    stage: int
    cycle: int
    step: int
    point: int

    @classmethod
    def fromstring(cls, x: str) -> FilterDataFilename:
        s = re.search(r"S(\d+)_C(\d+)_T(\d+)_P(\d+)_M(\d)_X(\d)_filterdata.xml$", x)
        if s is None:
            raise ValueError
        return cls(
            data.FilterSet.fromstring(f"x{s[6]}-m{s[5]}"),
            int(s[1]),
            int(s[2]),
            int(s[3]),
            int(s[4]),
        )

    def tostring(self) -> str:
        return f"S{self.stage:02}_C{self.cycle:03}_T{self.step:02}_P{self.point:04}_M{self.filterset.em}_X{self.filterset.ex}_filterdata.xml"

    def is_same_point(self, other: FilterDataFilename) -> bool:
        return (
            (self.stage == other.stage)
            and (self.cycle == other.cycle)
            and (self.step == other.step)
            and (self.point == other.point)
        )


class QSConnectionAsync:
    """Class for connection to a QuantStudio instrument server, using asyncio"""

    _protocol: QS_IS_Protocol

    async def __aenter__(self) -> QSConnectionAsync:
        await self.connect()
        return self

    async def __aexit__(self, exc_type: type, exc: Error, tb: Any) -> None:
        if self._transport.is_closing():
            return
        await self.disconnect()

    async def disconnect(self) -> None:
        if self._transport.is_closing():
            return
        await self._protocol.disconnect()
        self._transport.close()

    @property
    def connected(self) -> bool:
        if hasattr(self, "_protocol"):
            if self._protocol.lostconnection.done():
                return False
            else:
                return True
        else:
            return False

    @overload
    async def list_files(
        self,
        path: str,
        *,
        leaf: str = "FILE",
        verbose: Literal[True],
        recursive: bool = False,
    ) -> list[dict[str, Any]]:
        ...

    @overload
    async def list_files(
        self,
        path: str,
        *,
        leaf: str = "FILE",
        verbose: Literal[False],
        recursive: bool = False,
    ) -> list[str]:
        ...

    @overload
    async def list_files(
        self,
        path: str,
        *,
        leaf: str = "FILE",
        verbose: bool = False,
        recursive: bool = False,
    ) -> list[str] | list[dict[str, Any]]:
        ...

    async def list_files(
        self,
        path: str,
        *,
        leaf: str = "FILE",
        verbose: bool = False,
        recursive: bool = False,
    ) -> list[str] | list[dict[str, Any]]:
        if not verbose:
            if recursive:
                raise NotImplementedError
            return (await self.run_command(f"{leaf}:LIST? {path}")).split("\n")[1:-1]
        else:
            v = (await self.run_command(f"{leaf}:LIST? -verbose {path}")).split("\n")[
                1:-1
            ]
            ret: list[dict[str, str | float | int]] = []
            for x in v:
                rm = re.match(
                    r'"([^"]+)" -type=(\S+) -size=(\S+) -mtime=(\S+) -atime=(\S+) -ctime=(\S+)$',
                    x,
                )
                if rm is None:
                    ag = ArgList.from_string(x)
                    d: dict[str, str | float | int] = {}
                    d["path"] = ag.args[0]
                    d |= ag.opts
                else:
                    d = {}
                    d["path"] = rm.group(1)
                    d["type"] = rm.group(2)
                    d["size"] = int(rm.group(3))
                    d["mtime"] = float(rm.group(4))
                    d["atime"] = float(rm.group(5))
                    d["ctime"] = float(rm.group(6))
                if d["type"] == "folder" and recursive:
                    ret += await self.list_files(
                        cast(str, d["path"]), leaf=leaf, verbose=True, recursive=True
                    )
                else:
                    ret.append(d)
            return ret

    async def compile_eds(self, run_name: str) -> None:
        """Take a finished run directory in experiments:, compile it into an EDS, and move it to
        public_run_complete:"""

        expfiles = await self.list_files("", leaf="experiment", verbose=True)

        results = [r for r in expfiles if r["path"] == run_name]

        if len(results) != 1:
            raise ValueError
        res = results[0]

        if "run" not in res:
            raise FileNotFoundError(res)

        if res["state"] not in ["Completed", "Terminated"]:
            raise RunNotFinishedError(res)

        if ("collected" in res) and (res["collected"]):
            raise AlreadyCollectedError(res)

        await self.run_command(
            f'exp:run -asynchronous <block> zip "{run_name}.eds" "{run_name}" </block>'
        )

        await self.run_command(
            f'file:move "experiments:{run_name}.eds" "public_run_complete:{run_name}.eds"'
        )

        await self.run_command(f'exp:attr= "{run_name}" collected True')

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
        initial_access_level: AccessLevel | None = None,
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
            await self.set_access_level(self._initial_access_level)

        return resp

    async def run_command_to_bytes(
        self, command: str | bytes | SCPICommand, just_ack: bool = True
    ) -> bytes:
        try:
            return (
                await self._protocol.run_command(command, just_ack=just_ack)
            ).rstrip()
        except CommandError as e:
            e.__traceback__ = None
            raise e

    async def run_command(
        self, command: str | bytes | SCPICommand, just_ack: bool = False
    ) -> str:
        try:
            return (await self.run_command_to_bytes(command, just_ack)).decode()
        except CommandError as e:
            e.__traceback__ = None
            raise e

    async def authenticate(self, password: str) -> None:
        challenge_key = await self.run_command(SCPICommand("CHAL?"))
        auth_rep = _gen_auth_response(password, challenge_key)
        await self.run_command(SCPICommand("AUTH", auth_rep))

    async def set_access_level(self, level: AccessLevel) -> None:
        await self.run_command(SCPICommand("ACC", level.value))

    async def get_expfile_list(
        self, glob: str, allow_nomatch: bool = False
    ) -> List[str]:
        try:
            fl = await self.run_command(SCPICommand("EXP:LIST?", glob))
        except NoMatch as ce:
            if allow_nomatch:
                return []
            else:
                raise ce
        else:
            assert fl.startswith("<quote.reply>")
            assert fl.endswith("</quote.reply>")
            return fl.split("\n")[1:-1]

    async def get_run_title(self) -> str:
        return (await self.run_command("RUNTitle?")).strip('"')

    async def get_exp_file(
        self, path: str, encoding: Literal["plain", "base64"] = "base64"
    ) -> bytes:
        reply = await self.run_command_to_bytes(
            f"EXP:READ? -encoding={encoding} {shlex.quote(path)}"
        )
        assert reply.startswith(b"<quote>\n")
        assert reply.endswith(b"</quote>")
        r = reply[8:-8]
        if encoding == "base64":
            return base64.decodebytes(r)
        else:
            return r

    async def read_dir_as_zip(self, path: str, leaf: str = "FILE") -> zipfile.ZipFile:
        """Read a directory on the

        Parameters
        ----------
        path : str
            path on the machine
        leaf : str, optional
            leaf to use, by default "FILE"

        Returns
        -------
        zipfile.ZipFile
            the returned zip file
        """
        x = await self.run_command_to_bytes(f'{leaf}:ZIPREAD? "{path}"')

        return zipfile.ZipFile(io.BytesIO(base64.decodebytes(x[7:-8])))

    async def read_file(
        self,
        path: str,
        context: str | None = None,
        leaf: str = "FILE",
        encoding: Literal["plain", "base64"] = "base64",
    ) -> bytes:
        if not context:
            contexts = ""
        elif context[-1] == ":":
            contexts = context
        else:
            contexts = context + ":"
        reply = await self.run_command_to_bytes(
            SCPICommand(f"{leaf}:READ?", contexts + path, encoding=encoding)
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

    @overload
    async def get_filterdata_one(
        self,
        ref: FilterDataFilename,
        *,
        run: Optional[str] = None,
        return_files: Literal[True],
    ) -> tuple[data.FilterDataReading, list[tuple[str, bytes]]]:
        ...

    @overload
    async def get_filterdata_one(
        self,
        ref: FilterDataFilename,
        *,
        run: Optional[str] = None,
        return_files: Literal[False] = False,
    ) -> data.FilterDataReading:
        ...

    async def get_filterdata_one(
        self,
        ref: FilterDataFilename,
        *,
        run: Optional[str] = None,
        return_files: bool = False,
    ) -> data.FilterDataReading | tuple[
        data.FilterDataReading, list[tuple[str, bytes]]
    ]:
        if run is None:
            run = await self.get_run_title()

        fl = await self.get_exp_file(f"{run}/apldbio/sds/filter/" + ref.tostring())

        if (x := ET.parse(io.BytesIO(fl)).find("PlatePointData/PlateData")) is not None:
            f = data.FilterDataReading(x)
        else:
            raise ValueError("PlateData not found")

        ql = (
            await self.get_expfile_list(
                f"{run}/apldbio/sds/quant/{f.filename_reading_string}_E*.quant"
            )
        )[-1]
        qf = await self.get_exp_file(ql)

        f.set_timestamp_by_quantdata(qf.decode())

        if return_files:
            files = [("filter/" + ref.tostring(), fl)]
            qn = re.search("quant/.*$", ql)
            assert qn is not None
            files.append((qn[0], qf))
            return f, files
        else:
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

    async def get_all_filterdata(
        self, run: str | None = None, as_list: bool = False
    ) -> Union[pd.DataFrame, List[data.FilterDataReading]]:
        if run is None:
            run = await self.get_run_title()

        pl = [
            await self.get_filterdata_one(FilterDataFilename.fromstring(x))
            for x in await self.get_expfile_list(
                f"{run}/apldbio/sds/filter/*_filterdata.xml"
            )
        ]

        if as_list:
            return pl

        return data.df_from_readings(pl)
