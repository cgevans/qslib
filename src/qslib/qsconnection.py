# SPDX-FileCopyrightText: 2021 - 2023 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

from __future__ import annotations

import base64
import hmac
import io
import logging
import re
import shlex
import ssl
import time
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Union, cast, overload

import pandas as pd

from . import data


from typing import Any, Coroutine, Literal, Optional, Protocol, Sequence, Type

from qslib.scpi_commands import AccessLevel, SCPICommand, _arglist

NL_OR_Q = re.compile(rb"(?:\n|<(/?)([\w.]+)[ *]*>?)")
TIMESTAMP = re.compile(rb"(\d{8,}\.\d{3})")

log = logging.getLogger(__name__)


def _validate_command_format(commandstring: bytes) -> None:
    # This is meant to validate that the command will not mess up comms
    # The command may be completely malformed otherwise

    tagseq: list[tuple[bytes, bytes, bytes]] = re.findall(
        rb"<(/?)([\w.]+)[ *]*>|(\n)", commandstring.rstrip()
    )  # tuple of close?,tag,newline?

    tagstack: list[bytes] = []
    for c, t, n in tagseq:
        if n and not tagstack:
            raise ValueError("newline outside of quotation")
        elif t and not c:
            tagstack.append(t)
        elif c:
            if not tagstack:
                raise ValueError(f"unbalanced tag <{c.decode()}{t.decode()}>")
            opentag = tagstack.pop()
            if opentag != t:
                raise ValueError(f"unbalanced tags <{opentag.decode()}> <{c.decode()}{t.decode()}>")
        elif n:
            continue
        else:
            raise ValueError("Unknown")
    if tagstack:
        raise ValueError("Unclosed tags")


class Error(Exception):
    pass


class CommandError(Error):
    @staticmethod
    def parse(command: str, ref_index: str, response: str) -> CommandError:
        m = re.match(r"\[(\w+)\] (.*)", response)
        if (not m) or (m[1] not in COM_ERRORS):
            return UnparsedCommandError(command, ref_index, response)
        try:
            return COM_ERRORS[m[1]].parse(command, ref_index, m[2])
        except ValueError:
            return UnparsedCommandError(command, ref_index, response)


@dataclass
class UnparsedCommandError(CommandError):
    """The machine has returned an error that we are not familiar with,
    and that we haven't parsed."""

    command: Optional[str]
    ref_index: Optional[str]
    response: str


@dataclass
class QS_IOError(CommandError):
    command: str
    message: str
    data: dict[str, str]

    @classmethod
    def parse(cls, command: str, ref_index: str, message: str) -> QS_IOError:
        m = re.match(r"(.*) --> (.*)", message)
        if not m:
            raise ValueError

        data = _arglist.parse_string(m[1])[0].opts

        return cls(command, m[2], data)


@dataclass
class InsufficientAccess(CommandError):
    command: str
    requiredAccess: AccessLevel
    currentAccess: AccessLevel
    message: str

    @classmethod
    def parse(cls, command: str, ref_index: str, message: str) -> InsufficientAccess:
        m = re.match(r'-requiredAccess="(\w+)" -currentAccess="(\w+)" --> (.*)', message)
        if not m:
            raise ValueError
        return cls(command, AccessLevel(m[1]), AccessLevel(m[2]), m[3])


@dataclass
class AuthError(CommandError):
    command: str
    message: str

    @classmethod
    def parse(cls, command: str, ref_index: str, message: str) -> AuthError:
        m = re.match(r"--> (.*)", message)
        if not m:
            raise ValueError
        return cls(command, m[1])


@dataclass
class AccessLevelExceeded(CommandError):
    command: str
    accessLimit: AccessLevel
    message: str

    @classmethod
    def parse(cls, command: str, ref_index: str, message: str) -> AccessLevelExceeded:
        m = re.match(r'-accessLimit="(\w+)" --> (.*)', message)
        if not m:
            raise ValueError
        return cls(command, AccessLevel(m[1]), m[2])


@dataclass
class InvocationError(CommandError):
    command: str
    message: str

    @classmethod
    def parse(cls, command: str, ref_index: str, message: str) -> InvocationError:
        return cls(command, message)


@dataclass
class NoMatch(CommandError):
    command: str
    message: str

    @classmethod
    def parse(cls, command: str, ref_index: str, message: str) -> NoMatch:
        return cls(command, message)


COM_ERRORS: dict[str, Type[CommandError]] = {
    "InsufficientAccess": InsufficientAccess,
    "AuthError": AuthError,
    "AccessLevelExceeded": AccessLevelExceeded,
    "InvocationError": InvocationError,
    "NoMatch": NoMatch,
    "IOError": QS_IOError,
}


class ReplyError(IOError):
    pass


class SubHandler(Protocol):
    def __call__(
        self, topic: bytes, message: bytes, timestamp: float | None
    ) -> Coroutine[None, None, None]:  # pragma: no cover
        ...


import socket
from queue import Queue
from threading import Event, Thread
import select


@dataclass
class QSCommand:
    ref: bytes
    ret_queue: Queue | None


@dataclass
class QSReturn:
    d: Literal[b"NEXT", b"OK", b"ERRor"]
    v: bytes
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
        return (
            f"S{self.stage:02}_C{self.cycle:03}_T{self.step:02}_P{self.point:04}"
            f"_M{self.filterset.em}_X{self.filterset.ex}_filterdata.xml"
        )

    def is_same_point(self, other: FilterDataFilename) -> bool:
        return (
            (self.stage == other.stage)
            and (self.cycle == other.cycle)
            and (self.step == other.step)
            and (self.point == other.point)
        )


class QSConnection:
    """Class for connection to a QuantStudio instrument server, using asyncio"""

    def __enter__(self) -> QSConnection:
        self.connect()
        return self

    def __exit__(self, exc_type: type, exc: Error, tb: Any) -> None:
        self.disconnect()

    @property
    def connected(self) -> bool:
        if self.qs_socket:
            return True
        else:
            return False

    @overload
    def list_files(
        self,
        path: str,
        *,
        leaf: str = "FILE",
        verbose: Literal[True],
        recursive: bool = False,
    ) -> list[dict[str, Any]]:
        ...

    @overload
    def list_files(
        self,
        path: str,
        *,
        leaf: str = "FILE",
        verbose: Literal[False],
        recursive: bool = False,
    ) -> list[str]:
        ...

    @overload
    def list_files(
        self,
        path: str,
        *,
        leaf: str = "FILE",
        verbose: bool = False,
        recursive: bool = False,
    ) -> list[str] | list[dict[str, Any]]:
        ...

    def list_files(
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
            return (self.run_command(f"{leaf}:LIST? {path}")).split("\n")[1:-1]
        else:
            v = (self.run_command(f"{leaf}:LIST? -verbose {path}")).split("\n")[
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
                    ret += self.list_files(
                        cast(str, d["path"]), leaf=leaf, verbose=True, recursive=True
                    )
                else:
                    ret.append(d)
            return ret

    def compile_eds(self, run_name: str) -> None:
        """Take a finished run directory in experiments:, compile it into an EDS, and move it to
        public_run_complete:"""

        expfiles = self.list_files("", leaf="experiment", verbose=True)

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

        self.run_command(
            f'exp:run -asynchronous <block> zip "{run_name}.eds" "{run_name}" </block>'
        )

        self.run_command(
            f'file:move "experiments:{run_name}.eds" "public_run_complete:{run_name}.eds"'
        )

        self.run_command(f'exp:attr= "{run_name}" collected True')

    def __init__(
        self,
        host: str = "localhost",
        port: int | None = None,
        ssl: bool | None = None,
        authenticate_on_connect: bool = True,
        initial_access_level: AccessLevel = AccessLevel.Observer,
        password: Optional[str] = None,
        client_certificate_path: Optional[str] = None,
        server_ca_file: Optional[str] = None,
    ):
        """Create a connection to a QuantStudio Instrument Server."""
        self.should_be_connected = False
        self.last_received = time.time()
        self.host = host
        self.port = port
        self.ssl = ssl
        self.password = password
        self._initial_access_level = initial_access_level
        self._authenticate_on_connect = authenticate_on_connect
        self.client_certificate_path = client_certificate_path
        self.server_ca_file = server_ca_file
        self.qs_socket = None
        
        self.connect()
        
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

    def connect(
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

        self.ready = Event()

        CTX = ssl.create_default_context()
        CTX.check_hostname = False
        CTX.verify_mode = ssl.CERT_NONE
        CTX.minimum_version = (
            ssl.TLSVersion.SSLv3
        )  # Yes, we actually need this for QS5 connections
        if self.client_certificate_path is not None:
            CTX.load_cert_chain(self.client_certificate_path)
        if self.server_ca_file is not None:
            CTX.load_verify_locations(self.server_ca_file)
            CTX.verify_mode = ssl.CERT_REQUIRED

        self.close_socket, self.close_socket_trigger = socket.socketpair()
        self.qs_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if (self.ssl is None) and (self.port is None):
            try:
                rs = socket.create_connection((self.host, 7443))
                self.qs_socket = CTX.wrap_socket(rs)
                self.ssl = True
                self.port = 7443
            except OSError:
                self.qs_socket = socket.create_connection((self.host, 7000))
                self.ssl = False
                self.port = 7000
        elif (self.ssl is None) and (self.port is not None):
            if self.port == 7443:
                self.ssl = True
            elif self.port == 7000:
                self.ssl = False
            else:
                raise ValueError("Port must be 7443 or 7000 if SSL is not specified")
        elif (self.ssl is not None) and (self.port is None):
            if self.ssl:
                self.port = 7443
            else:
                self.port = 7000

        if self.ssl:
            rs = socket.create_connection((self.host, self.port))
            self.qs_socket = CTX.wrap_socket(rs)
        else:     
            self.qs_socket = socket.create_connection((self.host, self.port))       

        self.connection_thread = Thread(target=self.connection_loop)
        self.connection_thread.start()

        self.ready.wait()
        resp = self.readymsg

        self._parse_access_line(resp)

        if self._authenticate_on_connect:
            if self.password is not None:
                self.authenticate(self.password)

        if self._initial_access_level is not None:
            self.set_access_level(self._initial_access_level)

        return resp

    def connection_loop(self):
        log.info("Made connection")
        self.should_be_connected = True
        # setup connection.
        self.waiting_commands = []
        self.buffer = io.BytesIO()
        self.quote_stack: list[bytes] = []
        self.topic_handlers: dict[bytes, SubHandler] = {}
        self.last_received = time.time()
        self.unclosed_quote_pos: int | None = None

        soft_close = False

        while not soft_close:
            r, _, _ = select.select([self.qs_socket, self.close_socket], [], [])

            if len(r) > 1:
                soft_close = True
            elif self.close_socket in r:
                log.debug("Closing connection.")
                break

            data = self.qs_socket.recv(4096)
            
            if len(data) == 0:
                log.debug("Connection closed")
                break

            log.debug(f"Received {data!r}")

            # If we have an unclosed tag opener (<) in the buffer, add it to the data
            if self.unclosed_quote_pos is not None:
                self.buffer.write(data)
                self.buffer.seek(self.unclosed_quote_pos)
                data = self.buffer.read()
                self.buffer.truncate(self.unclosed_quote_pos)
                self.buffer.seek(self.unclosed_quote_pos)
                self.unclosed_quote_pos = None
                print(data)

            lastwrite = 0
            for m in NL_OR_Q.finditer(data):
                if m[0] == b"\n":
                    if len(self.quote_stack) == 0:
                        self.buffer.write(data[lastwrite : m.end()])
                        lastwrite = m.end()
                        self.parse_message(self.buffer.getvalue())
                        self.buffer = io.BytesIO()
                    # else:  # This is not actually needed
                    #     continue
                else:
                    if m[0][-1] != ord(">"):
                        if m.end() != len(data):
                            raise ValueError(data, m[0])
                        # We have an unclosed tag opener (<) at the end of the data
                        log.debug(f"Unclosed tag opener: {m[0]!r}")
                        self.buffer.write(data[lastwrite : m.start()])
                        self.unclosed_quote_pos = self.buffer.tell()
                        self.buffer.write(m[0])
                        lastwrite = m.end()
                    elif not m[1]:
                        self.quote_stack.append(m[2])
                    else:
                        try:
                            i = self.quote_stack.index(m[2])
                        except ValueError:
                            log.error(
                                f"Close quote {m[2]!r} did not have open in stack {self.quote_stack}. "
                                "Disconnecting to avoid corruption."
                            )
                            self.quote_stack = []
                            self.connection_lost(ConnectionError())
                        else:
                            self.quote_stack = self.quote_stack[:i]
                    self.buffer.write(data[lastwrite : m.end()])
                    lastwrite = m.end()
            self.buffer.write(data[lastwrite:])
            self.last_received = time.time()

    def handle_sub_message(self, message: bytes) -> None:
        i = message.index(b" ")
        topic = message[0:i]
        if m := TIMESTAMP.match(message, i + 1):
            timestamp: float | None = float(m[1])
            i = m.end()
        else:
            timestamp = None
        for q in self.message_queues.get(topic, self.default_message_queues):
            q.put({'topic': topic, 'message': message[i + 1 :], 'timestamp': timestamp})

    def parse_message(self, ds: bytes) -> None:
        if ds.startswith((b"ERRor", b"OK", b"NEXT")):
            ms = ds.index(b" ")
            r = None
            for i, qsc in enumerate(self.waiting_commands):
                if ds.startswith(qsc.ref, ms + 1):
                    if qsc.ret_queue is not None:
                        qsc.ret_queue.put(
                            QSReturn(ds[:ms], ds[ms + len(qsc.ref) + 2 :])  # type: ignore
                        )
                    else:
                        log.info(f"{qsc.ref!r} complete: {ds!r}")
                    r = i
                    break
            if r is None:
                log.error(f"received unexpected command response: {ds!r}")
            elif not ds.startswith(b"NEXT"):
                del self.waiting_commands[r]
        elif ds.startswith(b"MESSage"):
            self.handle_sub_message(ds[8:])
        elif ds.startswith(b"READy"):
            self.readymsg = ds.decode()
            self.ready.set()
        else:
            log.error(f"Unknown message: {ds!r}")
        
    def run_command_to_bytes(
        self,
        comm: str | bytes | SCPICommand,
        ack_timeout: int = 300,
        just_ack: bool = True,
        uid: bool = True,
    ) -> bytes:
        if isinstance(comm, str):
            comm = comm.encode()
        elif isinstance(comm, SCPICommand):
            comm = comm.to_string().encode()
        comm = comm.rstrip()
        _validate_command_format(comm)
        log.debug(f"Running command {comm.decode()}")

        q = Queue()
        if uid:
            import random

            commref = str(random.randint(1, 2**30)).encode()
            comm_with_ref = commref + b" " + comm
        else:
            comm_with_ref = comm
        self.qs_socket.sendall(comm_with_ref + b"\n")
        log.debug(f"Sent command {comm_with_ref!r}")
        if m := re.match(rb"^(\d+) ", comm_with_ref):
            commref = m[1]
        else:
            commref = comm
        self.waiting_commands.append(QSCommand(commref, q))

        ret: QSReturn = q.get()
        

        log.debug(f"Received ({ret.d!r}, {ret.v!r})")

        if ret.d == b"NEXT":
            if just_ack:
                return b""
            else:
                ret = q.get()
                assert ret.d != "NEXXT"
                log.debug(f"Received ({ret.d!r}, {ret.v!r})")

        if ret.d == b"OK":
            return ret.v.rstrip()
        elif ret.d == b"ERRor":
            raise CommandError.parse(comm.decode(), commref.decode(), ret.v.decode().rstrip()) from None
        else:  # pragma: no cover
            raise CommandError.parse(comm.decode(), commref.decode(), (ret.d + b" " + ret.v).decode())

    def run_command(
        self, command: str | bytes | SCPICommand, just_ack: bool = False
    ) -> str:
        try:
            return self.run_command_to_bytes(command, just_ack).decode()
        except CommandError as e:
            e.__traceback__ = None
            raise e

    def disconnect(self) -> None:
        self.should_be_connected = False
        if self.qs_socket:
            self.run_command("QUIT")
            self.close_socket_trigger.close()

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if self.should_be_connected:
            log.warn("Lost connection")
        else:
            log.info("Connection closed")
        # self.lostconnection.set_result(exc)
        # self.should_be_connected = False

        # # Cancel all futures; we'll never recover them.
        # for _, future in self.waiting_commands:
        #     if future is not None:
        #         future.cancel()

        self.waiting_commands = []


    def authenticate(self, password: str) -> None:
        challenge_key = self.run_command(SCPICommand("CHAL?"))
        auth_rep = _gen_auth_response(password, challenge_key)
        self.run_command(SCPICommand("AUTH", auth_rep))

    def set_access_level(self, level: AccessLevel) -> None:
        self.run_command(SCPICommand("ACC", level.value))

    def get_expfile_list(
        self, glob: str, allow_nomatch: bool = False
    ) -> List[str]:
        try:
            fl = self.run_command(SCPICommand("EXP:LIST?", glob))
        except NoMatch as ce:
            if allow_nomatch:
                return []
            else:
                raise ce
        else:
            assert fl.startswith("<quote.reply>")
            assert fl.endswith("</quote.reply>")
            return fl.split("\n")[1:-1]

    def get_run_title(self) -> str:
        return (self.run_command("RUNTitle?")).strip('"')

    def get_exp_file(
        self, path: str, encoding: Literal["plain", "base64"] = "base64"
    ) -> bytes:
        reply = self.run_command_to_bytes(
            f"EXP:READ? -encoding={encoding} {shlex.quote(path)}"
        )
        assert reply.startswith(b"<quote>\n")
        assert reply.endswith(b"</quote>")
        r = reply[8:-8]
        if encoding == "base64":
            return base64.decodebytes(r)
        else:
            return r

    def read_dir_as_zip(self, path: str, leaf: str = "FILE") -> zipfile.ZipFile:
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

        if (path[0] != '"') and (path[-1] != '"'):
            path = '"' + path + '"'

        x = self.run_command_to_bytes(f"{leaf}:ZIPREAD? {path}")

        return zipfile.ZipFile(io.BytesIO(base64.decodebytes(x[7:-8])))

    def read_file(
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
        reply = self.run_command_to_bytes(
            SCPICommand(f"{leaf}:READ?", contexts + path, encoding=encoding)
        )
        assert reply.startswith(b"<quote>\n")
        assert reply.endswith(b"</quote>")
        r = reply[8:-8]
        if encoding == "base64":
            return base64.decodebytes(r)
        else:
            return r

    def get_sds_file(
        self,
        path: str,
        runtitle: Optional[str] = None,
        encoding: Literal["base64", "plain"] = "base64",
    ) -> bytes:
        if runtitle is None:
            runtitle = self.get_run_title()
        return self.get_exp_file(f"{runtitle}/apldbio/sds/{path}", encoding)

    def get_run_start_time(self) -> float:
        return float(self.run_command("RET ${RunStartTime:--}"))

    @overload
    def get_filterdata_one(
        self,
        ref: FilterDataFilename,
        *,
        run: Optional[str] = None,
        return_files: Literal[True],
    ) -> tuple[data.FilterDataReading, list[tuple[str, bytes]]]:
        ...

    @overload
    def get_filterdata_one(
        self,
        ref: FilterDataFilename,
        *,
        run: Optional[str] = None,
        return_files: Literal[False] = False,
    ) -> data.FilterDataReading:
        ...

    def get_filterdata_one(
        self,
        ref: FilterDataFilename,
        *,
        run: Optional[str] = None,
        return_files: bool = False,
    ) -> data.FilterDataReading | tuple[
        data.FilterDataReading, list[tuple[str, bytes]]
    ]:
        if run is None:
            run = self.get_run_title()

        fl = self.get_exp_file(f"{run}/apldbio/sds/filter/" + ref.tostring())

        if (x := ET.parse(io.BytesIO(fl)).find("PlatePointData/PlateData")) is not None:
            f = data.FilterDataReading(x)
        else:
            raise ValueError("PlateData not found")

        ql = (
            self.get_expfile_list(
                f"{run}/apldbio/sds/quant/{f.filename_reading_string}_E*.quant"
            )
        )[-1]
        qf = self.get_exp_file(ql)

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
    def get_all_filterdata(
        self, run: Optional[str], as_list: Literal[True]
    ) -> List[data.FilterDataReading]:
        ...

    @overload
    def get_all_filterdata(
        self, run: Optional[str], as_list: Literal[False]
    ) -> pd.DataFrame:
        ...

    def get_all_filterdata(
        self, run: str | None = None, as_list: bool = False
    ) -> Union[pd.DataFrame, List[data.FilterDataReading]]:
        if run is None:
            run = self.get_run_title()

        pl = [
            self.get_filterdata_one(FilterDataFilename.fromstring(x))
            for x in self.get_expfile_list(
                f"{run}/apldbio/sds/filter/*_filterdata.xml"
            )
        ]

        if as_list:
            return pl

        return data.df_from_readings(pl)
