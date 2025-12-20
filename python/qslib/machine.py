# SPDX-FileCopyrightText: 2021 - 2023 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

from __future__ import annotations

import base64
import logging
import random
import re
import shlex
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
from typing import IO, TYPE_CHECKING, Any, Generator, Literal, cast, overload
from datetime import datetime, timezone
from typing import TypedDict
from ._qslib import QSConnection, CommandError
import io
from .data import FilterSet, FilterDataReading, df_from_readings
import xml.etree.ElementTree as ET

if TYPE_CHECKING:
    import pandas as pd



from qslib.scpi_commands import AccessLevel, SCPICommand, ArgList

from ._util import _unwrap_tags
from .protocol import Protocol

from .base import MachineStatus, RunStatus  # noqa: E402

class FileListInfo(TypedDict, total=False):
    """Information about a file when verbose=True"""
    path: str
    type: str
    size: int
    mtime: datetime
    atime: datetime
    ctime: datetime
    state: str
    collected: bool


def _gen_auth_response(password: str, challenge_string: str) -> str:
    import hmac
    return hmac.digest(password.encode(), challenge_string.encode(), "md5").hex()


def _parse_argstring(argstring: str) -> dict[str, str]:
    unparsed = argstring.split()

    args: dict[str, str] = dict()
    # FIXME: do quotes allow spaces?
    for u in unparsed:
        m = re.match("-([^=]+)=(.*)$", u)
        if m is None:
            raise ValueError(f"Can't parse {u} in argstring.", u)
        args[m[1]] = m[2]

    return args


class AlreadyCollectedError(Exception): ...


class RunNotFinishedError(Exception): ...


@dataclass(frozen=True, order=True, eq=True)
class FilterDataFilename:
    filterset: FilterSet
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
            FilterSet.fromstring(f"x{s[6]}-m{s[5]}"),
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


log = logging.getLogger(__name__)


if TYPE_CHECKING:  # pragma: no cover
    import matplotlib.pyplot as plt  # noqa: F401

    from .experiment import Experiment


def _ensure_connection(level: AccessLevel = AccessLevel.Observer) -> Any:
    def wrap(func):
        @wraps(func)
        def wrapped(m: Machine, *args: Any, **kwargs: Any) -> Any:
            if m.automatic:
                with m.ensured_connection(level):
                    return func(m, *args, **kwargs)
            else:
                return func(m, *args, **kwargs)

        return wrapped

    return wrap


@dataclass(init=False)
class Machine:
    """
    A connection to a QuantStudio machine.  The connection can be opened and closed, and reused.
    A maximum access level can be set and changed, which will prevent the access level from going
    above that level.

    By default, the class tries to handle connections and access automatically.

    Parameters
    ----------

    host
        The host name or IP to connect to.

    password
        The password to use. Note that this class does not obscure or protect the password at all,
        because it should not be relied on for security.  See :ref:`access-and-security`  for more
        information.

    automatic
        Whether or not to automatically handle connection, disconnection, and where possible,
        access level.  Default True.

    max_access_level: "Observer", "Controller", "Administrator", or "Full"
        The maximum access level to allow.  This is *not* the initial access level, which
        will be Observer. The parameter can be changed later by changing the :code:`max_access_level`
        attribute.

    port
        The port to connect to.  If None, and ssl is None, then 7443 will be tried with SSL, and if
        it fails, then 7000 will be tried without SSL.

    ssl
        Whether or not to use SSL.  If None, then SSL will be chosen based on the port number.

    client_certificate_path
        Path to a PEM file containing the client certificate for TLS client authentication.
        The file may also contain the private key, or it can be provided separately via
        client_key_path.

    client_key_path
        Path to a PEM file containing the client private key for TLS client authentication.
        Only needed if the key is not included in client_certificate_path.

    server_ca_file
        Path to a PEM file containing CA certificate(s) for verifying the server's certificate.
        If not provided, server certificate verification is disabled (default).

    tls_server_name
        Expected server name for TLS hostname verification. If server_ca_file is provided but
        tls_server_name is None, certificate chain verification is performed but hostname
        is not checked. This is useful when connecting through tunnels or port forwards where
        the connection hostname differs from the certificate's CN/SAN.
    """

    host: str
    password: str | None = None
    automatic: bool = True
    _max_access_level: AccessLevel = AccessLevel.Controller
    port: int | None = None
    ssl: bool | None = None
    _initial_access_level: AccessLevel = AccessLevel.Observer
    _current_access_level: AccessLevel = AccessLevel.Guest
    _connection: QSConnection | None = None

    def asdict(self, password: bool = False) -> dict[str, str | int | None]:
        d: dict[str, str | int | None] = {"host": self.host}
        if self.password and password:
            d["password"] = self.password
        if self.max_access_level != Machine._max_access_level:
            d["max_access_level"] = self.max_access_level.value
        if self.port != Machine.port:
            d["port"] = self.port
        if self.ssl != Machine.ssl:
            d["ssl"] = self.ssl
        if self.automatic != Machine.automatic:
            d["automatic"] = self.automatic

        return d

    @property
    def connection(self) -> QSConnection:
        """The :class:`QSConnection` for the connection, or a :class:`ConnectionError`."""
        if self._connection is None:
            raise ConnectionError
        else:
            return self._connection

    @connection.setter
    def connection(self, v: QSConnection | None) -> None:
        self._connection = v

    @property
    def max_access_level(self) -> AccessLevel:
        return self._max_access_level

    @max_access_level.setter
    def max_access_level(self, v: AccessLevel | str) -> None:
        if not isinstance(v, AccessLevel):
            self._max_access_level = AccessLevel(v)
        else:
            self._max_access_level = v

    def __init__(
        self,
        host: str,
        password: str | None = None,
        automatic: bool = True,
        max_access_level: AccessLevel | str = AccessLevel.Controller,
        port: int | None = None,
        ssl: bool | None = None,
        client_certificate_path: str | None = None,
        client_key_path: str | None = None,
        server_ca_file: str | None = None,
        tls_server_name: str | None = None,
        _initial_access_level: AccessLevel | str = AccessLevel.Observer,
    ):
        self.host = host
        self.ssl = ssl
        # Determine port based on ssl if not provided
        if port is not None:
            self.port = port
        else:
            if self.ssl is False:
                self.port = 7000
            else:
                self.port = 7443
        self.password = password
        self.automatic = automatic
        self.max_access_level = AccessLevel(max_access_level)
        self._initial_access_level = AccessLevel(_initial_access_level)
        self._connection = None
        self.client_certificate_path = client_certificate_path
        self.client_key_path = client_key_path
        self.server_ca_file = server_ca_file
        self.tls_server_name = tls_server_name

    def connect(self) -> None:
        """Open the connection manually."""

        # Determine connection type based on ssl parameter
        if self.ssl is True:
            connection_type = "SSL"
        elif self.ssl is False:
            connection_type = "TCP"
        else:
            connection_type = "Auto"

        self.connection = QSConnection(
            host=self.host,
            port=self.port,
            connection_type=connection_type,
            client_cert_path=self.client_certificate_path,
            client_key_path=self.client_key_path,
            server_ca_path=self.server_ca_file,
            tls_server_name=self.tls_server_name,
        )
        if self.password is not None:
            self.authenticate(self.password)
        if self._initial_access_level is not None:
            self.set_access_level(self._initial_access_level)
        self._current_access_level = self.get_access_level()[0]


    @property
    def connected(self) -> bool:
        """Whether or not there is a current connection to the machine.

        Note that when using automatic connections, this will usually be False,
        because connections will only be active when running a command.
        """
        if (not hasattr(self, "_connection")) or (self._connection is None):
            return False
        else:
            return self.connection.connected

    def __enter__(self) -> Machine:
        try:
            self.connect()
        except Exception as e:
            self.disconnect()
            raise e
        return self

    @_ensure_connection(AccessLevel.Guest)
    def run_command(self, command: str | SCPICommand) -> str:
        """Run a SCPI command, and return the response as a string.
        Waits for OK, not just NEXT.

        Parameters
        ----------
        command : str
            command to run

        Returns
        -------
        str
            Response message (after "OK", not including it)

        Raises
        ------
        CommandError
            Received an Error response.
        """
        match command:
            case str():
                return str(self.connection.run_command(command).get_response())
            case SCPICommand():
                return str(self.connection.run_command(command.to_string()).get_response())
            case _:
                raise ValueError(f"Invalid command: {command}")
        

    @_ensure_connection(AccessLevel.Guest)
    def run_command_to_bytes(self, command: str | SCPICommand) -> bytes:
        """Run an SCPI command, and return the response as bytes (undecoded).
        Waits for NEXT.
        """
        match command:
            case str():
                return self.connection.run_command(command).get_response_bytes()
            case SCPICommand():
                return self.connection.run_command(command.to_string()).get_response_bytes()
            case _:
                raise ValueError(f"Invalid command: {command}")

    @_ensure_connection(AccessLevel.Guest)
    def run_command_to_ack(self, command: str | SCPICommand) -> str:
        """Run an SCPI command, and return the response as a string.
        Returns after the command is processed (OK or NEXT), but potentially
        before it has completed (NEXT).

        Parameters
        ----------
        commands
            command to run

        Returns
        -------
        str
            Response message (after "OK" or "NEXT", likely "" in latter case)

        Raises
        ------
        CommandError
            Received an Error response.
        """
        if self.connection is None:
            raise ConnectionError(f"Not connected to {self.host}")
        try:
            return self.connection.run_command(command).get_ack()
        except ValueError as e: # FIXME
            e.__traceback__ = None
            raise e

    @_ensure_connection(AccessLevel.Guest)
    def run_command_bytes(self, command: str | bytes | SCPICommand) -> bytes:
        """Run an SCPI command, and return the response as bytes (undecoded).
        Returns after the command is processed (OK or NEXT), but potentially
        before it has completed (NEXT).

        Parameters
        ----------
        command
            command to run

        Returns
        -------
        bytes
            Response message (after "OK" or "NEXT", likely "" in latter case)

        Raises
        ------
        CommandError
            Received
        """
        if self.connection is None:
            raise ConnectionError(f"Not connected to {self.host}.")
        if isinstance(command, str):
            command = command.encode()
        return self.connection.run_command_bytes(command).get_response_bytes()

    @_ensure_connection(AccessLevel.Controller)
    def define_protocol(self, protocol: Protocol) -> None:
        """Send a protocol to the machine. This *is not related* to a particular
        experiment.  The name on the machine is set by the protocol.

        Parameters
        ----------
        protocol
            protocol to send
        """
        protocol.validate()
        self.run_command(protocol.to_scpicommand())

    @_ensure_connection(AccessLevel.Observer)
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


    @overload
    def list_files(
        self,
        path: str,
        *,
        leaf: str = "FILE",
        verbose: Literal[True],
        recursive: bool = False,
    ) -> list[FileListInfo]: ...

    @overload
    def list_files(
        self,
        path: str,
        *,
        leaf: str = "FILE",
        verbose: Literal[False] = False,
        recursive: bool = False,
    ) -> list[str]: ...

    @overload
    def list_files(
        self,
        path: str,
        *,
        leaf: str = "FILE",
        verbose: bool = False,
        recursive: bool = False,
    ) -> list[str] | list[FileListInfo]: ...

    @_ensure_connection(AccessLevel.Observer)
    def list_files(
        self,
        path: str,
        *,
        leaf: str = "FILE",
        verbose: bool = False,
        recursive: bool = False,
    ) -> list[str] | list[FileListInfo]:
        if not verbose:
            if recursive:
                raise NotImplementedError
            return (self.run_command(f"{leaf}:LIST? {path}")).split("\n")[1:-1]
        else:
            v = (self.run_command(f"{leaf}:LIST? -verbose {path}")).split("\n")[
                1:-1
            ]
            ret: list[FileListInfo] = []
            for x in v:
                rm = re.match(
                    r'"([^"]+)" -type=(\S+) -size=(\S+) -mtime=(\S+) -atime=(\S+) -ctime=(\S+)$',
                    x,
                )
                if rm is None:
                    ag = ArgList.from_string(x)
                    d: dict[str, Any] = {}
                    d["path"] = cast(str, ag.args[0])
                    d |= ag.opts
                else:
                    d = {}
                    d["path"] = rm.group(1)
                    d["type"] = rm.group(2)
                    d["size"] = int(rm.group(3))
                    d["mtime"] = datetime.fromtimestamp(float(rm.group(4)), tz=timezone.utc)
                    d["atime"] = datetime.fromtimestamp(float(rm.group(5)), tz=timezone.utc)
                    d["ctime"] = datetime.fromtimestamp(float(rm.group(6)), tz=timezone.utc)
                if d["type"] == "folder" and recursive:
                    ret += self.list_files(
                        cast(str, d["path"]), leaf=leaf, verbose=True, recursive=True
                    )
                else:
                    ret.append(cast(FileListInfo, d))
            return ret

    @_ensure_connection(AccessLevel.Observer)
    def read_file(
        self, path: str, context: str | None = None, leaf: str = "FILE", encoding: Literal["base64", "plain"] = "base64"
    ) -> bytes:
        """Read a file.

        Parameters
        ----------
        path : str
            File path on the machine.
        context : str | None (default None)
            Context.
        leaf: str (default FILE)

        Returns
        -------
        bytes
            returned file
        """
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

    @_ensure_connection(AccessLevel.Controller)
    def write_file(self, path: str, data: str | bytes) -> None:
        if isinstance(data, str):
            data = data.encode()

        self.run_command_bytes(
            b"FILE:WRITE "
            + path.encode()
            + b" <quote.base64>\n"
            + base64.encodebytes(data)
            + b"\n</quote.base64>"
        )

    @overload
    def list_runs_in_storage(self, glob: str = "*", *, verbose: Literal[True]) -> list[FileListInfo]: ...

    @overload
    def list_runs_in_storage(self, glob: str = "*", *, verbose: Literal[False] = False) -> list[str]: ...

    @overload
    def list_runs_in_storage(self, glob: str = "*", *, verbose: bool = False) -> list[str] | list[FileListInfo]: ...

    @_ensure_connection(AccessLevel.Observer)
    def list_runs_in_storage(
        self, glob: str = "*", *, verbose: bool = False
    ) -> list[str] | list[FileListInfo]:
        """List runs in machine storage.

        Returns
        -------
        list[str]
            run filenames.  Retrieve with load_run_from_storage
            (to open as :any`Experiment`) or save_run_from_storage
            (to download and save it without opening.)
        """
        if not glob.endswith("eds"):
            glob = f"{glob}eds"
        try:
            filelist = self.list_files(f"public_run_complete:{glob}", verbose=verbose)
        except CommandError as e:
            if e.args[0]['error'] == 'NoMatch':
                return []
            else:
                raise e
        if not verbose:
            return [
                re.sub("^public_run_complete:", "", s)[:-4]
                for s in filelist
            ]
        else:
            a = filelist
            for e in a:
                e["path"] = re.sub("^public_run_complete:", "", e["path"])[:-4]
            return a

    @_ensure_connection(AccessLevel.Observer)
    def load_run_from_storage(self, path: str) -> "Experiment":  # type: ignore
        from .experiment import Experiment

        """Load a run from machine storage as an Experiment
        """
        return Experiment.from_machine_storage(self, path)

    @_ensure_connection(AccessLevel.Guest)
    def save_run_from_storage(
        self, machine_path: str, download_path: str | IO[bytes], overwrite: bool = False
    ) -> None:
        """Download a file from run storage on the machine.

        Parameters
        ----------
        machine_path : str
            filename on the machine
        download_path : str | IO[bytes]
            filename to download to, or an open file
        overwrite : bool, optional
            if False and provided a filename rather than an
            open file, will not overwrite existing filies; by default
            False
        """
        fdata = self.read_file(machine_path, context="public_run_complete")

        if not isinstance(download_path, str):
            file = download_path
            file.write(fdata)
        else:
            if overwrite:
                file = open(download_path, "wb")
            else:
                file = open(download_path, "xb")
            try:
                file.write(fdata)
            finally:
                file.close()

    @_ensure_connection(AccessLevel.Observer)
    def _get_log_from_byte(self, name: str | bytes, byte: int) -> bytes:
        if self.connection is None:
            raise Exception
        if isinstance(name, bytes):
            name = name.decode()

        # Generate a random u32 for the log transfer command
        log_ident = random.randint(0, 2**32 - 1)

        log_responder = self.connection.expect_ident(log_ident)

        logcommand = self.connection.run_command(
            f"eval? session.writeQueue.put(('OK {log_ident} \\<quote.base64\\>\\\\n'"
            f" + (lambda x: [x.seek({byte}), __import__('base64').encodestring(x.read())][1])"
            f"(open('/data/vendor/IS/experiments/{name}/apldbio/sds/messages.log')) +"
            " '\\</quote.base64\\>\\\\n', None))",
        )


        logcommand.get_response()

        logres = log_responder.get_response()

        return base64.decodebytes(logres[15:-16].encode()) # FIXME: don't encode/decode, and make this more robust

    @_ensure_connection(AccessLevel.Observer)
    def run_status(self) -> RunStatus:
        """Return information on the status of any run."""
        return RunStatus.from_machine(self)

    @_ensure_connection(AccessLevel.Observer)
    def machine_status(self) -> MachineStatus:
        """Return information on the status of the machine."""
        return MachineStatus.from_machine(self)

    @_ensure_connection(AccessLevel.Observer)
    def get_running_protocol(self) -> Protocol:
        p = _unwrap_tags(self.run_command("PROT? ${Protocol}"))
        pn, svs, rm = self.run_command(
            "RET ${Protocol} ${SampleVolume} ${RunMode}"
        ).split()
        p = f"PROT -volume={svs} -runmode={rm} {pn} " + p
        return Protocol.from_scpicommand(SCPICommand.from_string(p))

    def set_access_level(
        self,
        access_level: AccessLevel | str,
        exclusive: bool = False,
        stealth: bool = False,
    ) -> None:
        access_level = AccessLevel(access_level)

        if access_level > AccessLevel(self.max_access_level):
            raise ValueError(
                f"Access level {access_level} is above maximum {self.max_access_level}."
                " Change max_access level to continue."
            )

        self.run_command(
            f"ACC -stealth={stealth} -exclusive={exclusive} {access_level}"
        )
        log.debug(f"Took access level {access_level} {exclusive=} {stealth=}")
        self._current_access_level = access_level

    def get_access_level(
        self,
    ) -> tuple[AccessLevel, bool, bool]:
        ret = self.run_command("ACC?")
        m = re.match(r"^-stealth=(\w+) -exclusive=(\w+) (\w+)", ret)
        if m is None:
            raise ValueError(ret)
        level = AccessLevel(m[3])
        self._current_access_level = level
        return level, m[2] == "True", m[1] == "True"

    def authenticate(self, password: str) -> None:
        challenge_key = self.run_command(SCPICommand("CHAL?"))
        auth_rep = _gen_auth_response(password, challenge_key)
        self.run_command(SCPICommand("AUTH", auth_rep))
    


    @property
    @_ensure_connection(AccessLevel.Guest)
    def access_level(self) -> AccessLevel:
        return self.get_access_level()[0]

    @access_level.setter
    @_ensure_connection(AccessLevel.Guest)
    def access_level(self, v: AccessLevel | str) -> None:
        self.set_access_level(v)

    @_ensure_connection(AccessLevel.Controller)
    def drawer_open(self) -> None:
        """Open the machine drawer using the OPEN command. This will ensure proper
        cover/drawer operation.  It *will not check run status*, and will open and
        close the drawer during runs and potentially during imaging.
        """
        self.run_command("OPEN")

    @_ensure_connection(AccessLevel.Controller)
    def drawer_close(self, lower_cover: bool = True, check: bool = True) -> None:
        """Close the machine drawer using the OPEN command. This will ensure proper
        cover/drawer operation.  It *will not check run status*, and will open and
        close the drawer during runs and potentially during imaging.

        By default, it will lower the cover automaticaly after closing, use
        lower_cover=False to not do so.
        """
        self.run_command("CLOSE")
        if (drawerpos := self.drawer_position) != "Closed":
            log.error(f"Drawer position should be Closed, but is {drawerpos}.")
            if check:
                raise ValueError(f"Drawer position is {drawerpos}")
        if lower_cover:
            self.cover_lower(check=check, ensure_drawer=False)

    @property
    @_ensure_connection(AccessLevel.Observer)
    def block(self) -> tuple[bool, float]:
        """Returns whether the block is currently temperature-controlled, and the current block temperature setting."""
        sbool, v = self.run_command("BLOCK?").split()
        sbool = sbool.lower()
        v = float(v)

        if sbool == "on":
            return True, v
        elif sbool == "off":
            return False, v
        else:
            raise ValueError(f"Block status {sbool} {v} is not understood.")

    @block.setter
    @_ensure_connection(AccessLevel.Controller)
    def block(self, value: float | None | bool | tuple[bool, float]):
        """Set the block temperature control.

        If a float is given, it will be set to that temperature; None or False will
        turn off the block temperature control, and True will turn it on at the current set temperature.  A tuple can be given
        to specify both the on/off status and the temperature."""
        if (value is None) or (value is False):
            bcom = "OFF"
        elif value is True:
            bcom = "ON"
        elif isinstance(value, tuple):
            bcom = f"{'ON' if value[0] else 'OFF'} {float(value[1])}"
        else:
            try:
                bcom = f"ON {float(value)}"
            except ValueError:
                raise ValueError(f"Block value {value} is not understood.")
        self.run_command(f"BLOCK {bcom}")

    @property
    def status(self) -> RunStatus:
        """Return the current status of the run."""
        with self.ensured_connection(AccessLevel.Observer):
            return RunStatus.from_machine(self)

    @property
    def drawer_position(self) -> Literal["Open", "Closed", "Unknown"]:
        """Return the drawer position from the DRAW? command."""
        with self.ensured_connection(AccessLevel.Observer):
            d = self.run_command("DRAW?")
            if d not in ["Open", "Closed", "Unknown"]:
                raise ValueError(f"Drawer position {d} is not understood.")
            return cast(Literal["Open", "Closed", "Unknown"], d)

    @property
    def cover_position(self) -> Literal["Up", "Down", "Unknown", ""]:
        """Return the cover position from the ENG? command. Note that
        this does not always seem to work."""
        with self.ensured_connection(AccessLevel.Observer):
            f = self.run_command("ENG?")
            if f not in ["Up", "Down", "Unknown", ""]:
                raise ValueError(f"Cover position {f} is not understood.")
            if f == "":
                log.error("Cover position is blank. This should not happen.")
            return cast(Literal["Up", "Down", "Unknown", ""], f)

    @_ensure_connection(AccessLevel.Controller)
    def cover_lower(self, check: bool = True, ensure_drawer: bool = True) -> None:
        """Lower/engage the plate cover, closing the drawer if needed."""
        if ensure_drawer and (self.drawer_position in ("Open", "Unknown")):
            self.drawer_close(lower_cover=False, check=check)
        self.run_command("COVerDOWN")
        if (covpos := self.cover_position) != "Down":
            log.error(f"Cover position should be Down, but is {covpos}.")
            if check:
                raise ValueError(f"Cover position should be Down, but is {covpos}.")

    def __exit__(self, exc_type: type, exc: Exception, tb: Any) -> None:
        self.disconnect()

    def __del__(self) -> None:
        if self.connected:
            self.disconnect()

    def disconnect(self) -> None:
        """Cleanly disconnect from the machine."""
        if self.connection is None:
            raise ConnectionError(f"Not connected to {self.host}.")

        del self._connection

        self._connection = None
        self._current_access_level = AccessLevel.Guest

    @_ensure_connection(AccessLevel.Controller)
    def abort_current_run(self) -> None:
        """Abort (stop immediately) the current run."""
        self.run_command("AbortRun ${RunTitle}")

    @_ensure_connection(AccessLevel.Controller)
    def stop_current_run(self) -> None:
        """Stop (stop after cycle end) the current run."""
        self.run_command("StopRun ${RunTitle}")

    @_ensure_connection(AccessLevel.Controller)
    def pause_current_run(self) -> None:
        """Pause the current run now."""
        self.run_command_to_ack("PAUSe")

    @_ensure_connection(AccessLevel.Controller)
    def pause_current_run_at_temperature(self) -> None:
        raise NotImplementedError

    @_ensure_connection(AccessLevel.Controller)
    def resume_current_run(self) -> None:
        """Resume the current run."""
        self.run_command_to_ack("RESume")

    @property
    def power(self) -> bool:
        """Get and set the machine's operational power (lamp, etc) as a bool.

        Setting this to False will not turn off the machine, just power down
        the lamp, temperature control, etc.  It will do so even if there is
        currently a run.
        """
        with self.ensured_connection(AccessLevel.Observer):
            s = self.run_command("POW?").lower()
            if s == "on":
                return True
            elif s == "off":
                return False
            else:
                raise ValueError(f"Unexpected power status: {s}")

    @power.setter
    def power(self, value: Literal["on", "off", True, False]) -> None:
        with self.ensured_connection(AccessLevel.Controller):
            if value is True:
                value = "on"
            elif value is False:
                value = "off"
            self.run_command(f"POW {value}")

    @property
    def current_run_name(self) -> str | None:
        """Name of current run, or None if no run is active."""
        with self.ensured_connection(AccessLevel.Observer):
            out = self.run_command("RUNTitle?")
            if out == "-":
                return None
            else:
                return re.sub(r"(<([\w.]+)>)?([^<]+)(</[\w.]+>)?", r"\3", out)

    @_ensure_connection(AccessLevel.Controller)
    def restart_system(self) -> None:
        """Restart the system (both the InstrumentServer and android interface) by killing the zygote process."""
        self.run_command(SCPICommand("SYST:EXEC", "killall zygote"))

    @contextmanager
    def at_access(
        self,
        access_level: AccessLevel | str,
        exclusive: bool = False,
        stealth: bool = False,
    ) -> Generator[Machine, None, None]:
        fac, fex, fst = self.get_access_level()
        self.set_access_level(access_level, exclusive, stealth)
        log.debug(f"Took access level {access_level} {exclusive=} {stealth=}.")
        yield self
        self.set_access_level(fac, fex, fst)
        log.debug(
            f"Dropped access level {access_level}, returning to {fac} exclusive={fex} stealth={fst}."
        )

    @contextmanager
    def ensured_connection(
        self, access_level: AccessLevel = AccessLevel.Observer
    ) -> Generator[Machine, None, None]:
        if self.automatic:
            was_connected = self.connected
            old_access = self._current_access_level
            if not was_connected:
                self.connect()
                self.set_access_level(max(old_access, access_level))
            elif old_access < access_level:
                self.set_access_level(access_level)
            yield self
            if not was_connected:
                self.disconnect()
            elif old_access < access_level:
                self.set_access_level(old_access)
        else:
            yield self

    @_ensure_connection(AccessLevel.Controller)
    def compile_eds(self, run_name: str) -> None:
        """Take a finished run directory in experiments:, compile it into an EDS, and move it to
        public_run_complete:"""

        expfiles = self.list_files("", leaf="experiment", verbose=True)

        results = [r for r in expfiles if r["path"] == run_name]

        if len(results) == 0:
            raise FileNotFoundError(run_name)
        elif len(results) > 1:
            raise ValueError(f"Multiple runs with name {run_name}: {results}")
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

    def get_sds_file(
        self,
        path: str,
        runtitle: str | None = None,
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
        run: str | None = None,
        return_files: Literal[True],
    ) -> tuple[FilterDataReading, list[tuple[str, bytes]]]: ...

    @overload
    def get_filterdata_one(
        self,
        ref: FilterDataFilename,
        *,
        run: str | None = None,
        return_files: bool = False,
    ) -> FilterDataReading: ...

    def get_filterdata_one(
        self,
        ref: FilterDataFilename,
        *,
        run: str | None = None,
        return_files: bool = False,
    ) -> (
        FilterDataReading | tuple[FilterDataReading, list[tuple[str, bytes]]]
    ):
        if run is None:
            run = self.get_run_title()

        fl = self.get_exp_file(f"{run}/apldbio/sds/filter/" + ref.tostring())

        if (x := ET.parse(io.BytesIO(fl)).find("PlatePointData/PlateData")) is not None:
            f = FilterDataReading(x)
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
        self, as_list: Literal[True], run: str | None = None
    ) -> list[FilterDataReading]: ...

    @overload
    def get_all_filterdata(
        self, run: str | None = None, as_list: bool = False
    ) -> "pd.DataFrame": ...

    def get_all_filterdata(
        self, run: str | None = None, as_list: bool = False
    ) -> "pd.DataFrame | list[FilterDataReading]":
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

        return df_from_readings(pl)

    def get_expfile_list(
        self, glob: str, allow_nomatch: bool = False
    ) -> list[str]:
        try:
            fl = self.run_command(SCPICommand("EXP:LIST?", glob))
        except ValueError as ce: # FIXME
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
