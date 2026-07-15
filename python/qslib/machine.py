# SPDX-FileCopyrightText: 2021 - 2023 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

from __future__ import annotations

import base64
import gzip
import hashlib
import fnmatch
import logging
import random
import re
import shlex
import time
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
from typing import IO, TYPE_CHECKING, Any, Generator, Literal, cast, overload
from datetime import datetime, timezone
from typing import TypedDict
from ._qslib import QSConnection, CommandError
import io
from http.client import HTTPException
from .server import ServerClient, ServerError, ServerOutcomeUnknown, ServerUnavailable
from .data import FilterSet

if TYPE_CHECKING:
    DrawerPosition = Literal["Open", "Closed", "Unknown"]
    CoverPosition = Literal["Up", "Down", "Unknown", ""]
else:
    # At runtime these are plain ``str``. The precise ``Literal`` types above are
    # only for static checkers: resolving a live ``Literal`` return annotation
    # crashes jedi (IPython's completer), which breaks tab-completion on
    # ``Machine`` instances entirely.
    DrawerPosition = str
    CoverPosition = str


from qslib.scpi_commands import AccessLevel, SCPICommand, ArgList, specialize_command_error

from ._util import _unwrap_tags
from .protocol import Protocol

from ._qslib import (  # noqa: E402
    MachineStatus,
    RunStatus,
    StatusLedColor,
    StatusLedMode,
    StatusLedSet,
    StatusLedState,
)

import posixpath

# InstrumentServer SCPI file "context" -> absolute on-instrument directory, from
# ``Config/locations.ini`` (byte-identical across InstrumentServer 132/151/161).
# Used to turn a FILE-branch locator into an absolute path that qslib-server can
# serve raw over HTTP. Keys are lowercase (SCPI context names are matched
# case-insensitively); ``None``/``""`` is the default context. Contexts not
# listed here fall back to SCPI.
_SCPI_CONTEXT_ROOTS: dict[str | None, str] = {
    None: "/data/vendor/IS",
    "": "/data/vendor/IS",
    "file": "/data/vendor/IS",
    "default": "/data/vendor/IS",
    "experiments": "/data/vendor/IS/experiments",
    "runs": "/data/vendor/IS/runs",
    "logs": "/data/vendor/IS/logs",
    "templates": "/data/vendor/IS/templates",
    "calibrations": "/data/vendor/IS/calibrations",
    "public_run_complete": "/sdcard/public_run_complete",
    "private_run_complete": "/sdcard/private_run_complete",
}

# SCPI leaf (branch) -> its default context, for directory operations addressed
# by leaf rather than an explicit context (e.g. ``EXP:ZIPREAD?``). ``None`` means
# the FILE default context (``/data/vendor/IS``).
_SCPI_LEAF_DEFAULT_CONTEXT: dict[str, str | None] = {
    "FILE": None,
    "EXP": "experiments",
}


def _scpi_locator_abspath(locator: str) -> str | None:
    """Resolve a FILE-branch locator (``"[context:]relpath"``) to an absolute
    on-instrument path, mirroring the InstrumentServer's ``getPath``.

    Returns ``None`` if the context is not a known root (so the caller uses
    SCPI). Traversal (``.``/``..``) components are dropped, as the server does.
    """
    if ":" in locator:
        ctx, rel = locator.split(":", 1)
        key: str | None = ctx.lower()
    else:
        key, rel = None, locator
    base = _SCPI_CONTEXT_ROOTS.get(key)
    if base is None:
        return None
    parts = [p for p in rel.replace("\\", "/").split("/") if p not in ("", ".", "..")]
    return posixpath.join(base, *parts) if parts else base


def _join_scpi_path(prefix: str, relative: str) -> str:
    if not prefix:
        return relative
    if prefix.endswith(":"):
        return prefix + relative
    return posixpath.join(prefix, relative)


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


_NO_SEMANTIC_RESULT = object()


def _ensure_connection(level: AccessLevel = AccessLevel.Observer) -> Any:
    def wrap(func):
        @wraps(func)
        def wrapped(m: Machine, *args: Any, **kwargs: Any) -> Any:
            semantic = m._semantic_dispatch(func.__name__, args, kwargs)
            if semantic is not _NO_SEMANTIC_RESULT:
                return semantic
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
    server_port: int | None = None
    server_token: str | None = None
    _server: ServerClient | None = None

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
        if self.server_port != Machine.server_port:
            d["server_port"] = self.server_port
        if self.server_token and password:
            d["server_token"] = self.server_token

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
        server_port: int | None = None,
        server_token: str | None = None,
        server_connect_timeout: int = 3,
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
        self.server_port = server_port
        self.server_token = server_token
        self.server_connect_timeout = server_connect_timeout
        self._server = None

    def connect(self) -> None:
        """Open the connection manually.

        Manual connections are always direct SCPI. ``server_port`` opts
        automatic high-level methods into the semantic HTTP API; it does not
        silently turn a manually owned session into an administrator tunnel.
        """
        conn = self._direct_connection()
        self.connection = conn

        if self.password is not None:
            self.authenticate(self.password)
        if self._initial_access_level is not None:
            try:
                self.set_access_level(self._initial_access_level)
            except CommandError as e:
                from .scpi_commands import InsufficientAccess

                se = specialize_command_error(e)
                if isinstance(se, InsufficientAccess):
                    raise InsufficientAccess(
                        "Authentication required for remote connections. Provide a password to the Machine constructor."
                    ) from e
                raise
        self._current_access_level = self.get_access_level()[0]

    def _direct_connection(self) -> QSConnection:
        """Open a direct SSL/TCP SCPI connection (the non-qslib-server path)."""
        if self.ssl is True:
            connection_type = "SSL"
        elif self.ssl is False:
            connection_type = "TCP"
        else:
            connection_type = "Auto"
        return QSConnection(
            host=self.host,
            port=self.port,
            connection_type=connection_type,
            client_cert_path=self.client_certificate_path,
            client_key_path=self.client_key_path,
            server_ca_path=self.server_ca_file,
            tls_server_name=self.tls_server_name,
        )

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
        except ValueError as e:  # FIXME
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
        self.run_command(protocol.to_scpi_string())

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

    @_ensure_connection(AccessLevel.Observer)
    def download_dir(self, remote_dir: str, dest_dir: str | Path, *, leaf: str = "FILE", fast: bool = True) -> bool:
        """Download a directory tree from the machine into ``dest_dir`` over HTTP.

        When ``qslib-server`` is explicitly configured and ``fast`` is true,
        the directory is enumerated via its named directory resource and each
        file is fetched raw (no ``ZIPREAD`` base64+deflate),
        preserving the tree under ``dest_dir`` — the same on-disk layout that
        extracting :meth:`read_dir_as_zip` produces.

        Returns ``True`` if the directory was downloaded over HTTP; ``False`` if
        qslib-server is not available/preferred, the context is unknown, or a
        transfer error occurred (so the caller should fall back to SCPI
        ``ZIPREAD``). Raises :class:`FileNotFoundError` if the directory does not
        exist on the server (distinct from the fallback signal, for name probing).
        """
        server = self._fast_server(fast)
        if server is None:
            return False
        context = _SCPI_LEAF_DEFAULT_CONTEXT.get(leaf, leaf)
        locator = (context + ":" if context else "") + remote_dir
        abspath = _scpi_locator_abspath(locator)
        if abspath is None:
            return False
        try:
            server.download_dir(abspath, dest_dir)
        except FileNotFoundError as e:
            # The directory itself is missing (a 404 on the listing): a genuine
            # "not found", which callers use for name probing.
            raise FileNotFoundError(remote_dir) from e
        except (ServerError, OSError, HTTPException):
            # A transfer error (network drop, a file removed mid-download, a torn
            # body read) is not a missing directory: fall back to SCPI ZIPREAD
            # rather than report the run as absent.
            log.warning("qslib-server download_dir failed for %r; falling back to SCPI", remote_dir, exc_info=True)
            return False
        return True

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
            v = (self.run_command(f"{leaf}:LIST? -verbose {path}")).split("\n")[1:-1]
            ret: list[FileListInfo] = []
            for x in v:
                rm = re.match(
                    r'"([^"]+)" -type=(\S+) -size=(\S+) -mtime=(\S+) -atime=(\S+) -ctime=(\S+)(?: (.*))?$',
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
                    # Parse any extra -key=value options (e.g. -collected, -state, -run)
                    if rm.group(7):
                        for om in re.finditer(r"-(\w+)=(\S+)", rm.group(7)):
                            val: Any = om.group(2)
                            if val.lower() in ("true", "false"):
                                val = val.lower() == "true"
                            d[om.group(1)] = val
                if d["type"] == "folder" and recursive:
                    ret += self.list_files(cast(str, d["path"]), leaf=leaf, verbose=True, recursive=True)
                else:
                    ret.append(cast(FileListInfo, d))
            return ret

    @_ensure_connection(AccessLevel.Observer)
    def read_file(
        self,
        path: str,
        context: str | None = None,
        leaf: str = "FILE",
        encoding: Literal["base64", "plain"] = "base64",
        *,
        fast: bool = True,
        fallback: bool = True,
    ) -> bytes:
        """Read a file, preferring qslib-server's HTTP transfer when configured.

        When the machine has an explicitly configured ``qslib-server`` and
        ``fast`` is true, the file is fetched
        over plain HTTP straight off disk, avoiding the base64+TLS overhead of
        ``FILE:READ`` over SCPI. Otherwise, or on any qslib-server error when
        ``fallback`` is true, it is read over SCPI.

        The HTTP path resolves ``(context, path)`` to one of qslib-server's
        advertised named contexts. This covers the default ``FILE`` context and
        known contexts such as ``public_run_complete`` (completed ``.eds``
        files). An unknown context or a non-``FILE`` leaf falls back to SCPI.

        Parameters
        ----------
        path : str
            File path on the machine.
        context : str | None (default None)
            SCPI file context. An unrecognised context forces the SCPI path.
        leaf : str (default FILE)
            SCPI file leaf. A non-``FILE`` leaf forces the SCPI path.
        encoding : "base64" | "plain" (default base64)
            SCPI transfer encoding (ignored on the HTTP path).
        fast : bool (default True)
            Prefer qslib-server's HTTP transfer when it is available.
        fallback : bool (default True)
            Fall back to SCPI if the HTTP transfer fails.

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

        locator = contexts + path
        abspath = _scpi_locator_abspath(locator) if (leaf == "FILE" and "${" not in locator) else None
        server = self._fast_server(fast) if abspath is not None else None
        if server is not None:
            assert abspath is not None
            try:
                data = server.get_abs_file(abspath)
                assert data is not None
                return data
            except (ServerError, OSError, HTTPException):
                # A ServerError or a transport failure mid-transfer (network
                # drop, torn body read): fall back to SCPI unless disabled.
                log.warning("qslib-server file read failed for %r; falling back to SCPI", path, exc_info=True)
                if not fallback:
                    raise

        reply = self.run_command_to_bytes(SCPICommand(f"{leaf}:READ?", contexts + path, encoding=encoding))
        if not reply.startswith(b"<quote>\n") or not reply.endswith(b"</quote>"):
            raise ValueError("Unexpected reply format: expected <quote>...</quote>")
        r = reply[8:-8]
        if encoding == "base64":
            return base64.decodebytes(r)
        else:
            return r

    @property
    def server(self) -> ServerClient | None:
        """A :class:`~qslib.server.ServerClient` for the on-instrument
        ``qslib-server``, or ``None`` if ``server_port`` was not set.

        qslib-server is reached at the same host as SCPI, on ``server_port``.
        """
        if self.server_port is None:
            return None
        if self._server is None:
            self._server = ServerClient(
                self.host,
                port=self.server_port,
                token=self.server_token,
                timeout=float(self.server_connect_timeout),
            )
        return self._server

    def _semantic_server(self, resource: str, *, mutation: bool = False) -> ServerClient | None:
        """Select the semantic backend without opening a SCPI connection.

        A manually owned/direct connection always wins. Capability failures are
        negatively cached by :class:`ServerClient`, so an absent optional
        server adds no repeated latency to direct workflows.
        """
        if self.server_port is None or not self.automatic or self.connected:
            return None
        server = self.server
        assert server is not None
        try:
            capabilities = server.capabilities()
        except ServerError:
            return None
        if resource not in capabilities.get("resources", []):
            return None
        if mutation:
            if resource == "files" and not capabilities.get("file_writes", False):
                return None
            if resource != "files" and not capabilities.get("controls", False):
                return None
        return server

    @staticmethod
    def _run_status_from_server(value: dict[str, Any]) -> RunStatus:
        fields = [
            str(value.get("name", "-")),
            str(value.get("stage_name", value.get("stage", -1))),
            str(value.get("num_stages", -1)),
            str(value.get("cycle", -1)),
            str(value.get("num_cycles", -1)),
            str(value.get("step", -1)),
            str(value.get("point", -1)),
            str(value.get("state", "unknown")).upper(),
        ]
        return RunStatus.from_bytes(shlex.join(fields).encode())

    @staticmethod
    def _machine_status_from_server(value: dict[str, Any]) -> MachineStatus:
        target_temperatures = " ".join(
            f"-{key}={temperature}" for key, temperature in value.get("target_temperatures_c", {}).items()
        )
        target_controlled = " ".join(
            f"-{key}={'True' if controlled else 'False'}"
            for key, controlled in value.get("target_controlled", {}).items()
        )
        fields = [
            str(value.get("drawer", "unknown")).title(),
            str(value.get("cover", "unknown")).title(),
            str(value.get("lamp_status", "unknown")),
            " ".join(str(number) for number in value.get("sample_temperatures_c", [])),
            " ".join(str(number) for number in value.get("block_temperatures_c", [])),
            str(value.get("cover_temperature_c", 0.0)),
            target_temperatures,
            target_controlled,
            str(value.get("led_temperature_c", 0.0)),
        ]
        return MachineStatus.from_bytes(shlex.join(fields).encode())

    def _semantic_dispatch(self, name: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        """Dispatch bounded high-level calls before the connection decorator.

        Returning ``_NO_SEMANTIC_RESULT`` means the permanent direct-SCPI
        implementation should run unchanged.
        """
        read_methods = {
            "run_status",
            "machine_status",
            "get_zone_count",
            "status_led",
            "block",
            "read_file",
            "_get_log_from_byte",
        }
        mutations = {
            "drawer_open",
            "drawer_close",
            "cover_lower",
            "set_status_led",
            "status_led_off",
            "abort_current_run",
            "stop_current_run",
            "pause_current_run",
            "resume_current_run",
            "compile_eds",
            "write_file",
        }
        if name not in read_methods | mutations | {"download_dir", "list_files"}:
            return _NO_SEMANTIC_RESULT

        is_mutation = name in mutations or (name in {"block", "status_led"} and bool(args))
        if name in {"read_file", "write_file", "download_dir", "list_files", "_get_log_from_byte"}:
            fast = bool(kwargs.get("fast", True))
            if not fast:
                return _NO_SEMANTIC_RESULT
            server = self._semantic_server("files", mutation=name == "write_file")
        elif name in {
            "run_status",
            "pause_current_run",
            "resume_current_run",
            "stop_current_run",
            "abort_current_run",
            "compile_eds",
        }:
            server = self._semantic_server("runs", mutation=is_mutation)
        else:
            server = self._semantic_server("instrument", mutation=is_mutation)
        if server is None:
            return _NO_SEMANTIC_RESULT

        try:
            if name == "run_status":
                return self._run_status_from_server(server.current_run())
            if name == "machine_status":
                return self._machine_status_from_server(server.instrument_status())
            if name == "get_zone_count":
                return int(server.instrument_status()["zone_count"])
            if name == "drawer_open":
                server.set_drawer("open", lower_cover=False, verify=True)
                return None
            if name == "drawer_close":
                lower_cover = bool(args[0]) if args else bool(kwargs.get("lower_cover", True))
                check = bool(args[1]) if len(args) > 1 else bool(kwargs.get("check", True))
                server.set_drawer("closed", lower_cover=lower_cover, verify=check)
                return None
            if name == "cover_lower":
                check = bool(args[0]) if args else bool(kwargs.get("check", True))
                ensure_drawer = bool(args[1]) if len(args) > 1 else bool(kwargs.get("ensure_drawer", True))
                if ensure_drawer:
                    server.set_drawer("closed", lower_cover=True, verify=check)
                else:
                    server.set_cover("down", verify=check)
                return None
            if name == "set_status_led":
                color = args[0] if args else kwargs["color"]
                mode = args[1] if len(args) > 1 else kwargs.get("mode", "on")
                color_name = str(getattr(color, "value", color)).split(".")[-1]
                mode_name = str(getattr(mode, "value", mode)).split(".")[-1]
                server.set_indicator(color_name, mode_name)
                return None
            if name == "status_led_off":
                current = server.instrument_status().get("indicator", {})
                server.set_indicator(str(current.get("color") or "white"), "off")
                return None
            if name == "status_led":
                if args:
                    value = args[0]
                    if isinstance(value, tuple):
                        color, mode = value
                    else:
                        color, mode = value, "on"
                    color_name = str(getattr(color, "value", color)).split(".")[-1]
                    mode_name = str(getattr(mode, "value", mode)).split(".")[-1]
                    server.set_indicator(color_name, mode_name)
                    return None
                indicator = server.instrument_status()["indicator"]
                return StatusLedState.from_bytes(
                    f"{indicator.get('color') or '-'} {indicator.get('mode', 'off')}".encode()
                )
            if name == "block":
                if not args:
                    block = server.instrument_status()["block"]
                    return bool(block["enabled"]), float(block["target_c"])
                value = args[0]
                if value is None or value is False:
                    server.set_block(False)
                elif value is True:
                    server.set_block(True)
                elif isinstance(value, tuple):
                    server.set_block(bool(value[0]), float(value[1]))
                else:
                    server.set_block(True, float(value))
                return None
            if name in {"pause_current_run", "resume_current_run", "stop_current_run", "abort_current_run"}:
                action = name.removesuffix("_current_run")
                current = server.current_run().get("name", "")
                operation = server.run_action(str(current), action)
                completed = server.wait_operation(operation)
                if completed.get("state") != "succeeded":
                    raise ServerError(f"server {action} operation failed: {completed.get('error')}")
                return None
            if name == "compile_eds":
                operation = server.run_action(str(args[0]), "compile")
                completed = server.wait_operation(operation, timeout=610.0)
                if completed.get("state") != "succeeded":
                    raise ServerError(f"server compile operation failed: {completed.get('error')}")
                return None
            if name == "read_file":
                path = str(args[0])
                context = args[1] if len(args) > 1 else kwargs.get("context")
                leaf = args[2] if len(args) > 2 else kwargs.get("leaf", "FILE")
                if leaf != "FILE" or "${" in path:
                    return _NO_SEMANTIC_RESULT
                locator = (f"{context}:" if context else "") + path
                absolute = _scpi_locator_abspath(locator)
                if absolute is None:
                    return _NO_SEMANTIC_RESULT
                data = server.get_abs_file(absolute)
                assert data is not None
                return data
            if name == "_get_log_from_byte":
                run_name = args[0].decode() if isinstance(args[0], bytes) else str(args[0])
                offset = int(args[1])
                absolute = f"/data/vendor/IS/experiments/{run_name}/apldbio/sds/messages.log"
                data = server.get_abs_file(absolute, range_start=offset)
                assert data is not None
                return data
            if name == "write_file":
                path, data = str(args[0]), args[1]
                if "${" in path:
                    return _NO_SEMANTIC_RESULT
                absolute = _scpi_locator_abspath(path)
                if absolute is None:
                    return _NO_SEMANTIC_RESULT
                server.put_abs_file(absolute, data.encode() if isinstance(data, str) else data)
                return None
            if name == "download_dir":
                remote_dir, destination = str(args[0]), args[1]
                leaf = str(kwargs.get("leaf", "FILE"))
                context = _SCPI_LEAF_DEFAULT_CONTEXT.get(leaf, leaf)
                absolute = _scpi_locator_abspath((f"{context}:" if context else "") + remote_dir)
                if absolute is None:
                    return _NO_SEMANTIC_RESULT
                server.download_dir(absolute, destination)
                return True
            if name == "list_files":
                path = str(args[0])
                leaf = str(kwargs.get("leaf", "FILE"))
                verbose = bool(kwargs.get("verbose", False))
                recursive = bool(kwargs.get("recursive", False))
                if leaf.upper() not in {"FILE", "EXP"}:
                    return _NO_SEMANTIC_RESULT
                if leaf.upper() == "EXP" and ":" not in path:
                    names = [str(item) for item in server.list_experiments().get("experiments", [])]
                    pattern = path.rstrip("/") or "*"
                    names = [item for item in names if fnmatch.fnmatch(item, pattern)]
                    if not verbose:
                        return [f"{item}/" for item in names]
                    epoch = datetime.fromtimestamp(0, tz=timezone.utc)
                    return [
                        FileListInfo(
                            path=f"{item}/",
                            type="folder",
                            size=0,
                            mtime=epoch,
                            atime=epoch,
                            ctime=epoch,
                        )
                        for item in names
                    ]
                context = _SCPI_LEAF_DEFAULT_CONTEXT.get(leaf, leaf)
                absolute = _scpi_locator_abspath((f"{context}:" if context else "") + path)
                if absolute is None:
                    return _NO_SEMANTIC_RESULT
                pattern = None
                list_absolute = absolute
                relative_name = posixpath.basename(absolute)
                if any(character in relative_name for character in "*?["):
                    pattern = relative_name
                    list_absolute = posixpath.dirname(absolute)
                entries = server.list_dir(list_absolute)
                if pattern is not None:
                    entries = [item for item in entries if fnmatch.fnmatch(posixpath.basename(str(item["path"])), pattern)]
                    prefix = path[: len(path) - len(posixpath.basename(path))].rstrip("/")
                else:
                    prefix = path.rstrip("/")
                if not recursive:
                    entries = [item for item in entries if "/" not in str(item["path"])]
                if not verbose:
                    return [_join_scpi_path(prefix, str(item["path"])) for item in entries]
                result: list[FileListInfo] = []
                for item in entries:
                    modified = item.get("modified_at")
                    when = (
                        datetime.fromisoformat(str(modified).replace("Z", "+00:00"))
                        if modified
                        else datetime.fromtimestamp(0, tz=timezone.utc)
                    )
                    result.append(
                        FileListInfo(
                            path=_join_scpi_path(prefix, str(item["path"])),
                            type="file",
                            size=int(item["size"]),
                            mtime=when,
                            atime=when,
                            ctime=when,
                        )
                    )
                return result
        except ServerUnavailable:
            return _NO_SEMANTIC_RESULT
        except ServerOutcomeUnknown:
            raise
        except (ServerError, OSError, HTTPException):
            if is_mutation:
                raise
            log.debug("semantic server read failed for %s; using direct SCPI", name, exc_info=True)
            return _NO_SEMANTIC_RESULT
        return _NO_SEMANTIC_RESULT

    def _fast_server(self, fast: bool) -> ServerClient | None:
        """The qslib-server client to use for a fast-path file transfer, or
        ``None`` when the fast path is disabled or unavailable."""
        if not fast:
            return None
        return self._semantic_server("files")

    def ensure_server(
        self,
        binary: str | bytes | None = None,
        *,
        listen: str,
        remote_path: str = "/data/qslib-server",
        file_root: str = "/data/vendor/IS",
        extra_args: tuple[str, ...] = (),
        timeout: float = 5.0,
    ) -> ServerClient:
        """Ensure ``qslib-server`` is running, deploying it if needed.

        If qslib-server already answers ``/health``, its client is returned
        immediately. Otherwise, when ``binary`` (a path or the raw bytes of a
        cross-compiled qslib-server) is given, it is streamed to the instrument
        in chunks over SCPI (:meth:`_deploy_binary`; ``FILE:WRITE`` is unreliable
        for large files on some builds), made executable, and started in the
        background via ``SYST:EXEC`` (root), then polled until ready.

        On success, subsequent :meth:`read_file` calls prefer qslib-server's
        semantic HTTP transfer.

        Parameters
        ----------
        binary
            Path to, or bytes of, the qslib-server binary to deploy. Required
            only if qslib-server is not already running.
        listen
            The instrument-side bind address, e.g. ``"169.254.217.190:7500"``.
            This is the private eth0 IP on the instrument, which the client
            cannot infer, so it must be supplied.
        remote_path
            Persistent path to install the binary to on the instrument.
        file_root
            Base of qslib-server's named InstrumentServer contexts. Completed
            run contexts are resolved separately under ``/sdcard``.
        extra_args
            Additional qslib-server CLI arguments.

        Notes
        -----
        Requires ``server_port`` on the :class:`Machine`, and Controller access
        for the push. The exact on-device ``SYST:EXEC`` and path behaviour
        should be confirmed on the target instrument.
        """
        if self.server_port is None:
            raise ValueError("server_port must be set on the Machine to use qslib-server")
        client = self.server
        assert client is not None

        try:
            client.health()
            return client
        except ServerError:
            pass

        if binary is None:
            raise ServerError("qslib-server is not running and no `binary` was provided to deploy")

        # The SYST:EXEC argument is a SCPI double-quoted string that the
        # instrument also passes to a shell and that performs SCPI `$(...)`
        # substitution. shlex.quote neutralises the shell layer for each argv
        # element, but characters that break the outer SCPI string or trigger
        # SCPI substitution must be rejected outright.
        unsafe = set('"\\`$\n\r')

        def _safe(name: str, value: str) -> str:
            bad = unsafe & set(value)
            if bad:
                raise ValueError(
                    f"ensure_server {name} contains characters unsafe for SYST:EXEC "
                    f"({''.join(sorted(bad))!r}): {value!r}"
                )
            return value

        _safe("remote_path", remote_path)
        _safe("listen", listen)
        _safe("file_root", file_root)
        if self.server_token:
            _safe("server_token", self.server_token)
        for a in extra_args:
            _safe("extra_arg", a)

        data = Path(binary).read_bytes() if isinstance(binary, str) else binary
        auth_path = remote_path + ".auth.toml"
        _safe("auth_path", auth_path)
        args = [
            remote_path,
            "--listen",
            listen,
            "--file-root",
            file_root,
            "--allow-file-writes",
            "--allow-controls",
        ]
        if self.server_token:
            token_hash = hashlib.sha256(self.server_token.encode()).hexdigest()
            auth_toml = (
                "[[tokens]]\n"
                'name = "qslib-bootstrap"\n'
                f'sha256 = "{token_hash}"\n'
                'role = "administrator"\n'
            ).encode()
            args += ["--auth-config", auth_path]
        else:
            auth_toml = None
            args += ["--no-auth", "--unauthenticated-role", "administrator"]
        args += list(extra_args)
        cmdline = " ".join(shlex.quote(a) for a in args)

        # SYST:EXEC needs Controller; hold it for the whole deploy + launch.
        with self.ensured_connection(AccessLevel.Controller):
            if auth_toml is not None:
                self._deploy_binary(auth_path, auth_toml)
                self.run_command(f'SYST:EXEC "chmod 600 {shlex.quote(auth_path)}"')
            self._deploy_binary(remote_path, data)
            self.run_command(f'SYST:EXEC "chmod 755 {shlex.quote(remote_path)}"')
            # nohup so qslib-server survives the SYST:EXEC shell exiting.
            self.run_command(f'SYST:EXEC "nohup {cmdline} >/dev/null 2>&1 &"')

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                client.health()
                return client
            except (ServerError, OSError, HTTPException):
                time.sleep(0.1)
        raise ServerError("qslib-server did not become ready after deployment")

    def upgrade_server(
        self,
        binary: str | bytes,
        *,
        timeout: float = 60.0,
        poll_interval: float = 0.5,
    ) -> ServerClient:
        """Upgrade the running ``qslib-server`` to ``binary`` over HTTP.

        Unlike :meth:`ensure_server` (which deploys over SCPI and only helps when
        nothing is running), this replaces an *already running* qslib-server
        through its versioned upgrade endpoint: the binary is uploaded raw (fast,
        no base64+SCPI), verified by SHA-256 and a ``--version`` run on the
        instrument, installed atomically, and the server restarts into it —
        rolling back to the previous binary if the new one fails to start.

        Success is confirmed by polling ``/health`` until the running
        ``executable_sha256`` equals the uploaded binary's hash; a persistent old hash
        means the instrument rolled back, and this raises. Returns the
        :class:`~qslib.server.ServerClient` on success.

        This needs qslib-server already running (use :meth:`ensure_server` to
        bootstrap). No SCPI/Controller access is required.
        """
        if self.server_port is None:
            raise ValueError("server_port must be set on the Machine to upgrade qslib-server")
        client = self.server
        assert client is not None

        data = Path(binary).read_bytes() if isinstance(binary, str) else binary
        new_sha = hashlib.sha256(data).hexdigest()

        current = client.health()  # also confirms it is running
        if current.get("executable_sha256") == new_sha:
            log.info("qslib-server already running the requested build (%s)", new_sha[:12])
            return client

        log.info(
            "upgrading qslib-server %s -> %s",
            current.get("executable_sha256", "?")[:12],
            new_sha[:12],
        )
        client.upgrade(data)  # server verifies, installs, and restarts into the new binary

        deadline = time.monotonic() + timeout
        last: str | None = None
        while time.monotonic() < deadline:
            try:
                h = client.health()
                last = h.get("executable_sha256")
                if last == new_sha:
                    log.info("qslib-server upgrade confirmed (%s)", new_sha[:12])
                    return client
            except ServerError:
                pass  # server is restarting; keep polling
            time.sleep(poll_interval)
        raise ServerError(
            f"qslib-server upgrade did not take effect within {timeout}s "
            f"(running {last!r}, expected {new_sha}); it may have rolled back to the previous build"
        )

    def _deploy_binary(
        self,
        remote_path: str,
        data: bytes,
        *,
        chunk_chars: int = 40000,
    ) -> None:
        """Transfer ``data`` to ``remote_path`` on the instrument over SCPI.

        Streams the gzip-compressed binary as base64 in ``SYST:EXEC`` chunks
        (``echo -n <b64> | base64 -d``), then gunzips on the instrument, and
        verifies by size and (when ``md5sum`` is present) md5.

        This is used instead of :meth:`write_file` because on some
        InstrumentServer builds a single large ``FILE:WRITE`` times out; the
        SCPI command-size limit is only tens of KB, so the payload is chunked
        below that. Requires Controller access (caller supplies it) and
        ``gunzip``/``base64`` on the instrument. The verification scratch file is
        written under the default SCPI ``FILE`` context root so it can be read
        back over SCPI regardless of qslib-server's ``--file-root``.
        """
        gz = gzip.compress(data, 9)
        b64 = base64.b64encode(gz).decode()  # only [A-Za-z0-9+/=], SCPI/shell-safe
        # Each chunk must be a multiple of 4 base64 chars so it decodes on its
        # own and the concatenation equals the whole payload.
        step = max(4, chunk_chars - (chunk_chars % 4))
        q = shlex.quote(remote_path)
        gzq = shlex.quote(remote_path + ".gz")

        self.run_command(f'SYST:EXEC "rm -f {gzq} {q}"')
        for i in range(0, len(b64), step):
            self.run_command(f'SYST:EXEC "echo -n {b64[i : i + step]} | base64 -d >> {gzq}"')
        self.run_command(f'SYST:EXEC "gunzip -f {gzq}"')

        # Verify: size is universal; md5 only if md5sum exists on the device.
        # Scratch under the default FILE context root so the read-back below
        # (default context) resolves to the same file, independent of --file-root.
        check = _SCPI_CONTEXT_ROOTS[None].rstrip("/") + "/.qslib_deploy_check"
        self.run_command(f'SYST:EXEC "( wc -c {q}; md5sum {q} ) > {shlex.quote(check)} 2>/dev/null"')
        # Force SCPI: qslib-server is being deployed, so it is not yet serving.
        raw = self.read_file(".qslib_deploy_check", fast=False).decode(errors="replace")
        self.run_command(f'SYST:EXEC "rm -f {shlex.quote(check)}"')

        if str(len(data)) not in re.findall(r"\d+", raw):
            raise ServerError(f"binary transfer size mismatch (expected {len(data)} bytes): {raw!r}")
        remote_md5s = re.findall(r"\b[0-9a-f]{32}\b", raw)
        local_md5 = hashlib.md5(data).hexdigest()
        if remote_md5s and local_md5 not in remote_md5s:
            raise ServerError(f"binary transfer md5 mismatch (expected {local_md5}): {raw!r}")

    @_ensure_connection(AccessLevel.Controller)
    def write_file(self, path: str, data: str | bytes, *, fast: bool = True, fallback: bool = True) -> None:
        """Write ``data`` to ``path`` on the machine.

        When ``qslib-server`` is explicitly configured and ``fast`` is true,
        the file is uploaded over plain HTTP straight to
        disk, avoiding the base64+TLS overhead of ``FILE:WRITE`` over SCPI (which
        can time out on larger files). This applies only when ``path`` resolves
        to an absolute path under qslib-server's ``--file-root``; a path with an
        unresolved SCPI variable (``${...}``), an unknown context, or one outside
        the served root uses SCPI. Once an HTTP write is submitted, an uncertain
        result is surfaced and is never repeated over SCPI.
        """
        if isinstance(data, str):
            data = data.encode()

        abspath = _scpi_locator_abspath(path) if "${" not in path else None
        server = self._fast_server(fast) if abspath is not None else None
        if server is not None:
            assert abspath is not None
            try:
                server.put_abs_file(abspath, data)
                return
            except ServerOutcomeUnknown:
                raise
            except (ServerError, OSError, HTTPException):
                log.warning("qslib-server file write failed for %r; falling back to SCPI", path, exc_info=True)
                if not fallback:
                    raise

        self.run_command_bytes(
            b"FILE:WRITE " + path.encode() + b" <quote.base64>\n" + base64.encodebytes(data) + b"\n</quote.base64>"
        )

    def upload_zip_as_files(self, context_path: str, zipbytes: bytes, *, fast: bool = True) -> bool:
        """Unpack ``zipbytes`` onto the machine by uploading each member over
        qslib-server's HTTP ``PUT``, rooted at ``context_path``.

        This reproduces the InstrumentServer's ``EXP:ZIPWRITE`` (a plain unpack
        to disk -- create directories, write each file, no other side effects)
        without the base64+TLS cost of pushing the whole archive over SCPI.
        ``context_path`` is a SCPI locator for the destination directory (e.g.
        ``"experiments:<run>"``); each zip member is written at ``<resolved
        context_path>/<member>``.

        Returns ``True`` if every member was uploaded over HTTP; ``False`` only
        when the server was not selected or the context cannot be resolved.
        Transfer failures are surfaced because a preceding PUT may have
        committed and the mutation must not be repeated over SCPI.
        """
        server = self._semantic_server("files", mutation=True) if fast else None
        if server is None:
            return False
        base = _scpi_locator_abspath(context_path)
        if base is None:
            return False
        try:
            with zipfile.ZipFile(io.BytesIO(zipbytes)) as zf:
                for info in zf.infolist():
                    if info.is_dir():
                        continue
                    parts = [p for p in info.filename.split("/") if p and p not in (".", "..")]
                    if not parts:
                        continue
                    server.put_abs_file(posixpath.join(base, *parts), zf.read(info))
        except zipfile.BadZipFile:
            raise ValueError("invalid experiment ZIP package")
        except (ServerError, OSError, HTTPException):
            # A per-file PUT may have committed. Repeating the entire mutation
            # over SCPI could overwrite an accepted server-side write.
            raise
        return True

    @overload
    def list_runs_in_storage(self, glob: str = "*", *, verbose: Literal[True]) -> list[FileListInfo]: ...

    @overload
    def list_runs_in_storage(self, glob: str = "*", *, verbose: Literal[False] = False) -> list[str]: ...

    @overload
    def list_runs_in_storage(self, glob: str = "*", *, verbose: bool = False) -> list[str] | list[FileListInfo]: ...

    def list_runs_in_storage(self, glob: str = "*", *, verbose: bool = False) -> list[str] | list[FileListInfo]:
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
        except CommandError as error:
            from .scpi_commands import NoMatch

            se = specialize_command_error(error)
            if isinstance(se, NoMatch):
                return []
            raise se from error
        if not verbose:
            paths = cast(list[str], filelist)
            return [re.sub("^public_run_complete:", "", path)[:-4] for path in paths]
        else:
            entries = cast(list[FileListInfo], filelist)
            for entry in entries:
                entry["path"] = re.sub("^public_run_complete:", "", entry["path"])[:-4]
            return entries

    def load_run_from_storage(self, path: str) -> "Experiment":  # type: ignore
        from .experiment import Experiment

        """Load a run from machine storage as an Experiment
        """
        return Experiment.from_machine_storage(self, path)

    def save_run_from_storage(self, machine_path: str, download_path: str | IO[bytes], overwrite: bool = False) -> None:
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

    # Characters safe for embedding in a Python string literal sent via eval.
    # Allows alphanumeric, spaces (replaced by _ via runtitle_safe), hyphens,
    # underscores, dots, and parentheses only.
    _SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9_ \-\.()]+$")

    @_ensure_connection(AccessLevel.Observer)
    def _get_log_from_byte(self, name: str | bytes, byte: int) -> bytes:
        if self.connection is None:
            raise Exception
        if isinstance(name, bytes):
            name = name.decode()

        if not self._SAFE_NAME_RE.match(name):
            raise ValueError(
                f"Experiment name {name!r} contains characters not safe for remote eval. "
                f"Only alphanumeric, spaces, hyphens, underscores, dots, and parentheses are allowed."
            )

        # Generate a random u32 for the log transfer command
        log_ident = random.randint(0, 2**32 - 1)

        log_responder = self.connection.expect_ident(log_ident)

        # Use run_command_bytes to bypass SCPI argument parsing, which would
        # corrupt the embedded Python code (single quotes, backslashes, etc.)
        logcommand = self.connection.run_command_bytes(
            f"eval? session.writeQueue.put(('OK {log_ident} \\<quote.base64\\>\\\\n'"
            f" + (lambda x: [x.seek({byte}), __import__('base64').encodestring(x.read())][1])"
            f"(open('/data/vendor/IS/experiments/{name}/apldbio/sds/messages.log')) +"
            " '\\</quote.base64\\>\\\\n', None))".encode(),
        )

        logcommand.get_response()

        logres = log_responder.get_response()

        return base64.decodebytes(logres[15:-16].encode())  # FIXME: don't encode/decode, and make this more robust

    @_ensure_connection(AccessLevel.Observer)
    def run_status(self) -> RunStatus:
        """Return information on the status of any run."""
        out = self.run_command_bytes(RunStatus.command())
        return RunStatus.from_bytes(out)

    @_ensure_connection(AccessLevel.Observer)
    def machine_status(self) -> MachineStatus:
        """Return information on the status of the machine."""
        out = self.run_command_bytes(MachineStatus.command())
        return MachineStatus.from_bytes(out)

    def get_running_protocol(self) -> Protocol:
        server = self._semantic_server("runs")
        if server is not None:
            try:
                run = server.current_run()
                name = str(run.get("name", "-"))
                if name != "-":
                    return Protocol.from_xml(ET.fromstring(server.get_protocol(name)))
            except (ServerError, OSError, HTTPException, ET.ParseError):
                log.debug("semantic protocol query failed; using direct SCPI", exc_info=True)
        with self.ensured_connection(AccessLevel.Observer):
            p = _unwrap_tags(self.run_command("PROT? ${Protocol}"))
            pn, svs, rm = self.run_command("RET ${Protocol} ${SampleVolume} ${RunMode}").split()
            p = f"PROT -volume={svs} -runmode={rm} {pn} " + p
            return Protocol.from_scpi_string(p)

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

        try:
            self.run_command(f"ACC -stealth={stealth} -exclusive={exclusive} {access_level}")
        except CommandError as e:
            raise specialize_command_error(e) from e
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

    def generate_random_key(self) -> str:
        """Generate a 6-digit random authentication key on the server.

        Requires Controller or higher access. The key is valid for 10 minutes
        and grants Administrator access when used with :any:`authenticate`.
        Only one key is active at a time; calling this again before the key
        expires returns the same key.

        Returns:
            A 6-digit string (e.g., "042371").
        """
        server = self._semantic_server("instrument", mutation=True)
        if server is not None:
            try:
                operation = server.generate_access_key()
                completed = server.wait_operation(operation)
                if completed.get("state") != "succeeded":
                    raise ServerError(f"access-key operation failed: {completed.get('error')}")
                return str(completed.get("result", {}).get("key", ""))
            except ServerUnavailable:
                pass
        with self.ensured_connection(AccessLevel.Controller):
            return self.run_command("RAND?")

    def get_zone_count(self) -> int:
        """Query the number of temperature control zones from the server.

        Returns:
            The number of zones (typically 6 for current QuantStudio instruments).
        """
        server = self._semantic_server("instrument")
        if server is not None:
            try:
                return int(server.instrument_status()["zone_count"])
            except (ServerError, OSError, HTTPException):
                log.debug("semantic zone-count query failed; using direct SCPI", exc_info=True)
        with self.ensured_connection(AccessLevel.Observer):
            return int(self.run_command("TBC:ControlZones?"))

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

    @_ensure_connection(AccessLevel.Controller)
    def set_status_led(
        self,
        color: StatusLedColor | str,
        mode: StatusLedMode | str = "on",
    ) -> None:
        """Set the front-panel status LED.

        This is the machine's indicator light, not the optical excitation lamp.

        Parameters
        ----------
        color
            One of red, green, blue, yellow, cyan, magenta, white
            (a :any:`StatusLedColor` or its name, case-insensitive).
        mode
            "on" (solid, the default), "blink", or "off"
            (a :any:`StatusLedMode` or its name).

        Notes
        -----
        Requires Controller access.
        """
        self.run_command(StatusLedSet(color, mode).command_string())

    @_ensure_connection(AccessLevel.Controller)
    def status_led_off(self) -> None:
        """Turn the front-panel status LED off. Requires Controller access."""
        self.run_command("LED:LightOFF")

    @property
    @_ensure_connection(AccessLevel.Observer)
    def status_led(self) -> StatusLedState:
        """Current color and mode of the front-panel status LED.

        Reading returns a :any:`StatusLedState` (``.color`` is ``None`` when off).
        Setting accepts a color name/:any:`StatusLedColor` (solid on), or a
        ``(color, mode)`` tuple.
        """
        return StatusLedState.from_bytes(self.run_command_bytes(StatusLedState.command()))

    @status_led.setter
    @_ensure_connection(AccessLevel.Controller)
    def status_led(
        self,
        value: StatusLedColor | str | tuple[StatusLedColor | str, StatusLedMode | str],
    ) -> None:
        if isinstance(value, tuple):
            color, mode = value
        else:
            color, mode = value, "on"
        self.set_status_led(color, mode)

    @property
    @_ensure_connection(AccessLevel.Observer)
    def block(self) -> tuple[bool, float]:
        """Returns whether the block is currently temperature-controlled, and the current block temperature setting."""
        sbool, v = self.run_command("BLOCK?").split()
        sbool = sbool.lower()
        v = float(v)

        if sbool in ("on", "true"):
            return True, v
        elif sbool in ("off", "false"):
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
        server = self._semantic_server("runs")
        if server is not None:
            try:
                return self._run_status_from_server(server.current_run())
            except ServerError:
                log.debug("semantic run status failed; using direct SCPI", exc_info=True)
        with self.ensured_connection(AccessLevel.Observer):
            out = self.run_command_bytes(RunStatus.command())
            return RunStatus.from_bytes(out)

    @property
    def drawer_position(self) -> DrawerPosition:
        """Return the drawer position from the DRAW? command."""
        server = self._semantic_server("instrument")
        if server is not None:
            try:
                return cast(DrawerPosition, str(server.instrument_status()["drawer"]).title())
            except ServerError:
                log.debug("semantic drawer status failed; using direct SCPI", exc_info=True)
        with self.ensured_connection(AccessLevel.Observer):
            d = self.run_command("DRAW?")
            if d not in ["Open", "Closed", "Unknown"]:
                raise ValueError(f"Drawer position {d} is not understood.")
            return cast(DrawerPosition, d)

    @property
    def cover_position(self) -> CoverPosition:
        """Return the cover position from the ENG? command. Note that
        this does not always seem to work."""
        server = self._semantic_server("instrument")
        if server is not None:
            try:
                return cast(CoverPosition, str(server.instrument_status()["cover"]).title())
            except ServerError:
                log.debug("semantic cover status failed; using direct SCPI", exc_info=True)
        with self.ensured_connection(AccessLevel.Observer):
            f = self.run_command("ENG?")
            if f not in ["Up", "Down", "Unknown", ""]:
                raise ValueError(f"Cover position {f} is not understood.")
            if f == "":
                log.error("Cover position is blank. This should not happen.")
            return cast(CoverPosition, f)

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
        """Cleanly disconnect from the machine by sending QUIT."""
        if self._connection is None:
            return

        try:
            self._connection.disconnect()
        except Exception:
            pass  # Best-effort: ignore errors during disconnect
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
        server = self._semantic_server("instrument")
        if server is not None:
            try:
                return bool(server.instrument_status()["power_enabled"])
            except ServerError:
                log.debug("semantic power query failed; using direct SCPI", exc_info=True)
        with self.ensured_connection(AccessLevel.Observer):
            s = self.run_command("POW?").lower()
            if s in ("on", "true"):
                return True
            elif s in ("off", "false"):
                return False
            else:
                raise ValueError(f"Unexpected power status: {s}")

    @power.setter
    def power(self, value: Literal["on", "off", True, False]) -> None:
        server = self._semantic_server("instrument", mutation=True)
        if server is not None:
            try:
                server.set_power(value is True or (isinstance(value, str) and value.lower() == "on"))
                return
            except ServerUnavailable:
                pass
        with self.ensured_connection(AccessLevel.Controller):
            if value is True:
                value = "on"
            elif value is False:
                value = "off"
            self.run_command(f"POW {value}")

    @property
    def current_run_name(self) -> str | None:
        """Name of current run, or None if no run is active."""
        server = self._semantic_server("runs")
        if server is not None:
            try:
                status = server.current_run()
                if str(status.get("state", "")).lower() == "idle" or status.get("name") == "-":
                    return None
                return str(status["name"])
            except ServerError:
                log.debug("semantic current-run query failed; using direct SCPI", exc_info=True)
        with self.ensured_connection(AccessLevel.Observer):
            out = self.run_command("RUNTitle?")
            if out == "-":
                return None
            else:
                return re.sub(r"(<([\w.]+)>)?([^<]+)(</[\w.]+>)?", r"\3", out)

    def restart_system(self) -> None:
        """Restart the system (both the InstrumentServer and android interface) by killing the zygote process."""
        server = self._semantic_server("instrument", mutation=True)
        if server is not None:
            try:
                operation = server.restart_instrument()
                # The HTTP server may disappear after acknowledgement. The durable
                # operation resource is still the only safe result to inspect.
                server.wait_operation(operation)
                return
            except ServerUnavailable:
                pass
        with self.ensured_connection(AccessLevel.Controller):
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
        try:
            yield self
        finally:
            try:
                self.set_access_level(fac, fex, fst)
            except BaseException:
                # A context must never hand a potentially elevated connection
                # back to its caller when exact tuple restoration failed.
                self.disconnect()
                raise
            log.debug(f"Dropped access level {access_level}, returning to {fac} exclusive={fex} stealth={fst}.")

    @contextmanager
    def ensured_connection(self, access_level: AccessLevel = AccessLevel.Observer) -> Generator[Machine, None, None]:
        if self.automatic:
            was_connected = self.connected
            old_access = self._current_access_level
            if not was_connected:
                self.connect()
                try:
                    self.set_access_level(max(old_access, access_level))
                except BaseException:
                    self.disconnect()
                    raise
            elif old_access < access_level:
                self.set_access_level(access_level)
            try:
                yield self
            finally:
                if not was_connected:
                    self.disconnect()
                elif old_access < access_level:
                    try:
                        self.set_access_level(old_access)
                    except BaseException:
                        self.disconnect()
                        raise
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

        if ("collected" in res) and res["collected"]:
            raise AlreadyCollectedError(res)

        self.run_command(f'exp:run -asynchronous <block> zip "{run_name}.eds" "{run_name}" </block>')

        self.run_command(f'file:move "experiments:{run_name}.eds" "public_run_complete:{run_name}.eds"')

        self.run_command(f'exp:attr= "{run_name}" collected True')

    def get_exp_file(self, path: str, encoding: Literal["plain", "base64"] = "base64") -> bytes:
        reply = self.run_command_to_bytes(f"EXP:READ? -encoding={encoding} {shlex.quote(path)}")
        if not reply.startswith(b"<quote>\n") or not reply.endswith(b"</quote>"):
            raise ValueError("Unexpected reply format: expected <quote>...</quote>")
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
    ) -> tuple[Any, list[tuple[str, bytes]]]: ...

    @overload
    def get_filterdata_one(
        self,
        ref: FilterDataFilename,
        *,
        run: str | None = None,
        return_files: bool = False,
    ) -> Any: ...

    def get_filterdata_one(
        self,
        ref: FilterDataFilename,
        *,
        run: str | None = None,
        return_files: bool = False,
    ) -> Any:
        """Fetch a single filterdata reading from the machine.

        Returns a Rust PlateData object with timestamp set from quant data.
        """
        from ._qslib import FilterDataCollection, QuantFile

        if run is None:
            run = self.get_run_title()

        fl = self.get_exp_file(f"{run}/apldbio/sds/filter/" + ref.tostring())
        fdc = FilterDataCollection.from_xml_bytes(fl)

        if not fdc.plate_point_data or not fdc.plate_point_data[0].plate_data:
            raise ValueError("PlateData not found")

        plate_data = fdc.plate_point_data[0].plate_data[0]

        # Build quant filename from the reference
        reading_str = f"S{ref.stage:02}_C{ref.cycle:03}_T{ref.step:02}_P{ref.point:04}_{ref.filterset.upperform}"
        ql = self.get_expfile_list(f"{run}/apldbio/sds/quant/{reading_str}_E*.quant")[-1]
        qf_bytes = self.get_exp_file(ql)
        qf = QuantFile.parse(qf_bytes.decode())
        plate_data.timestamp = qf.conditions.timestamp

        if return_files:
            files = [("filter/" + ref.tostring(), fl)]
            qn = re.search("quant/.*$", ql)
            assert qn is not None
            files.append((qn[0], qf_bytes))
            return plate_data, files
        else:
            return plate_data

    def get_all_filterdata(self, run: str | None = None, as_list: bool = False) -> Any:
        """Fetch all filterdata from the machine.

        Returns a list of Rust PlateData objects (as_list=True) or a Polars DataFrame.
        """
        if run is None:
            run = self.get_run_title()

        plate_data_list = [
            self.get_filterdata_one(FilterDataFilename.fromstring(x), run=run)
            for x in self.get_expfile_list(f"{run}/apldbio/sds/filter/*_filterdata.xml")
        ]

        if as_list:
            return plate_data_list

        import polars as pl_mod

        frames = [p.to_polars() for p in plate_data_list]
        return pl_mod.concat(frames)

    def get_expfile_list(self, glob: str, allow_nomatch: bool = False) -> list[str]:
        try:
            fl = self.run_command(SCPICommand("EXP:LIST?", glob))
        except ValueError as ce:  # FIXME
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
