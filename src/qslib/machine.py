from __future__ import annotations

import asyncio
from asyncio.futures import Future
import base64
import io
import logging
import re
import zipfile
from contextlib import contextmanager
from dataclasses import astuple, dataclass
from typing import Any, Generator, IO, Literal, overload

import nest_asyncio
import paramiko.pkey

from qslib.qs_is_protocol import CommandError

from .qsconnection_async import QSConnectionAsync
from .tcprotocol import Protocol
from .util import _unwrap_tags
import shlex

from sshtunnel import SSHTunnelForwarder

nest_asyncio.apply()

from .base import RunStatus, MachineStatus, AccessLevel

log = logging.getLogger(__name__)


@dataclass(init=False)
class Machine:
    """
    A connection to a QuantStudio machine.  The connection can be opened and closed, and reused.
    A maximum access level can be set and changed, which will prevent the access level from going
    above that level.  By default, the initial connection is as Observer.

    For clean access, the class provides a context manager for the connection, and the
    `at_level` method provides a context manager for access level:

    >>> with Machine('machine', 'password', max_access_level="Controller") as m:
    >>>     # Now connected
    >>>     print(m.run_status())  # runs at Observer level
    >>>     with m.at_level("Controller", exclusive=True):
    >>>         # Runs as Controller, and fails if another Controller is connected (exclusive)
    >>>         m.abort_run()
    >>>         m.drawer_open()
    >>>     # Now back to Observer
    >>>     print(m.status())
    >>> # Now disconnected.

    The connection context manager can also be used with :code:`with m:` form for a Machine
    instance :code:`m` that already exists, in which case it will connect and disconnect.

    If you don't want to use these, you can also use :any:`connect` and :any:`disconnect`.

    Note that there is *no supported method* on the machine's server for removing hanging connections
    other than a reboot, and AB's software will not start runs when other connections hold Controller
    level.

    Parameters
    ----------

    host: str
        The host name or IP to connect to.

    password: str
        The password to use. Note that this class does not obscure or protect the password at all,
        because it should not be relied on for security.  See :ref:`access-and-security`  for more
        information.

    max_access_level: "Observer", "Controller", "Administrator", or "Full"
        The maximum access level to allow.  This is *not* the initial access level, which
        will be Observer. The parameter can be changed later by changing the :code:`max_access_level`
        attribute.

    port: int (default = 7000)
        The port to connect to. (Use the normal SCPI port, not the line-editor connection usually
        on 2323).

    tunnel_host: str or tuple[str, int], optional
        If set, to a hostname/IP string or (hostname/IP, port) tuple, create an SSH tunnel to the tunnel_host
        in order to connect to the machine, rather than connecting directly.  This uses paramiko, and
        will not read your ssh configuration file for any host aliases, but will use your keys if you
        have an ssh-agent running.

    tunnel_user: str, optional
        If set, specify the user for the tunnel connection.  If unset, the local user name is used.

    tunnel_key: str or paramiko.pkey.PKey, optional
        If set, specify the filename of a private key, or the paramiko-loaded key, to use for the tunnel
        connection.

    Examples
    --------

    Set up a connection

    """

    host: str
    password: str | None = None
    max_access_level: AccessLevel = AccessLevel.Observer
    port: int = 7000
    _initial_access_level: AccessLevel = AccessLevel.Observer
    _qsc_real: QSConnectionAsync | None = None

    def asdict(self, password: bool = False) -> dict[str, str | int]:
        d: dict[str, str | int] = {"host": self.host}
        if self.password and password:
            d["password"] = self.password
        if self.max_access_level != Machine.max_access_level:
            d["max_access_level"] = self.max_access_level.value
        if self.port != Machine.port:
            d["port"] = self.port

        return d

    @property
    def _qsc(self) -> QSConnectionAsync:
        if self._qsc_real is None:
            raise ConnectionError
        else:
            return self._qsc_real

    @_qsc.setter
    def _qsc(self, v: QSConnectionAsync | None) -> None:
        self._qsc_real = v

    def __init__(
        self,
        host: str,
        password: str | None = None,
        max_access_level: AccessLevel | str = AccessLevel.Observer,
        port: int = 7000,
        connect_now: bool = False,
        _initial_access_level: AccessLevel | str = AccessLevel.Observer,
    ):
        self.host = host
        self.port = port
        self.password = password
        self.max_access_level = AccessLevel(max_access_level)
        self._initial_access_level = AccessLevel(_initial_access_level)
        self._qsc_real = None

        if connect_now:
            self.connect()

    def connect(self) -> None:
        """Open the connection."""
        loop = asyncio.get_event_loop()

        self._qsc = QSConnectionAsync(
            self.host,
            self.port,
            password=self.password,
            initial_access_level=self._initial_access_level,
        )
        loop.run_until_complete(self._qsc.connect())

    @property
    def connected(self) -> bool:
        if (not hasattr(self, "_qsc_real")) or (self._qsc_real is None):
            return False
        else:
            return self._qsc.connected

    def __enter__(self) -> Machine:
        self.connect()
        return self

    def run_command(self, command: str) -> str:
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
        if self._qsc is None:
            raise ConnectionError(f"Not connected to {self.host}.")
        loop = asyncio.get_event_loop()
        try:
            return loop.run_until_complete(self._qsc.run_command(command))
        except CommandError as e:
            e.__traceback__ = None
            raise e

    def run_command_to_ack(self, command: str) -> str:
        """Run an SCPI command, and return the response as a string.
        Returns after the command is processed (OK or NEXT), but potentially
        before it has completed (NEXT).

        Parameters
        ----------
        command : str
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
        if self._qsc is None:
            raise ConnectionError(f"Not connected to {self.host}")
        loop = asyncio.get_event_loop()
        try:
            return loop.run_until_complete(
                self._qsc.run_command(command, just_ack=True)
            )
        except CommandError as e:
            e.__traceback__ = None
            raise e

    def define_protocol(self, protocol: Protocol) -> None:
        """Send a protocol to the machine. This *is not related* to a particular
        experiment.  The name on the machine is set by the protocol.

        Parameters
        ----------
        protocol : Protocol
            protocol to send
        """
        self.run_command(f"{protocol.to_command()}")

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
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._qsc.read_dir_as_zip(path, leaf))

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

    def list_files(
        self,
        path: str,
        *,
        leaf: str = "FILE",
        verbose: bool = False,
        recursive: bool = False,
    ) -> list[str] | list[dict[str, Any]]:

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(
            self._qsc.list_files(path, leaf=leaf, verbose=verbose, recursive=recursive)
        )

    def read_file(
        self, path: str, context: str | None = None, leaf: str = "FILE"
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
        return asyncio.get_event_loop().run_until_complete(
            self._qsc.read_file(path, context, leaf)
        )

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

    def list_runs_in_storage(self) -> list[str]:
        """List runs in machine storage.

        Returns
        -------
        list[str]
            run filenames.  Retrieve with load_run_from_storage
            (to open as :any`Experiment`) or save_run_from_storage
            (to download and save it without opening.)
        """
        x = self.run_command("FILE:LIST? public_run_complete:")
        a = x.split("\n")[1:-1]
        return [
            re.sub("^public_run_complete:", "", s)[:-4] for s in a if s.endswith(".eds")
        ]

    def load_run_from_storage(self, path: str) -> "Experiment":  # type: ignore
        from .experiment import Experiment

        """Load a run from machine storage as an Experiment
        """
        return Experiment.from_machine_storage(self, path)

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

    def run_command_bytes(self, command: str | bytes) -> bytes:
        """Run an SCPI command, and return the response as bytes (undecoded).
        Returns after the command is processed (OK or NEXT), but potentially
        before it has completed (NEXT).

        Parameters
        ----------
        command : str | bytes
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
        if self._qsc is None:
            raise ConnectionError(f"Not connected to {self.host}.")
        if isinstance(command, str):
            command = command.encode()
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._qsc._protocol.run_command(command))

    def _get_log_from_byte(self, name: str | bytes, byte: int) -> bytes:
        logfuture: Future[
            tuple[bytes, bytes, Future[tuple[bytes, bytes, None]] | None]
        ] = asyncio.Future()
        if self._qsc is None:
            raise Exception
        if isinstance(name, bytes):
            name = name.decode()
        self._qsc._protocol.waiting_commands.append((b"logtransfer", logfuture))

        logcommand = self._qsc._protocol.run_command(
            f"eval? session.writeQueue.put(('OK logtransfer \\<quote.base64\\>\\\\n' + (lambda x: [x.seek({byte}), __import__('base64').encodestring(x.read())][1])(open('/data/vendor/IS/experiments/{name}/apldbio/sds/messages.log')) + '\\</quote.base64\\>\\\\n', None))",
            ack_timeout=200,
        )

        loop = asyncio.get_event_loop()

        loop.run_until_complete(logcommand)

        loop.run_until_complete(logfuture)

        return base64.decodebytes(logfuture.result()[1][15:-17])

    def run_status(self) -> RunStatus:
        """Return information on the status of any run."""
        return RunStatus.from_machine(self)

    def machine_status(self) -> MachineStatus:
        """Return information on the status of the machine."""
        return MachineStatus.from_machine(self)

    def get_running_protocol(self) -> Protocol:
        p = _unwrap_tags(self.run_command("PROT? ${Protocol}"))
        pn, svs, rm = self.run_command(
            "RET ${Protocol} ${SampleVolume} ${RunMode}"
        ).split()
        p = f"PROT -volume={svs} -runmod={rm} {pn} " + p
        return Protocol.from_command(p)

    def set_access_level(
        self,
        access_level: AccessLevel | str,
        exclusive: bool = False,
        stealth: bool = False,
        _log: bool = True,
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
        if _log:
            log.info(f"Took access level {access_level} {exclusive=} {stealth=}")

    def get_access_level(
        self,
    ) -> tuple[AccessLevel, bool, bool]:
        ret = self.run_command("ACC?")
        m = re.match(r"^-stealth=(\w+) -exclusive=(\w+) (\w+)", ret)
        if m is None:
            raise ValueError(ret)
        return AccessLevel(m[3]), m[2] == "True", m[1] == "True"

    def drawer_open(self) -> None:
        """Open the machine drawer using the OPEN command. This will ensure proper
        cover/drawer operation.  It *will not check run status*, and will open and
        close the drawer during runs and potentially during imaging.
        """
        self.run_command("OPEN")

    def drawer_close(self, lower_cover: bool = False) -> None:
        """Close the machine drawer using the OPEN command. This will ensure proper
        cover/drawer operation.  It *will not check run status*, and will open and
        close the drawer during runs and potentially during imaging.

        By default, it will not lower the cover automaticaly after closing, use
        lower_cover=True to do so.
        """
        self.run_command("CLOSE")
        if lower_cover:
            self.cover_lower()

    @property
    def status(self) -> RunStatus:
        """Return the current status of the run."""
        return RunStatus.from_machine(self)

    @property
    def drawer_position(self) -> Literal["Open", "Closed", "Unknown"]:
        """Return the drawer position from the DRAW? command."""
        return self.run_command("DRAW?")  # type: ignore

    @property
    def cover_position(self) -> Literal["Up", "Down", "Unknown"]:
        """Return the cover position from the ENG? command. Note that
        this does not always seem to work."""
        f = self.run_command("ENG?")
        assert f in ["Up", "Down", "Unknown"]
        return f  # type: ignore

    def cover_lower(self) -> None:
        """Lower/engage the plate cover, closing the drawer if needed."""
        self.drawer_close(lower_cover=False)
        self.run_command("COVerDOWN")

    def __exit__(self, exc_type: type, exc: Exception, tb: Any) -> None:
        self.disconnect()

    def __del__(self) -> None:
        if self.connected:
            self.disconnect()

    def disconnect(self) -> None:
        """Cleanly disconnect from the machine."""
        if self._qsc is None:
            raise ConnectionError(f"Not connected to {self.host}.")

        loop = asyncio.get_event_loop()

        loop.run_until_complete(self._qsc.disconnect())
        self._qsc_real = None

    def abort_current_run(self) -> None:
        """Abort (stop immediately) the current run."""
        self.run_command("AbortRun ${RunTitle}")

    def stop_current_run(self) -> None:
        """Stop (stop after cycle end) the current run."""
        self.run_command("StopRun ${RunTitle}")

    def pause_current_run(self) -> None:
        """Pause the current run now."""
        self.run_command_to_ack("PAUSe")

    def pause_current_run_at_temperature(self) -> None:
        raise NotImplementedError

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
        s = self.run_command("POW?").lower()
        if s == "on":
            return True
        elif s == "off":
            return False
        else:
            raise ValueError(f"Unexpected power status: {s}")

    @power.setter
    def power(self, value: Literal["on", "off", True, False]):  # type: ignore
        if value is True:
            value = "on"
        elif value is False:
            value = "off"
        self.run_command(f"POW {value}")

    @property
    def current_run_name(self) -> str | None:
        """Name of current run, or None if no run is active."""
        out = self.run_command("RUNTitle?")
        if out == "-":
            return None
        else:
            return re.sub(r"(<([\w.]+)>)?([^<]+)(</[\w.]+>)?", r"\3", out)

    @contextmanager
    def at_access(
        self,
        access_level: AccessLevel | str,
        exclusive: bool = False,
        stealth: bool = False,
    ) -> Generator[Machine, None, None]:
        fac, fex, fst = self.get_access_level()
        self.set_access_level(access_level, exclusive, stealth, _log=False)
        log.info(f"Took access level {access_level} {exclusive=} {stealth=}.")
        yield self
        self.set_access_level(fac, fex, fst, _log=False)
        log.info(
            f"Dropped access level {access_level}, returning to {fac} exclusive={fex} stealth={fst}."
        )

    @contextmanager
    def ensured_connection(
        self, access_level: AccessLevel = AccessLevel.Observer
    ) -> Generator[Machine, None, None]:
        was_connected = self.connected
        if not was_connected:
            self.connect()
        old_access = self.get_access_level()
        if old_access[0] < access_level:
            self.set_access_level(access_level)
        yield self
        if not was_connected:
            self.disconnect()
        elif old_access[0] < access_level:
            self.set_access_level(*old_access)
