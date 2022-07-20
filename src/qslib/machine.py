# SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
#
# SPDX-License-Identifier: AGPL-3.0-only

from __future__ import annotations

import asyncio
import base64
import logging
import re
import zipfile
from asyncio.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
from typing import IO, TYPE_CHECKING, Any, Generator, Literal, cast, overload

import nest_asyncio

from qslib.qs_is_protocol import CommandError
from qslib.scpi_commands import AccessLevel, SCPICommand

from ._util import _unwrap_tags
from .protocol import Protocol
from .qsconnection_async import QSConnectionAsync

nest_asyncio.apply()

from .base import MachineStatus, RunStatus

log = logging.getLogger(__name__)

if TYPE_CHECKING:  # pragma: no cover
    import matplotlib.pyplot as plt

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
        The port to connect to. (Use the normal SCPI port, not the line-editor connection usually
        on 2323).  Default is 7000.

    """

    host: str
    password: str | None = None
    automatic: bool = True
    _max_access_level: AccessLevel = AccessLevel.Controller
    port: int = 7000
    _initial_access_level: AccessLevel = AccessLevel.Observer
    _current_access_level: AccessLevel = AccessLevel.Guest
    _connection: QSConnectionAsync | None = None

    def asdict(self, password: bool = False) -> dict[str, str | int]:
        d: dict[str, str | int] = {"host": self.host}
        if self.password and password:
            d["password"] = self.password
        if self.max_access_level != Machine._max_access_level:
            d["max_access_level"] = self.max_access_level.value
        if self.port != Machine.port:
            d["port"] = self.port
        if self.automatic != Machine.automatic:
            d["automatic"] = self.automatic

        return d

    @property
    def connection(self) -> QSConnectionAsync:
        """The :class:`QSConnectionAsync` for the connection, or a :class:`ConnectionError`."""
        if self._connection is None:
            raise ConnectionError
        else:
            return self._connection

    @connection.setter
    def connection(self, v: QSConnectionAsync | None) -> None:
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
        port: int = 7000,
        _initial_access_level: AccessLevel | str = AccessLevel.Observer,
    ):
        self.host = host
        self.port = port
        self.password = password
        self.automatic = automatic
        self.max_access_level = AccessLevel(max_access_level)
        self._initial_access_level = AccessLevel(_initial_access_level)
        self._connection = None

    def connect(self) -> None:
        """Open the connection manually."""
        loop = asyncio.get_event_loop()

        self.connection = QSConnectionAsync(
            self.host,
            self.port,
            password=self.password,
            initial_access_level=self._initial_access_level,
        )
        loop.run_until_complete(self.connection.connect())
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
        if self.connection is None:
            raise ConnectionError(f"Not connected to {self.host}.")
        loop = asyncio.get_event_loop()
        try:
            return loop.run_until_complete(self.connection.run_command(command))
        except CommandError as e:
            e.__traceback__ = None
            raise e

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
        loop = asyncio.get_event_loop()
        try:
            return loop.run_until_complete(
                self.connection.run_command(command, just_ack=True)
            )
        except CommandError as e:
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
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.connection._protocol.run_command(command))

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
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.connection.read_dir_as_zip(path, leaf))

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

    @_ensure_connection(AccessLevel.Observer)
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
            self.connection.list_files(
                path, leaf=leaf, verbose=verbose, recursive=recursive
            )
        )

    @_ensure_connection(AccessLevel.Observer)
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
            self.connection.read_file(path, context, leaf)
        )

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

    @_ensure_connection(AccessLevel.Observer)
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
        logfuture: Future[
            tuple[bytes, bytes, Future[tuple[bytes, bytes, None]] | None]
        ] = asyncio.Future()
        if self.connection is None:
            raise Exception
        if isinstance(name, bytes):
            name = name.decode()
        self.connection._protocol.waiting_commands.append((b"logtransfer", logfuture))

        logcommand = self.connection._protocol.run_command(
            f"eval? session.writeQueue.put(('OK logtransfer \\<quote.base64\\>\\\\n'"
            f" + (lambda x: [x.seek({byte}), __import__('base64').encodestring(x.read())][1])"
            f"(open('/data/vendor/IS/experiments/{name}/apldbio/sds/messages.log')) +"
            " '\\</quote.base64\\>\\\\n', None))",
            ack_timeout=200,
        )

        loop = asyncio.get_event_loop()

        loop.run_until_complete(logcommand)

        loop.run_until_complete(logfuture)

        return base64.decodebytes(logfuture.result()[1][15:-17])

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

    @property
    def access_level(self) -> AccessLevel:
        return self._current_access_level

    @access_level.setter
    def access_level(self, v: AccessLevel | str) -> None:
        with self.ensured_connection(AccessLevel.Guest):
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

        loop = asyncio.get_event_loop()

        loop.run_until_complete(self.connection.disconnect())
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
            if not was_connected:
                self.connect()
            old_access = self.access_level
            if old_access < access_level:
                self.set_access_level(access_level)
            yield self
            if not was_connected:
                self.disconnect()
            elif old_access < access_level:
                self.set_access_level(old_access)
        else:
            yield self
