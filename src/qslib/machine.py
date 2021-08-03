from __future__ import annotations
from abc import ABC, abstractmethod
import base64
from dataclasses import dataclass, field
import io
import logging
from os import PathLike
from typing import Any, Callable, ClassVar, IO, List, Literal, Type, TypeVar
import zipfile

from qslib.tcprotocol import Protocol
from .qsconnection_async import AccessLevel, QSConnectionAsync
import asyncio
from contextlib import contextmanager
import re
import shlex
from typeguard import typechecked
import nest_asyncio
import logging

nest_asyncio.apply()


def _get_protodef_or_def(var, default):
    return f"$[ top.getChild('PROTOcolDEFinition').variables.get('{var}'.lower(), {default}) ]".encode()


T = TypeVar("T", bound="BaseStatus")

log = logging.getLogger(__name__)


class BaseStatus(ABC):
    @classmethod
    @property
    @abstractmethod
    def _comlist(cls: Type[T]) -> dict[str, tuple[bytes, Callable]]:
        ...

    @classmethod
    @property
    @abstractmethod
    def _com(cls: Type[T]) -> bytes:
        ...

    @classmethod
    def from_machine(cls: Type[T], connection: "Machine") -> T:
        out = connection.run_command_bytes(cls._com)
        return cls.from_bytes(out)

    @classmethod
    def from_bytes(cls: Type[T], out: bytes) -> Type[T]:
        return cls(
            **{
                k: inst(v)
                for (k, (_, inst)), v in zip(
                    cls._comlist.items(), shlex.split(out.decode())
                )
            }
        )


@dataclass
class RunStatus(BaseStatus):
    name: str
    stage: int
    num_stages: int
    cycle: int
    num_cycles: int
    step: int
    point: int
    state: str

    _comlist: ClassVar[dict[str, tuple[bytes, Callable]]] = {
        "name": (b"${RunTitle:--}", str),
        "stage": (b"${Stage:--1}", int),
        "num_stages": (_get_protodef_or_def("${RunMacro}-Stages", -1), int),
        "cycle": (b"${Cycle:--1}", int),
        "num_cycles": (
            _get_protodef_or_def("${RunMacro}-Stage${Stage}-Count", -1),
            int,
        ),
        "step": (b"${Step:--1}", int),
        "point": (b"${Point:--1}", int),
        "state": (b"$(ISTAT?)", str),
    }
    _com: ClassVar[bytes] = b"RET " + b" ".join(v for v, _ in _comlist.values())


@dataclass
class MachineStatus(BaseStatus):
    drawer: str
    cover: str
    lamp_status: str

    _comlist: ClassVar[dict[str, tuple[bytes, Callable]]] = {
        "drawer": (b"$(DRAWER?)", str),
        "cover": (b'$[ "$(ENG?)" or "unknown" ]', str),
        "lamp_status": (b"$(LST?)", str),
    }

    _com: ClassVar[bytes] = b"RET " + b" ".join(v for v, _ in _comlist.values())


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

    Examples
    --------

    Set up a connection

    """

    host: str
    password: str | None
    max_access_level: AccessLevel
    port: int = 7000
    _initial_access_level: AccessLevel

    def __init__(
        self,
        host,
        password: str | None = None,
        max_access_level: AccessLevel = "Observer",
        port: int = 7000,
        connect_now: bool = False,
        _initial_access_level: AccessLevel = "Observer",
    ):
        self.host = host
        self.port = port
        self.password = password
        self.max_access_level = max_access_level
        self._initial_access_level = _initial_access_level

        if connect_now:
            self.connect()

    def connect(self):
        """Open the connection."""
        loop = asyncio.get_event_loop()
        self._qsc = QSConnectionAsync(
            self.host,
            self.port,
            password=self.password,
            initial_access_level=self._initial_access_level,
        )
        loop.run_until_complete(self._qsc.connect())

    def __enter__(self):
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
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._qsc.run_command(command))

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
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._qsc.run_command(command, just_ack=True))

    def define_protocol(self, protocol: Protocol):
        """Send a protocol to the machine. This *is not related* to a particular
        experiment.  The name on the machine is set by the protocol.

        Parameters
        ----------
        protocol : Protocol
            protocol to send
        """
        self.run_command(f"{protocol.to_command()}")

    def read_dir_as_zip(self, path: str, leaf="FILE") -> zipfile.ZipFile:
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
        x = self.run_command_bytes(f"{leaf}:ZIPREAD? {path}")

        return zipfile.ZipFile(io.BytesIO(base64.decodebytes(x[7:-10])))

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
        if not context:
            contexts = ""
        elif context[-1] == ":":
            contexts = context
        else:
            contexts = context + ":"
        x = self.run_command_bytes(f"{leaf}:READ? {contexts}{path}")

        return base64.decodebytes(x[7:-10])

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
        return [re.sub("^public_run_complete:", "", s) for s in a if s.endswith("eds")]

    def load_run_from_storage(self, path: str) -> "Experiment":  # type: ignore
        from qslib.experiment import Experiment

        """Load a run from machine storage as an Experiment
        """
        return Experiment.from_machine_storage(self, path)

    def save_run_from_storage(
        self, machine_path: str, download_path: str | IO[bytes], overwrite=False
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
        if isinstance(command, str):
            command = command.encode()
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._qsc._protocol.run_command(command))

    def run_status(self) -> RunStatus:
        """Return information on the status of any run."""
        return RunStatus.from_machine(self)

    def machine_status(self) -> MachineStatus:
        """Return information on the status of the machine."""
        return MachineStatus.from_machine(self)

    @typechecked
    def set_access_level(
        self,
        access_level: Literal[
            "Guest", "Observer", "Controller", "Administrator", "Full"
        ],
        exclusive: bool = False,
        stealth: bool = False,
        _log: bool = True,
    ):
        self.run_command(
            f"ACC -stealth={stealth} -exclusive={exclusive} {access_level}"
        )
        if _log:
            log.info(f"Took access level {access_level} {exclusive=} {stealth=}")

    @typechecked
    def get_access_level(
        self,
    ) -> tuple[
        Literal["Guest", "Observer", "Controller", "Administrator", "Full"], bool, bool
    ]:
        ret = self.run_command("ACC?")
        m = re.match(r"^-stealth=(\w+) -exclusive=(\w+) (\w+)", ret)
        if m is None:
            raise ValueError(ret)
        return (m[3], m[2] == "True", m[1] == "True")  # type: ignore

    def drawer_open(self):
        """Open the machine drawer using the OPEN command. This will ensure proper
        cover/drawer operation.  It *will not check run status*, and will open and
        close the drawer during runs and potentially during imaging.
        """
        self.run_command("OPEN")

    def drawer_close(self, lower_cover: bool = False):
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

    def cover_lower(self):
        """Lower/engage the plate cover, closing the drawer if needed."""
        self.drawer_close(lower_cover=False)
        self.run_command("COVerDOWN")

    def __exit__(self, exc_type: type, exc: Exception, tb: Any):
        self.disconnect()

    def __del__(self):
        self.disconnect()

    def disconnect(self):
        """Cleanly disconnect from the machine."""
        self.run_command("QUIT")
        self._qsc._transport.close()

    def abort_current_run(self):
        """Abort (stop immediately) the current run."""
        self.run_command("AbortRun ${RunTitle}")

    def stop_current_run(self):
        """Stop (stop after cycle end) the current run."""
        self.run_command("StopRun ${RunTitle}")

    def pause_current_run(self):
        """Pause the current run now."""
        self.run_command_to_ack("PAUSe")

    def pause_current_run_at_temperature(self):
        raise NotImplementedError

    def resume_current_run(self):
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

    @property
    def current_run_name(self) -> str | None:
        """Name of current run, or None if no run is active."""
        out = self.run_command("RUNTitle?")
        if out == "-":
            return None
        else:
            return out

    @power.setter
    @typechecked
    def power(self, value: Literal["on", "off", True, False]):  # type: ignore
        if value is True:
            value = "on"
        elif value is False:
            value = "off"
        self.run_command(f"POW {value}")

    @contextmanager
    def at_access(self, access_level, exclusive=False, stealth=False):
        fac, fex, fst = self.get_access_level()
        self.set_access_level(access_level, exclusive, stealth, _log=False)
        log.info(f"Took access level {access_level} {exclusive=} {stealth=}.")
        yield self
        self.set_access_level(fac, fex, fst, _log=False)
        log.info(
            f"Dropped access level {access_level}, returning to {fac} exclusive={fex} stealth={fst}."
        )