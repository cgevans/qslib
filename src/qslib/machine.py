from __future__ import annotations
from abc import ABC, abstractmethod
import base64
from dataclasses import dataclass
import io
from typing import Any, Callable, ClassVar, Literal, Type, TypeVar
import zipfile
from .qsconnection_async import QSConnectionAsync
import asyncio
from contextlib import contextmanager
import re
import shlex
from typeguard import typechecked
import nest_asyncio

nest_asyncio.apply()


def _get_protodef_or_def(var, default):
    return f"$[ top.getChild('PROTOcolDEFinition').variables.get('{var}'.lower(), {default}) ]".encode()


T = TypeVar("T", bound="BaseStatus")


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
    host: str
    port: int | str
    password: str | None

    def __init__(
        self,
        host,
        port,
        password=None,
        initial_access_level="Observer",
        connect_now=False,
    ):
        self.host = host
        self.port = port
        self.password = password

        if connect_now:
            self.connect()

    def connect(self):
        loop = asyncio.get_event_loop()
        self._qsc = QSConnectionAsync(
            self.host,
            self.port,
            password=self.password,
            initial_access_level="Observer",
        )
        loop.run_until_complete(self._qsc.connect())

    def __enter__(self):
        self.connect()
        return self

    def run_command(self, command):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._qsc.run_command(command))

    def run_command_to_ack(self, command):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._qsc.run_command(command, just_ack=True))

    def define_protocol(self, protocol):
        self.run_command(f"{protocol.to_command()}")

    def read_dir_as_zip(self, path, leaf="FILE"):
        x = self.run_command_bytes(f"{leaf}:ZIPREAD? {path}")

        return zipfile.ZipFile(io.BytesIO(base64.decodebytes(x[7:-10])))

    def run_command_bytes(self, command: str | bytes) -> bytes:
        if isinstance(command, str):
            command = command.encode()
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._qsc._protocol.run_command(command))

    @typechecked
    def set_access_level(
        self,
        access_level: Literal[
            "Guest", "Observer", "Controller", "Administrator", "Full"
        ],
        exclusive: bool = False,
        stealth: bool = False,
    ):
        self.run_command(
            f"ACC -stealth={stealth} -exclusive={exclusive} {access_level}"
        )

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
        self.run_command("OPEN")

    def drawer_close(self, lower_cover=False):
        self.run_command("CLOSE")
        if lower_cover:
            self.cover_lower()

    @property
    def status(self):
        return RunStatus.from_machine(self)

    @property
    def drawer_position(self):
        return self.run_command("DRAW?")

    @property
    def cover_position(self):
        return self.run_command("ENG?")

    def cover_lower(self):
        self.drawer_close(lower_cover=False)
        self.run_command("COVerDOWN")

    def __exit__(self, exc_type: type, exc: Exception, tb: Any):
        self.disconnect()

    def __del__(self):
        self.disconnect()

    def disconnect(self):
        self.run_command("QUIT")
        self._qsc._transport.close()

    def abort_current_run(self):
        self.run_command("AbortRun ${RunTitle}")

    def stop_current_run(self):
        self.run_command("StopRun ${RunTitle}")

    def pause_current_run(self):
        self.run_command_to_ack("PAUSe")

    def pause_current_run_at_temperature(self):
        raise NotImplementedError

    def resume_current_run(self):
        self.run_command_to_ack("RESume")

    @property
    def power(self):
        s = self.run_command("POW?").lower()
        if s == "on":
            return True
        elif s == "off":
            return False
        else:
            raise ValueError(f"Unexpected power status: {s}")

    @property
    def current_run_title(self):
        out = self.run_command("RUNTitle?")
        if out == "-":
            return None
        else:
            return out

    @power.setter
    @typechecked
    def power(self, value: Literal["on", "off", True, False]):
        if value is True:
            value = "on"
        elif value is False:
            value = "off"
        self.run_command(f"POW {value}")

    @contextmanager
    def at_access(self, access_level, exclusive=False, stealth=False):
        fac, fex, fst = self.get_access_level()
        self.set_access_level(access_level, exclusive, stealth)
        yield self
        self.set_access_level(fac, fex, fst)
