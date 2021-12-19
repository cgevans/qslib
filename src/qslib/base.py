from __future__ import annotations
import shlex
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Tuple, TypeVar, Callable, Type, ClassVar
import re

T = TypeVar("T", bound="BaseStatus")

if False:  # for mypy
    from .machine import Machine


class BaseStatus(ABC):
    @classmethod
    @property
    @abstractmethod
    def _comlist(
        cls: Type[T],
    ) -> Dict[str, Tuple[bytes, Callable[[Any], Any]]]:  # pragma: no cover
        ...

    @classmethod
    @property
    @abstractmethod  # pragma: no cover
    def _com(cls: Type[T]) -> bytes:  # pragma: no cover
        ...

    @classmethod
    def from_machine(cls: Type[T], connection: "Machine") -> T:  # type: ignore
        out = connection.run_command_bytes(cls._com)
        return cls.from_bytes(out)

    @classmethod
    def from_bytes(cls: Type[T], out: bytes) -> T:
        return cls(
            **{
                k: inst(v)
                for (k, (_, inst)), v in zip(
                    cls._comlist.items(), shlex.split(out.decode())
                )
            }
        )


def _get_protodef_or_def(var: str, default: Any) -> bytes:
    return f"$[ top.getChild('PROTOcolDEFinition').variables.get('{var}'.lower(), {default}) ]".encode()


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

    _comlist: ClassVar[Dict[str, Tuple[bytes, Callable[[Any], Any]]]] = {
        "name": (
            b"${RunTitle:--}",
            lambda out: re.sub(r"(<([\w.]+)>)?([^<]+)(</[\w.]+>)?", r"\3", out),
        ),
        "stage": (b"${Stage:--1}", lambda x: int(x) if x != "PRERUN" else 0),
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

    _comlist: ClassVar[Dict[str, Tuple[bytes, Callable[[Any], Any]]]] = {
        "drawer": (b"$(DRAWER?)", str),
        "cover": (b'$[ "$(ENG?)" or "unknown" ]', str),
        "lamp_status": (b"$(LST?)", str),
    }

    _com: ClassVar[bytes] = b"RET " + b" ".join(v for v, _ in _comlist.values())


_accesslevel_order: Dict[str, int] = {
    "Guest": 0,
    "Observer": 1,
    "Controller": 2,
    "Administrator": 3,
    "Full": 4,
}


class AccessLevel(Enum):
    Guest = "Guest"
    Observer = "Observer"
    Controller = "Controller"
    Administrator = "Administrator"
    Full = "Full"
    value: str

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, AccessLevel):
            other = AccessLevel(other)
        return _accesslevel_order[self.value] > _accesslevel_order[other.value]

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, AccessLevel):
            other = AccessLevel(other)
        return _accesslevel_order[self.value] >= _accesslevel_order[other.value]

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, AccessLevel):
            other = AccessLevel(other)
        return _accesslevel_order[self.value] < _accesslevel_order[other.value]

    def __le__(self, other: object) -> bool:
        if not isinstance(other, AccessLevel):
            other = AccessLevel(other)
        return _accesslevel_order[self.value] <= _accesslevel_order[other.value]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AccessLevel):
            other = AccessLevel(other)
        return _accesslevel_order[self.value] == _accesslevel_order[other.value]

    def __str__(self) -> str:
        return self.value
