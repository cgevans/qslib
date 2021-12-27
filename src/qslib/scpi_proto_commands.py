import textwrap
from future import __annotations__
import dataclasses
import shlex
from typing import Protocol, Sequence, Type

import numpy as np
import pint
import attr
from abc import ABC, abstractmethod
from dataclasses import dataclass

UR = pint.get_application_registry()


@dataclass(init=False)
class SCPICommand:
    """
    A representation of an SCPI Command
    """

    command: str
    args: Sequence[
        str | int | float | np.number | Sequence[str | int | float | np.number]
    ]
    opts: dict[
        str, str | int | float | np.number | Sequence[str | int | float | np.number]
    ]

    def __init__(
        self,
        command: str,
        *args: str | int | float | np.number | Sequence[str | int | float | np.number],
        **kwargs: str
        | int
        | float
        | np.number
        | Sequence[str | int | float | np.number],
    ) -> None:
        self.command = command
        self.args = args
        self.opts = kwargs

    def _optformat(
        self,
        v: str
        | int
        | float
        | np.number
        | Sequence[str | int | float | np.number]
        | Sequence["SCPICommand"],
    ) -> str:
        if isinstance(v, str):
            if "\n" in v:
                return f"<quote>{v}</quote>"
            else:
                return shlex.quote(v)
        elif isinstance(v, (int, float | np.number)):
            return str(v)
        elif isinstance(v, (Sequence, np.ndarray)):
            if isinstance(v[0], SCPICommand):
                q = "multiline." + self.command.lower()
                return (
                    f"<{q}>\n"
                    + textwrap.indent("".join(str(x) for x in v), "\t")
                    + f"</{q}>"
                )
            else:
                return ",".join(self._optformat(v))
        else:
            raise TypeError(f"{v}, of type {type(v)}, not understood.")

    def to_command_string(self) -> str:
        return (
            " ".join(
                [self.command]
                + [f"-{k}={self._optformat(v)}" for k, v in self.opts.items()]
                + [self._optformat(v) for v in self.args]
            )
            + "\n"
        )

    def __str__(self) -> str:
        return self.to_command_string()

    def to_scpicommand(self) -> "SCPICommand":
        return self


class SCPICommandLike(Protocol):
    def to_scpicommand(self) -> SCPICommand:
        ...
