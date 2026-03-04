# SPDX-FileCopyrightText: 2021 - 2023 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

"""SCPI Command class and parsing"""

from __future__ import annotations

import textwrap
from abc import ABC, abstractmethod
from dataclasses import dataclass

from typing import Any, Sequence, Type, TypeVar

import numpy as np

from ._qslib import AccessLevel as AccessLevel, CommandError


class NoMatch(CommandError):
    """Raised when a file or pattern match finds no results."""
    pass


class AuthError(CommandError):
    """Raised when authentication fails (wrong password)."""
    pass


class AccessLevelExceeded(CommandError):
    """Raised when the requested access level exceeds what is allowed."""
    pass


class InsufficientAccess(CommandError):
    """Raised when the current access level is insufficient for an operation."""
    pass


class ExclusiveAccessGiven(CommandError):
    """Raised when another session holds exclusive access."""
    pass


class AccessGiven(CommandError):
    """Raised when access has already been given to another session."""
    pass


_ERROR_CLASS_MAP: dict[str, type[CommandError]] = {
    "NoMatch": NoMatch,
    "AuthError": AuthError,
    "AccessLevelExceeded": AccessLevelExceeded,
    "InsufficientAccess": InsufficientAccess,
    "ExclusiveAccessGiven": ExclusiveAccessGiven,
    "AccessGiven": AccessGiven,
}


def specialize_command_error(e: CommandError) -> CommandError:
    """Re-raise a CommandError as a more specific subclass if possible."""
    if e.args and isinstance(e.args[0], dict):
        error_class = e.args[0].get("error", "")
    else:
        error_class = str(e)
    if error_class in _ERROR_CLASS_MAP:
        specific = _ERROR_CLASS_MAP[error_class](*e.args)
        specific.__cause__ = e
        return specific
    return e


def quote_string_if_needed(s: str) -> str:
    """Quote a string if it contains spaces, quotes, or newlines.

    Strings with newlines are wrapped in <quote>...</quote> tags.
    Strings with spaces or quotation marks are wrapped in double quotes with
    escaped quotes. Other characters like $, {, } are not escaped.
    """
    if "\n" in s:
        return f"<quote>{s}</quote>"
    if " " in s or '"' in s:
        return '"' + s.replace('"', '\\"') + '"'
    return s


@dataclass
class ArgList:
    "A representation of an SCPI list of options (-key=value) and arguments."
    opts: dict[str, bool | int | float | str]
    args: list[bool | int | float | str]

    @classmethod
    def from_string(cls, argument_string: str) -> ArgList:
        """Parse an SCPI argument string."""
        from ._qslib import parse_arglist
        result = parse_arglist(argument_string)
        return cls(opts=result.opts, args=list(result.args))


T = TypeVar("T")


class SCPICommandLike(ABC):
    """Abstract class for an object that can be converted from/to an SCPICommand."""

    @abstractmethod
    def to_scpicommand(self, **kwargs: Any) -> SCPICommand:  # pragma: no cover
        """Convert the object to an :any:`SCPICommand`"""
        ...

    @classmethod
    @abstractmethod
    def from_scpicommand(cls: Type[T], com: SCPICommand) -> T:  # pragma: no cover
        """Try to create the object from an :any:`SCPICommand`."""
        ...


@dataclass(init=False)
class SCPICommand(SCPICommandLike):
    """
    A representation of an SCPI Command.
    """

    command: str
    args: Sequence[
        str
        | int
        | float
        | np.number[Any]
        | Sequence[str | int | float | np.number[Any]]
        | Sequence["SCPICommand"]
    ]
    opts: dict[
        str,
        str
        | int
        | float
        | np.number[Any]
        | Sequence[str | int | float | np.number[Any]],
    ]
    comment: str | None

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, SCPICommand):
            return False

        return (
            (self.command == other.command)
            and (self.args == other.args)
            and (self.opts == other.opts)
        )

    def __init__(
        self,
        command: str,
        *args: str
        | int
        | float
        | np.number[Any]
        | Sequence[str | int | float | np.number[Any]]
        | Sequence["SCPICommand"],
        comment: str | None = None,
        **kwargs: (
            str
            | int
            | float
            | np.number[Any]
            | Sequence[str | int | float | np.number[Any]]
        ),
    ) -> None:
        if " " in command:
            if args or comment or kwargs:
                raise ValueError
            n = SCPICommand.from_string(command)
            self.command = n.command
            self.args = n.args
            self.opts = n.opts
            self.comment = n.comment
            return

        self.command = command.upper()
        self.args = args
        self.opts = {k.lower(): v for k, v in kwargs.items()}
        self.comment = comment

    def _optformat(
        self,
        opt_val: (
            str
            | int
            | float
            | np.number[Any]
            | Sequence[str | int | float | np.number[Any]]
            | Sequence[SCPICommand]
            | SCPICommand
        ),
    ) -> str:
        if isinstance(opt_val, SCPICommand):
            opt_val = [opt_val]
        if isinstance(opt_val, str):
            return quote_string_if_needed(opt_val)
        if isinstance(opt_val, (int, float, np.number)):
            return str(opt_val)
        if isinstance(opt_val, (Sequence, np.ndarray)):
            if isinstance(opt_val[0], SCPICommand):
                q = "multiline." + self.command.lower()
                return (
                    f"<{q}>\n"
                    + textwrap.indent("".join(str(x) for x in opt_val), "\t")
                    + f"</{q}>"
                )
            return ",".join(self._optformat(x) for x in opt_val)

        raise TypeError(f"{opt_val}, of type {type(opt_val)}, not understood.")

    def to_string(self) -> str:
        """Create a usable command string, including terminal newline."""
        if self.comment:
            comment = f" # {self.comment}"
        else:
            comment = ""
        return (
            " ".join(
                [self.command]
                + [f"-{k}={self._optformat(v)}" for k, v in self.opts.items()]
                + [self._optformat(v) for v in self.args]
            )
            + comment
            + "\n"
        )

    @classmethod
    def from_string(cls, command_string: str) -> SCPICommand:
        """Parse (as SCPICommands) an SCPI command string."""
        from ._qslib import SCPICommand as RustSCPICommand
        rust_cmd = RustSCPICommand.from_string(command_string)
        return _convert_rust_scpi(rust_cmd)

    def specialize(self) -> SCPICommandLike:
        """If possible, convert SCPICommand to QSLib classes for the command."""
        if self.command.upper() in _scpi_command_classes:
            return _scpi_command_classes[self.command.upper()].from_scpicommand(self)
        return self

    def __str__(self) -> str:
        return self.to_string()

    def to_scpicommand(self, **kwargs: Any) -> SCPICommand:
        return self

    @classmethod
    def from_scpicommand(cls, com: SCPICommand) -> SCPICommand:
        return com


def _convert_rust_scpi(rust_cmd: Any) -> SCPICommand:
    """Convert a Rust SCPICommand to a Python SCPICommand, recursively converting nested commands."""
    from ._qslib import SCPICommand as RustSCPICommand

    def convert_arg(arg: Any) -> Any:
        if isinstance(arg, RustSCPICommand):
            return _convert_rust_scpi(arg)
        if isinstance(arg, list):
            return [convert_arg(item) for item in arg]
        return arg

    args = tuple(convert_arg(a) for a in rust_cmd.args)
    opts = {k: convert_arg(v) for k, v in rust_cmd.opts.items()}

    cmd = SCPICommand.__new__(SCPICommand)
    cmd.command = rust_cmd.command
    cmd.args = args
    cmd.opts = opts
    cmd.comment = rust_cmd.comment
    return cmd


_scpi_command_classes: dict[str, SCPICommandLike] = {}
