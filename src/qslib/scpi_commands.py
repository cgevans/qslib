# SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
#
# SPDX-License-Identifier: AGPL-3.0-only

"""SCPI Command class and parsing"""

from __future__ import annotations

import re
import shlex
import textwrap
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal, Sequence, Type, TypeVar, cast

import numpy as np
import pyparsing as pp
from pyparsing import ParserElement
from pyparsing import pyparsing_common as ppc

pp.ParserElement.setDefaultWhitespaceChars("")

_ws_or_end = (
    (pp.Regex(r"[ \t\r]+") | pp.StringEnd() | pp.FollowedBy("\n"))
    .suppress()
    .setName("<ws/end>")
)
_nl = (
    (pp.Literal("\n") + pp.Optional(pp.Regex(r"[ \t\r]+")))
    .suppress()
    .setName("<newline>")
)
_fwe = pp.FollowedBy(_ws_or_end).suppress().setName("<fwe?>")
_fweqc = pp.FollowedBy(_ws_or_end | "<" | ",").suppress().setName("<fweqc?>")


def _make_multi_keyword(kwd_str: str, kwd_value: Any) -> ParserElement:
    x = pp.oneOf(kwd_str)
    x.setParseAction(pp.replaceWith(kwd_value))
    return x


_pbool = _make_multi_keyword("true True", True) | _make_multi_keyword(
    "False false", False
)

_qs = pp.quotedString
_qs.setParseAction(lambda toks: toks[0][1:-1])

_quote_content = pp.Word(pp.alphanums + "._")
_quote_open = pp.Combine("<" + _quote_content + ">")
_quote_close = pp.Combine("</" + pp.matchPreviousExpr(_quote_content) + ">")
_quote_close_any = pp.Combine("</" + _quote_content + ">")

_command_forward = pp.Forward()

_commands_block = (
    _quote_open.suppress()
    + _nl
    + pp.delimitedList(_command_forward, _nl)
    + _nl
    + _quote_close.suppress()
)

_opt_value_one = (
    (_pbool + _fweqc)
    | (ppc.number + _fweqc)
    | (_quote_open.suppress() + pp.Regex(r"[^<]+") + _quote_close.suppress() + _fweqc)
    | (pp.Regex(r"[^ \t\n<\",#']+") + _fweqc)
    | (_qs + _fweqc)
).setParseAction(lambda toks: toks[0])

_ovcl = pp.delimited_list(_opt_value_one, ",")

_opt_value = (_commands_block | _ovcl | _opt_value_one).setParseAction(
    lambda toks: toks[0]
    if not isinstance(toks[0], SCPICommand) and len(toks) == 1
    else (list(toks[:]),)
)

_opt_kv_pair = (
    pp.Literal("-").suppress()
    + ppc.identifier("key")
    + pp.Literal("=").suppress()
    + _opt_value.setResultsName("value")
).setResultsName("opt", listAllMatches=True)

_arg = _opt_value.setResultsName("arg", listAllMatches=True)

_arglist = pp.delimitedList(
    _opt_kv_pair
    | (_quote_open.suppress() + _opt_kv_pair + _quote_close.suppress())
    | _arg,
    _ws_or_end,
)

_arglist.setParseAction(
    lambda toks: ArgList(
        {k: v for k, v in toks.get("opt", [])}, list(toks.get("arg", []))
    )
)


@dataclass
class ArgList:
    "A representation of an SCPI list of options (-key=value) and arguments."
    opts: dict[str, bool | int | float | str]
    args: list[bool | int | float | str]

    @classmethod
    def from_string(cls, argument_string: str) -> ArgList:
        """Parse an SCPI argument string."""
        return cast(ArgList, _arglist.parseString(argument_string)[0])


_NULLIST = ArgList({}, [])

_commentstring: pp.ParserElement = pp.Combine(
    pp.Regex(r"\s*#\s?").suppress()
    + pp.Regex(r"[^\n]+")
    + (pp.StringEnd() | pp.FollowedBy("\n")).suppress()
)

_command: pp.ParserElement = cast(
    pp.ParserElement,
    (
        pp.Combine(pp.Regex("[A-Za-z:_]+"))("command")
        + _ws_or_end
        + pp.Optional(_arglist("arglist"))
        + pp.Optional(_commentstring("comment"))
        + _ws_or_end
    ).setParseAction(
        lambda toks: SCPICommand(
            toks["command"],
            comment=toks.get("comment", None),
            *toks.get("arglist", _NULLIST).args,  # FIXME
            **toks.get("arglist", _NULLIST).opts,
        )
    ),
)

_command_forward << _command


_accesslevel_order: dict[str, int] = {
    "Guest": 0,
    "Observer": 1,
    "Controller": 2,
    "Administrator": 3,
    "Full": 4,
}


class AccessLevel(Enum):
    """QS machine access level, with comparisons."""

    Guest = "Guest"
    Observer = "Observer"
    Controller = "Controller"
    Administrator = "Administrator"
    Full = "Full"
    # value: str

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

    def __hash__(self) -> int:
        return self.value.__hash__()

    def __str__(self) -> str:
        return self.value


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
        **kwargs: str
        | int
        | float
        | np.number[Any]
        | Sequence[str | int | float | np.number[Any]],
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
        opt_val: str
        | int
        | float
        | np.number[Any]
        | Sequence[str | int | float | np.number[Any]]
        | Sequence[SCPICommand]
        | SCPICommand,
    ) -> str:
        if isinstance(opt_val, SCPICommand):
            opt_val = [opt_val]
        if isinstance(opt_val, str):
            if "\n" in opt_val:
                return f"<quote>{opt_val}</quote>"
            return shlex.quote(opt_val)
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
        return cast(SCPICommand, _command.parseString(command_string)[0])

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


_scpi_command_classes: dict[str, SCPICommandLike] = {}
