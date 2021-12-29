from __future__ import annotations

import shlex
import textwrap
from dataclasses import dataclass
from enum import Enum
from typing import Protocol, Sequence, Type, TypeVar, cast

import numpy as np
import pyparsing as pp

pp.ParserElement.setDefaultWhitespaceChars("")
from pyparsing import pyparsing_common as ppc

_ws_or_end = (pp.White(" \t\r") | pp.StringEnd() | pp.FollowedBy("\n")).suppress()
_nl = (
    (pp.Literal("\n") + pp.Optional(pp.White(" \t\r"))).suppress().setName("<newline>")
)
_fwe = pp.FollowedBy(_ws_or_end).suppress()
_fweqc = pp.FollowedBy(_ws_or_end | "<" | ",").suppress()


def _make_multi_keyword(kwd_str, kwd_value):
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
    | (pp.Regex(r"[^ \t\n<\",']+") + _fweqc)
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

_command: pp.ParserElement = cast(
    pp.ParserElement,
    (
        ppc.identifier("command")
        + _ws_or_end
        + pp.Optional(_arglist("arglist") + _ws_or_end, {})
    ).setParseAction(
        lambda toks: SCPICommand(
            toks["command"],
            *toks["arglist"].args,
            **toks["arglist"].opts,
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


@dataclass
class ArgList:
    opts: dict[str, bool | int | float | str]
    args: list[bool | int | float | str]

    @classmethod
    def from_string(cls, v: str) -> ArgList:
        return cast(ArgList, _arglist.parseString(v)[0])


@dataclass(init=False)
class SCPICommand:
    """
    A representation of an SCPI Command.
    """

    command: str
    args: Sequence[
        str
        | int
        | float
        | np.number
        | Sequence[str | int | float | np.number]
        | Sequence["SCPICommand"]
    ]
    opts: dict[
        str, str | int | float | np.number | Sequence[str | int | float | np.number]
    ]

    def __init__(
        self,
        command: str,
        *args: str
        | int
        | float
        | np.number
        | Sequence[str | int | float | np.number]
        | Sequence["SCPICommand"],
        **kwargs: str
        | int
        | float
        | np.number
        | Sequence[str | int | float | np.number],
    ) -> None:
        self.command = command.upper()
        self.args = args
        self.opts = {k.lower(): v for k, v in kwargs.items()}

    def _optformat(
        self,
        v: str
        | int
        | float
        | np.number
        | Sequence[str | int | float | np.number]
        | Sequence["SCPICommand"]
        | "SCPICommand",
    ) -> str:
        if isinstance(v, SCPICommand):
            v = [v]
        if isinstance(v, str):
            if "\n" in v:
                return f"<quote>{v}</quote>"
            else:
                return shlex.quote(v)
        elif isinstance(v, (int, float, np.number)):
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
                return ",".join(self._optformat(x) for x in v)
        else:
            raise TypeError(f"{v}, of type {type(v)}, not understood.")

    def to_string(self) -> str:
        return (
            " ".join(
                [self.command]
                + [f"-{k}={self._optformat(v)}" for k, v in self.opts.items()]
                + [self._optformat(v) for v in self.args]
            )
            + "\n"
        )

    @classmethod
    def from_string(cls, v: str) -> SCPICommand:
        return cast(SCPICommand, _command.parseString(v)[0])

    def specialize(self) -> SCPICommandLike:
        if self.command.upper() in _scpi_command_classes:
            return _scpi_command_classes[self.command.upper()].from_scpicommand(self)
        else:
            return self

    def __str__(self) -> str:
        return self.to_string()

    def to_scpicommand(self, **kwargs) -> "SCPICommand":
        return self

    @classmethod
    def from_scpicommand(cls, com: SCPICommand) -> "SCPICommand":
        return com


T = TypeVar("T")


class SCPICommandLike(Protocol):
    def to_scpicommand(self, **kwargs) -> SCPICommand:  # pragma: no cover
        ...

    @classmethod
    def from_scpicommand(cls: Type[T], com: SCPICommand) -> T:  # pragma: no cover
        ...


_scpi_command_classes: dict[str, SCPICommandLike] = {}
