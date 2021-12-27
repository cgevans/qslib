from __future__ import annotations

from dataclasses import dataclass

import pyparsing as pp

pp.ParserElement.setDefaultWhitespaceChars("")
from pyparsing import pyparsing_common as ppc

we = (pp.White(" \t\r") | pp.StringEnd() | pp.FollowedBy("\n")).suppress()
nl = (pp.Literal("\n") + pp.Optional(pp.White(" \t\r"))).suppress().setName("<newline>")
fwe = pp.FollowedBy(we).suppress()
fweq = pp.FollowedBy(we | "<").suppress()


def make_multi_keyword(kwd_str, kwd_value):
    x = pp.oneOf(kwd_str)
    x.setParseAction(pp.replaceWith(kwd_value))
    return x


pbool = make_multi_keyword("true True", True) | make_multi_keyword("False false", False)

qs = pp.quotedString
qs.setParseAction(lambda toks: toks[0][1:-1])

quote_content = pp.Word(pp.alphanums + "._")
quote_open = pp.Combine("<" + quote_content + ">")
quote_close = pp.Combine("</" + pp.matchPreviousExpr(quote_content) + ">")
quote_close_any = pp.Combine("</" + quote_content + ">")


optvalue = (
    (pbool + fweq)
    | (ppc.number + fweq)
    | (quote_open.suppress() + pp.Regex(r"[^ \t\n<]+") + quote_close.suppress() + fweq)
    | (pp.Regex(r"[^ \t\n<\"']+") + fweq)
    | (qs + fweq)
).setParseAction(lambda toks: toks[0])

optkey = ppc.identifier("key")

optpair = (
    pp.Literal("-").suppress()
    + optkey
    + pp.Literal("=").suppress()
    + optvalue.setResultsName("value")
).setResultsName("opt", listAllMatches=True)

arg = optvalue.setResultsName("arg", listAllMatches=True)

arglist = (
    pp.delimitedList(
        optpair | (quote_open.suppress() + optpair + quote_close.suppress()) | arg,
        we,
    )
)("arglist")
arglist.setParseAction(
    lambda toks: {
        "opts": {k: v for k, v in toks.get("opt", [])},
        "args": list(toks.get("arg", [])),
    }
)


@dataclass(init=False)
class ArgList:
    opts: dict[str, bool | int | float | str]
    args: list[bool | int | float | str]

    def __init__(self, s: str) -> None:
        v = arglist.parseString(s)
        self.opts = v["arglist"]["opts"]
        self.args = v["arglist"]["args"]


mlf = pp.Forward()

quotedmultiline = (
    quote_open.suppress() + nl + pp.delimitedList(mlf, nl) + nl + quote_close.suppress()
)

command = (
    ppc.identifier
    + we
    + pp.Optional(arglist + we, {})
    + pp.Optional(quotedmultiline("body"))
).setParseAction(
    lambda toks: {"command": toks[0], **toks[1]}
    | ({"body": list(toks["body"])} if "body" in toks.keys() else {})
)

mlf << command
