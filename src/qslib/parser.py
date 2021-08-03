import pyparsing as pp

pp.ParserElement.setDefaultWhitespaceChars("")
from pyparsing import pyparsing_common as ppc

we = (pp.White(" \t\r") | pp.StringEnd() | pp.FollowedBy("\n")).suppress()
nl = (pp.Literal("\n") + pp.Optional(pp.White(" \t\r"))).suppress().setName("<newline>")
fwe = pp.FollowedBy(we).suppress()


def make_keyword(kwd_str, kwd_value):
    return pp.Keyword(kwd_str).setParseAction(pp.replaceWith(kwd_value))


def make_multi_keyword(kwd_str, kwd_value):
    return pp.oneOf(kwd_str).setParseAction(pp.replaceWith(kwd_value))


pbool = make_multi_keyword("true True", True) | make_multi_keyword("False false", False)

qs = pp.quotedString.setParseAction(lambda toks: toks[0][1:-1])

optvalue = (
    (pbool + fwe)
    | (ppc.number + fwe)
    | (pp.Word(pp.alphanums + "_.,-") + fwe)
    | (qs + fwe)
).setParseAction(lambda toks: toks[0])

optkey = ppc.identifier("key")

optpair = (
    pp.Literal("-").suppress()
    + optkey
    + pp.Literal("=").suppress()
    + optvalue.setResultsName("value")
).setResultsName("opt", listAllMatches=True)

arg = optvalue.setResultsName("arg", listAllMatches=True)

arglist = (pp.delimitedList(optpair | arg, we))("arglist").setParseAction(
    lambda toks: {
        "opts": {k: v for k, v in toks.get("opt", [])},
        "args": list(toks.get("arg", [])),
    }
)

quote_content = pp.Word(pp.alphanums + "._")
quote_open = pp.Combine("<" + quote_content + ">")
quote_close = pp.Combine("</" + pp.matchPreviousExpr(quote_content) + ">")

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
