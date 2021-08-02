import pyparsing as pp

pp.ParserElement.setDefaultWhitespaceChars(" \t")
from pyparsing import pyparsing_common as ppc

we = (pp.White(" \t\r") | pp.StringEnd() | pp.FollowedBy("\n")).suppress()
nl = (pp.Literal("\n") + pp.White(" \t\r", min=0)).suppress().setName("<newline>")
fwe = pp.FollowedBy(we).suppress()


def make_keyword(kwd_str, kwd_value):
    return pp.Keyword(kwd_str).setParseAction(pp.replaceWith(kwd_value))


def make_multi_keyword(kwd_str, kwd_value):
    return pp.oneOf(kwd_str).setParseAction(pp.replaceWith(kwd_value))


pbool = make_multi_keyword("true True", True) | make_multi_keyword("False false", False)

argvalue = pbool | ppc.number

argpair = (
    pp.Combine(pp.Suppress("-") + ppc.identifier + pp.Suppress("=")) + argvalue + we
).setParseAction(lambda toks: (toks[0].lower(), toks[1]))

contentvalue = ((ppc.number | pp.Word(pp.alphas)) + we).setParseAction(
    lambda toks: (toks[0],)
)


def _arrangeitems(toks):
    args = {}
    content = []
    for t in toks:
        if len(t) == 1:
            content.append(t[0])
        elif len(t) == 2:
            args[t[0]] = t[1]
        else:
            raise ValueError
    return {"args": args, "content": content}


argcontentlist = pp.ZeroOrMore(argpair | contentvalue).setParseAction(_arrangeitems)


def command_onearg(cls, name):
    return (
        (pp.Keyword(name).suppress() + we + argcontentlist)
        .setParseAction(lambda toks: cls(toks[0]["content"], **toks[0]["args"]))
        .setName(name)
    )


def command_args(cls, name):
    return (
        (pp.Keyword(name).suppress() + we + argcontentlist)
        .setParseAction(lambda toks: cls(*toks[0]["content"], **toks[0]["args"]))
        .setName(name)
    )
