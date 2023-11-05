# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

import pytest

from qslib.scpi_commands import AccessLevel, ArgList, SCPICommand


def test_unknown():
    s = SCPICommand("UNKNown", "arg1", ["hi", 1, 2], "thing that\n needs quoting")

    assert s.specialize() == s

    assert s.to_scpicommand() == s

    assert SCPICommand.from_scpicommand(s) == s

    print(s.to_string())

    assert s == SCPICommand.from_string(s.to_string())


def test_unknown_type():
    with pytest.raises(TypeError):
        SCPICommand("TEST", AccessLevel).to_string()  # type: ignore


def test_comment():
    assert (
        SCPICommand.from_string("EXPOSURE m4,x1,quant,500 # test comment\n").comment
        == "test comment"
    )

    assert SCPICommand.from_string("COMMAND '#' b").comment is None


def test_arglist():
    assert ArgList.from_string("-v1=2.0 t1 t2") == ArgList({"v1": 2.0}, ["t1", "t2"])


def test_neq_other():
    assert SCPICommand("Exp") != "Exp"


def test_auto():
    com = 'TESTCOM arg -an="opt with a string"'
    assert SCPICommand(com) == SCPICommand.from_string(com)

    with pytest.raises(ValueError):
        SCPICommand(com, "extra arg")
