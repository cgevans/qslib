import pytest
from qslib.scpi_commands import AccessLevel, SCPICommand


def test_unknown():
    s = SCPICommand("UNKNown", "arg1", ["hi", 1, 2], "thing that\n needs quoting")

    assert s.specialize() == s

    assert s.to_scpicommand() == s

    assert SCPICommand.from_scpicommand(s) == s

    print(s.to_string())

    assert s == SCPICommand.from_string(s.to_string())


def test_unknown_type():
    with pytest.raises(TypeError):
        s = SCPICommand("TEST", AccessLevel).to_string()  # type: ignore
