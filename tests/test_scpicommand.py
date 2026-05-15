# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

import pytest

from qslib.scpi_commands import AccessLevel, ArgList, SCPICommand, quote_string_if_needed


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
    assert SCPICommand.from_string("EXPOSURE m4,x1,quant,500 # test comment\n").comment == "test comment"

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


def test_command_suffix_chars():
    """Test that command names with +, -, ~, < suffixes are parsed."""
    for suffix in ["+", "-", "~", "<"]:
        cmd = SCPICommand.from_string(f"SUBS{suffix} topic1\n")
        assert cmd.command == f"SUBS{suffix}".upper()
        assert cmd.args[0] == "topic1"


def test_bool_parsing_extended():
    """Test that all server-recognized boolean forms are parsed correctly."""
    for val in ["true", "True", "yes", "Yes", "on", "On"]:
        result = ArgList.from_string(f"-flag={val}")
        assert result.opts["flag"] is True, f"Expected True for '{val}'"

    for val in ["false", "False", "no", "No", "off", "Off"]:
        result = ArgList.from_string(f"-flag={val}")
        assert result.opts["flag"] is False, f"Expected False for '{val}'"

    # open/closed stay as strings (valid response values like DRAW? → "Open")
    for val in ["open", "opened", "close", "closed"]:
        result = ArgList.from_string(f"-flag={val}")
        assert isinstance(result.opts["flag"], str), f"Expected string for '{val}'"


def test_specialize_command_error():
    """Test that CommandError is specialized into appropriate subclasses."""
    from qslib._qslib import CommandError
    from qslib.scpi_commands import (
        ExclusiveAccessGiven,
        AccessGiven,
        NoMatch,
        specialize_command_error,
    )

    # Test ExclusiveAccessGiven
    e = CommandError({"error": "ExclusiveAccessGiven", "message": "Another session has exclusive control"})
    se = specialize_command_error(e)
    assert isinstance(se, ExclusiveAccessGiven)

    # Test AccessGiven
    e = CommandError({"error": "AccessGiven", "message": "Access already given"})
    se = specialize_command_error(e)
    assert isinstance(se, AccessGiven)

    # Test NoMatch
    e = CommandError({"error": "NoMatch", "message": "No files found"})
    se = specialize_command_error(e)
    assert isinstance(se, NoMatch)

    # Test InsufficientAccess
    e = CommandError({"error": "InsufficientAccess", "message": "Not allowed"})
    se = specialize_command_error(e)
    from qslib.scpi_commands import InsufficientAccess

    assert isinstance(se, InsufficientAccess)

    # Test unknown error class stays as CommandError
    e = CommandError({"error": "SomethingElse", "message": "Unknown"})
    se = specialize_command_error(e)
    assert type(se) is CommandError


# --- Phase 5: New edge case tests for quote_string_if_needed and SCPICommand ---


def test_quote_string_if_needed_plain():
    """Plain string with no spaces, quotes, or newlines should not be quoted."""
    assert quote_string_if_needed("hello") == "hello"
    assert quote_string_if_needed("abc123") == "abc123"
    assert quote_string_if_needed("path/to/file.xml") == "path/to/file.xml"


def test_quote_string_if_needed_spaces():
    """String with spaces should be wrapped in double quotes."""
    result = quote_string_if_needed("hello world")
    assert result == '"hello world"'
    # Verify it starts and ends with double quotes
    assert result[0] == '"' and result[-1] == '"'


def test_quote_string_if_needed_newlines():
    """String with newlines should use <quote>...</quote> block format."""
    result = quote_string_if_needed("line1\nline2")
    assert result == "<quote>line1\nline2</quote>"
    # Newline handling takes priority over space handling
    result2 = quote_string_if_needed("line one\nline two")
    assert result2 == "<quote>line one\nline two</quote>"


def test_quote_string_if_needed_quotes():
    """String containing double quotes should have them escaped."""
    result = quote_string_if_needed('say "hello"')
    assert result == '"say \\"hello\\""'
    # A string with only quotes (no spaces) should still be quoted
    result2 = quote_string_if_needed('a"b')
    assert result2 == '"a\\"b"'


def test_scpicommand_with_block():
    """SCPICommand with a block argument (multiline string) should serialize via <quote> tags."""
    multiline_content = "line one\nline two\nline three"
    cmd = SCPICommand("SETDATA", multiline_content)
    serialized = cmd.to_string()
    # The newline in content triggers <quote>...</quote> wrapping
    assert "<quote>" in serialized
    assert "</quote>" in serialized
    assert multiline_content in serialized
    # Round-trip: parse it back and verify equivalence
    parsed = SCPICommand.from_string(serialized)
    assert parsed.command == "SETDATA"
    assert parsed.args[0] == multiline_content
