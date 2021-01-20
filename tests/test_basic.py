import pytest

from qslib.data import FilterSet
import qslib.qsconnection_async as qsa


def test_filterset_strings() -> None:
    a = FilterSet(1, 4)

    assert a == FilterSet.fromstring("x1-m4")
    assert a == FilterSet.fromstring("M4_X1")

    assert a.lowerform == "x1-m4"
    assert a.upperform == "M4_X1"

    assert str(a) == a.lowerform


def test_parse_argstring() -> None:
    # pylint: disable=protected-access
    # FIXME: do quotes allow spaces?
    assert qsa._parse_argstring('-arg1=val1 -arg2="val-2"') == {
        "arg1": "val1",
        "arg2": '"val-2"',
    }


def test_validate_command_format() -> None:
    # pylint: disable=protected-access
    with pytest.raises(ValueError):
        qsa._validate_command_format("\nText\n<a>")

    with pytest.raises(ValueError):
        qsa._validate_command_format("\nText")

    with pytest.raises(ValueError):
        qsa._validate_command_format("<a>\n\n")

    with pytest.raises(ValueError):
        qsa._validate_command_format("<a>\nText\n</b><a>")

    with pytest.raises(ValueError):
        qsa._validate_command_format("<a>\nText\n</a><a>")

    with pytest.raises(ValueError):
        qsa._validate_command_format("<a>\nText\n<a></a>")

    assert qsa._validate_command_format("Text") is None

    assert qsa._validate_command_format("Text\n") is None

    assert (
        qsa._validate_command_format("<a>Text<a>Text</a>\n" "<b>Text\n\n\n</b></a>")
        is None
    )
