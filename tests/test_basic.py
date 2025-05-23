# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

import pytest

from qslib.data import FilterSet


def test_filterset_strings() -> None:
    a = FilterSet(1, 4)

    assert a == FilterSet.fromstring("x1-m4")
    assert a == FilterSet.fromstring("M4_X1")

    assert a.lowerform == "x1-m4"
    assert a.upperform == "M4_X1"

    assert str(a) == a.lowerform


# def test_parse_argstring() -> None:
#     # pylint: disable=protected-access
#     # FIXME: do quotes allow spaces?
#     assert qsa._parse_argstring('-arg1=val1 -arg2="val-2"') == {
#         "arg1": "val1",
#         "arg2": '"val-2"',
#     }


# def test_validate_command_format() -> None:
#     # pylint: disable=protected-access
#     with pytest.raises(ValueError):
#         qsp._validate_command_format(b"\nText\n<a>")

#     with pytest.raises(ValueError):
#         qsp._validate_command_format(b"\nText")

#     with pytest.raises(ValueError):
#         qsp._validate_command_format(b"<a>\n\n")

#     with pytest.raises(ValueError):
#         qsp._validate_command_format(b"<a>\nText\n</b><a>")

#     with pytest.raises(ValueError):
#         qsp._validate_command_format(b"<a>\nText\n</a><a>")

#     with pytest.raises(ValueError):
#         qsp._validate_command_format(b"<a>\nText\n<a></a>")

#     assert qsp._validate_command_format(b"Text") is None

#     assert qsp._validate_command_format(b"Text\n") is None

#     assert (
#         qsp._validate_command_format(b"<a>Text<a>Text</a>\n" b"<b>Text\n\n\n</b></a>")
#         is None
#     )
