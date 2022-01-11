# SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
# SPDX-License-Identifier: AGPL-3.0-only

import pytest

from qslib.scpi_commands import AccessLevel

levels = ["Guest", "Observer", "Controller", "Administrator", "Full"]

invalid = 5


def test_access():
    for l1 in levels:
        for l2 in levels:
            assert (AccessLevel(l1) < AccessLevel(l2)) == (
                levels.index(l1) < levels.index(l2)
            )
            assert (AccessLevel(l1) <= AccessLevel(l2)) == (
                levels.index(l1) <= levels.index(l2)
            )
            assert (AccessLevel(l1) > AccessLevel(l2)) == (
                levels.index(l1) > levels.index(l2)
            )
            assert (AccessLevel(l1) >= AccessLevel(l2)) == (
                levels.index(l1) >= levels.index(l2)
            )
            assert (AccessLevel(l1) == AccessLevel(l2)) == (
                levels.index(l1) == levels.index(l2)
            )
            assert (AccessLevel(l1) < l2) == (levels.index(l1) < levels.index(l2))
            assert (AccessLevel(l1) <= l2) == (levels.index(l1) <= levels.index(l2))
            assert (AccessLevel(l1) > l2) == (levels.index(l1) > levels.index(l2))
            assert (AccessLevel(l1) >= l2) == (levels.index(l1) >= levels.index(l2))
            assert (AccessLevel(l1) == l2) == (levels.index(l1) == levels.index(l2))
        with pytest.raises(ValueError):
            AccessLevel(l1) > invalid  # type: ignore
        with pytest.raises(ValueError):
            AccessLevel(l1) >= invalid  # type: ignore
        with pytest.raises(ValueError):
            AccessLevel(l1) < invalid  # type: ignore
        with pytest.raises(ValueError):
            AccessLevel(l1) <= invalid  # type: ignore
        with pytest.raises(ValueError):
            AccessLevel(l1) == invalid  # type: ignore
        assert str(AccessLevel(l1)) == l1
