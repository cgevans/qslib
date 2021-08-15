from qslib.base import AccessLevel
import pytest

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
        with pytest.raises(NotImplementedError):
            AccessLevel(l1) > invalid  # type: ignore
        with pytest.raises(NotImplementedError):
            AccessLevel(l1) >= invalid  # type: ignore
        with pytest.raises(NotImplementedError):
            AccessLevel(l1) < invalid  # type: ignore
        with pytest.raises(NotImplementedError):
            AccessLevel(l1) <= invalid  # type: ignore
        with pytest.raises(NotImplementedError):
            AccessLevel(l1) == invalid  # type: ignore
        assert str(AccessLevel(l1)) == l1