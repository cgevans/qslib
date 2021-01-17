import pytest

from qslib.skeleton import fib

__author__ = "Constantine Evans"
__copyright__ = "Constantine Evans"
__license__ = "GPL-3.0-or-later"


def test_fib():
    assert fib(1) == 1
    assert fib(2) == 1
    assert fib(7) == 13
    with pytest.raises(AssertionError):
        fib(-10)
