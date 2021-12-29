from dataclasses import astuple

import pytest  # noqa

from qslib.qsconnection_async import FilterDataFilename
from qslib._util import _pp_seqsliceint


@pytest.mark.parametrize("stage", [1, 63, 2623])
@pytest.mark.parametrize("cycle", [4, 262, 2633])
@pytest.mark.parametrize("step", [5, 63, 2633])
@pytest.mark.parametrize("point", [24, 53250])
@pytest.mark.parametrize("em", [2])
@pytest.mark.parametrize("ex", [2, 3])
def test_fd_fn(stage, cycle, step, point, em, ex):
    fds = f"S{stage:02}_C{cycle:03}_T{step:02}" f"_P{point:04}_M{em}_X{ex}"
    fdfn = fds + "_filterdata.xml"

    x = FilterDataFilename.fromstring(fdfn)

    assert astuple(x) == ((ex, em, True), stage, cycle, step, point)

    assert fdfn == x.tostring()


def test_pp_seqsliceint():
    assert _pp_seqsliceint(5) == "5"
    assert _pp_seqsliceint(slice(1, 5)) == "1 to 5"
    assert _pp_seqsliceint(slice(5, None)) == "5 onwards"
    assert _pp_seqsliceint(slice(None, 5)) == "up to 5"
    assert _pp_seqsliceint([1, 2, 5]) == "[1, 2, 5]"
    assert _pp_seqsliceint(slice(1, 5, 2)) == "1 to 5 by step 2"
    with pytest.raises(TypeError):
        _pp_seqsliceint(5.5)  # type: ignore
