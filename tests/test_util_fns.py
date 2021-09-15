import pytest  # noqa

from qslib.qsconnection_async import _parse_fd_fn, _index_to_filename_ref

@pytest.mark.parametrize("stage", [1, 63, 2623])
@pytest.mark.parametrize("cycle", [4, 262, 2633])
@pytest.mark.parametrize("step", [5, 63, 2633])
@pytest.mark.parametrize("point", [24, 53250])
@pytest.mark.parametrize("em", [2])
@pytest.mark.parametrize("ex", [2, 3])

def test_fd_fn(stage, cycle, step, point, em, ex):
    fds = (f"S{stage:02}_C{cycle:03}_T{step:02}"
            f"_P{point:04}_M{em}_X{ex}")
    fdfn = fds + "_filterdata.xml"

    x = _parse_fd_fn(fdfn)
    
    assert x == (f"x{ex}-m{em}", stage, cycle, step, point)

    assert fds == _index_to_filename_ref(x)
