# SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
# SPDX-License-Identifier: AGPL-3.0-only

from dataclasses import astuple

import hypothesis.strategies as st
import pytest  # noqa
from hypothesis import given

from qslib._util import _pp_seqsliceint
from qslib.qsconnection_async import FilterDataFilename


@given(
    stage=st.integers(0, 10000),
    cycle=st.integers(0, 10000),
    step=st.integers(0, 10000),
    point=st.integers(0, 10000),
    em=st.integers(1, 6),
    ex=st.integers(1, 6),
)
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
