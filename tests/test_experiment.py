# SPDX-FileCopyrightText: 2021-2023 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2

import pytest

from qslib import Experiment, Protocol, Stage, Step


def test_create():
    Experiment(protocol=Protocol([Stage([Step(30, 25)])]))


def test_fail_plots():
    exp = Experiment(protocol=Protocol([Stage([Step(30, 25)])]))

    with pytest.raises(ValueError, match="no temperature data"):
        exp.plot_temperatures()

    with pytest.raises(ValueError, match="no data available"):
        exp.plot_over_time()

    with pytest.raises(ValueError, match="no data available"):
        exp.plot_anneal_melt()


@pytest.mark.parametrize("ch", ["/", "!", "}"])
def test_unsafe_names(ch):
    with pytest.raises(ValueError, match=r"Invalid characters \(" + ch + r"\)"):
        Experiment(name=f"a{ch}b")
