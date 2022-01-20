# SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
# SPDX-License-Identifier: AGPL-3.0-only

import pytest

from qslib import *


def test_create():

    exp = Experiment(protocol=Protocol([Stage([Step(30, 25)])]))


def test_fail_plots():
    exp = Experiment(protocol=Protocol([Stage([Step(30, 25)])]))

    with pytest.raises(ValueError, match="no temperature data"):
        exp.plot_temperatures()

    with pytest.raises(ValueError, match="no data available"):
        exp.plot_over_time()

    with pytest.raises(ValueError, match="no data available"):
        exp.plot_anneal_melt()
