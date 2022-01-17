# SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
# SPDX-License-Identifier: AGPL-3.0-only

import pytest

from qslib import *


def test_create():

    exp = Experiment(protocol=Protocol([Stage([Step(30, 25)])]))
